"""Structural verification of the refactored responsibility boundaries.

Part 1: Rules are pure -- cross-segment same-device cells, None vs empty
        string devices, input immutability, lazy/streaming finish.
Part 2: order runs on opaque, non-subscriptable cells with custom rules and a
        driver stub exposing only the mandated ports; checks ordering, same-key
        merging, single-pass access, buffer/fanin limits and the fact that the
        input close and final segments stay with the outer lifecycle.
Part 3: Driver ownership release, reverse cleanup and error collection under
        the fault-injecting workspace, with no reduction rules attached.
Part 4: real import/call dependency inspection so rules, file ownership and
        the merge algorithm cannot cross their assigned module boundaries.
"""
import ast
import sys
from collections.abc import Hashable
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from driver import Driver  # noqa: E402
from reduction import Rules  # noqa: E402
from sorter import order  # noqa: E402
from workspace import Handle, MemoryWorkspace, Workspace  # noqa: E402

PRODUCTION = ["reduction.py", "driver.py", "sorter.py", "pipeline.py",
              "spill.py"]


# --------------------------------------------------------------------------
# Part 1: pure rules
# --------------------------------------------------------------------------

def part_rules_purity():
    rules = Rules()
    assert vars(rules) == {}

    # lift validates and never mutates the source row; bool amounts rejected.
    row = ["g", "d", 7]
    cell = rules.lift(row)
    assert cell == ["g", "d", 1, 7] and row == ["g", "d", 7]
    assert cell is not row
    for bad in (["g", "d", True], ["g", 3, 1], [1, "d", 1],
                ["g", "d"], ["g", "d", 1, 2], "nope", None):
        try:
            rules.lift(bad)
        except ValueError as error:
            assert str(error) == "record"
        else:
            raise AssertionError("bad row accepted: %r" % (bad,))

    # None devices sort before every string device; "" is first among strings.
    none_cell = rules.lift(["g", None, 1])
    empty_cell = rules.lift(["g", "", 1])
    word_cell = rules.lift(["g", "z", 1])
    assert rules.key(none_cell) < rules.key(empty_cell) < rules.key(word_cell)
    assert rules.key(rules.lift(["a", "z", 1])) < rules.key(
        rules.lift(["b", None, 1]))

    # Same device arriving from different segments combines into a fresh cell.
    left = ["g", "d", 2, 5]
    right = ["g", "d", 3, 8]
    merged = rules.combine(left, right)
    assert merged == ["g", "d", 5, 13]
    assert left == ["g", "d", 2, 5] and right == ["g", "d", 3, 8]
    assert merged is not left and merged is not right

    # finish is lazy: building it consumes nothing before the first pull.
    started = {"n": 0}

    def tracked():
        for value in (1, 2):
            started["n"] += 1
            yield ["g", "d", value, value]

    gen = rules.finish(tracked())
    assert started["n"] == 0
    gen.close()  # closing before first pull must be harmless
    assert started["n"] == 0

    # Cross-segment same device: the ordering layer merges equal keys via
    # combine, so finish then sees exactly one cell per (group, device).
    merged_d = rules.combine(["a", "d", 2, 9], ["a", "d", 1, -3])
    cells = [["a", None, 2, 4], merged_d, ["b", "", 1, 0], ["b", None, 1, 7]]
    # a: counts 2+3=5, sums 4+6=10, one distinct device;
    # b: "" counts as a device, None does not.
    assert list(rules.finish(iter(cells))) == [
        ("a", 5, 10, 1), ("b", 2, 7, 1)]
    # finish streams without retaining a device set: two already-merged cells
    # for distinct keys are counted per cell (the merge guarantees uniqueness).
    assert list(rules.finish(iter([["a", "d", 1, 1]]))) == [("a", 1, 1, 1)]
    assert list(rules.finish(iter([]))) == []
    print("PART1_RULES_PURE_OK")


# --------------------------------------------------------------------------
# Part 2: order over opaque cells through a port-only stub driver
# --------------------------------------------------------------------------

class Opaque:
    """Cell without field subscripting; order must never reach inside it."""
    __slots__ = ("k", "n", "v")

    def __init__(self, k, n, v):
        self.k, self.n, self.v = k, n, v

    def __getitem__(self, index):  # pragma: no cover - must never be called
        raise AssertionError("order subscripted an opaque cell")


class CustomRules:
    """Different domain than production; deliberately has no finish()."""
    def lift(self, record):
        return Opaque(record[0], 1, record[1])

    def key(self, cell):
        return cell.k

    def combine(self, left, right):
        return Opaque(left.k, left.n + right.n, left.v + right.v)


class CountingInput:
    """Single-pass iterator: a second pass after exhaustion is an error."""
    def __init__(self, rows):
        self.rows, self.pulls = rows, 0
        self.iter_count, self.exhausted, self.closed = 0, False, False

    def __iter__(self):
        self.iter_count += 1
        if self.exhausted:
            raise AssertionError("input iterated more than once")
        return self

    def __next__(self):
        if self.pulls == len(self.rows):
            self.exhausted = True
            raise StopIteration
        row = self.rows[self.pulls]
        self.pulls += 1
        return row

    def close(self):
        self.closed = True


class StubDriver:
    """In-memory segment double implementing only the mandated driver ports."""
    PORTS = frozenset(
        ["allocate", "open_read", "open_write", "read", "write",
         "close", "remove"])

    def __init__(self):
        self.data = {}
        self.cursors = {}
        self.open_tokens = {}
        self.write_opens = {}
        self.read_opens = {}
        self.removed = []
        self.events = []
        self.peak = 0
        self._serial = 0
        self._fail = None

    def allocate(self):
        name = "s%04d" % (len(self.data) + len(self.removed) + 1)
        self.data[name] = []
        self.write_opens[name] = 0
        self.read_opens[name] = 0
        self.events.append(("allocate", name))
        return name

    def open_write(self, name):
        self._serial += 1
        token = ("w", self._serial)
        self.open_tokens[token] = name
        self.write_opens[name] += 1
        self.peak = max(self.peak, len(self.open_tokens))
        self.events.append(("open_write", name))
        return token

    def open_read(self, name):
        self._serial += 1
        token = ("r", self._serial)
        self.open_tokens[token] = name
        self.cursors[token] = 0
        self.read_opens[name] += 1
        self.peak = max(self.peak, len(self.open_tokens))
        self.events.append(("open_read", name))
        return token

    def read(self, token):
        name = self.open_tokens[token]
        index = self.cursors[token]
        segment = self.data[name]
        if index == len(segment):
            return None
        cell = segment[index]
        self.cursors[token] = index + 1
        return cell

    def write(self, token, cell):
        if self._fail is not None:
            kind, left = self._fail
            left[0] -= 1
            if left[0] == 0:
                self._fail = None
                raise RuntimeError("injected:" + kind)
        self.open_tokens[token]  # KeyError if token unknown
        self.data[self.open_tokens[token]].append(cell)

    def close(self, token):
        name = self.open_tokens.pop(token)
        self.cursors.pop(token, None)
        self.events.append(("close", name))

    def remove(self, name):
        del self.data[name]
        self.removed.append(name)
        self.events.append(("remove", name))


class PortOnlyProxy:
    """Exposes exactly the driver ports; any other attribute access fails."""
    def __init__(self, inner):
        object.__setattr__(self, "_inner", inner)

    def __getattribute__(self, name):
        inner = object.__getattribute__(self, "_inner")
        if name in StubDriver.PORTS:
            return getattr(inner, name)
        raise AssertionError("order reached through non-port attribute: "
                             + name)


def reference_opaque(rows):
    totals = {}
    for key, weight in rows:
        n, v = totals.get(key, (0, 0))
        totals[key] = (n + 1, v + weight)
    return [(key, n, v) for key, (n, v) in sorted(totals.items())]


def part_order_boundaries():
    rng_seed = 90210
    grid = [(n, size, fanin) for n in (0, 1, 2, 3, 5, 9, 17, 33)
            for size in (1, 2, 3, 7) for fanin in (2, 3)]
    seed = rng_seed
    for n, size, fanin in grid:
        seed += 1
        rows = [("k%02d" % ((seed * 7 + i * 3) % 11),
                 (seed + i * 5) % 13 - 6) for i in range(n)]
        inner = StubDriver()
        driver = PortOnlyProxy(inner)
        source = CountingInput(rows)
        cells = list(order(source, driver, CustomRules(), size, fanin))

        got = [(c.k, c.n, c.v) for c in cells]
        assert got == reference_opaque(rows), (n, size, fanin)
        # Keys are globally ordered and equal keys were merged away.
        keys = [c.k for c in cells]
        assert keys == sorted(keys)
        assert len(keys) == len(set(keys))

        # Single pass: every row pulled exactly once; re-entering iteration
        # after exhaustion is rejected, and order never closes the source.
        assert source.pulls == n and source.exhausted
        try:
            iter(source)
        except AssertionError:
            pass
        else:
            raise AssertionError("second pass over input was allowed")
        assert source.closed is False
        # No cleanup port even exists; final segments stay owned by the job.
        assert not hasattr(inner, "cleanup")
        if n:
            assert inner.data, "final segments must remain after order ends"
        # Each segment is written once and read at most once, sequentially.
        assert all(count == 1 for count in inner.write_opens.values())
        assert all(count <= 1 for count in inner.read_opens.values())
        # At most fanin read heads plus one write handle.
        assert inner.peak <= fanin + 1, inner.peak
        # Removed segments were always consumed by a merge first.
        for name in inner.removed:
            assert inner.read_opens[name] == 1

    # Explicit singleton tail batch: it must still be rewritten, not skipped.
    inner = StubDriver()
    rows = [("k", i) for i in range(3)]
    out = list(order(CountingInput(rows), PortOnlyProxy(inner),
                     CustomRules(), 1, 2))
    # Three initial runs s1..s3, fanin 2 -> merge round: [s1,s2]->s4 then the
    # singleton batch [s3]->s5 is read and rewritten anyway; final merge of
    # s4,s5 produces the ordered, same-key-merged output.
    assert [(c.k, c.n, c.v) for c in out] == [("k", 3, 3)]
    assert len(inner.write_opens) == 5
    singleton = "s0003"
    assert inner.read_opens[singleton] == 1 and singleton in inner.removed
    assert "s0005" in inner.data and "s0004" in inner.data
    # The final two segments remain owned (outer lifecycle), never removed.
    assert set(inner.data) == {"s0004", "s0005"}
    assert "s0004" not in inner.removed and "s0005" not in inner.removed

    # Injected driver failure propagates; every opened token is released and
    # order still does not close the input or invoke cleanup.
    inner = StubDriver()
    inner._fail = ("write", [2])
    source = CountingInput([("k", 1), ("k", 2), ("j", 3), ("j", 4)])
    try:
        list(order(source, PortOnlyProxy(inner), CustomRules(), 1, 2))
    except RuntimeError as error:
        assert str(error) == "injected:write"
    else:
        raise AssertionError("driver failure was swallowed")
    assert inner.open_tokens == {}
    assert source.closed is False

    # Early generator close releases merge handles but keeps final segments
    # and the input open; ownership stays with the outer lifecycle.
    inner = StubDriver()
    source = CountingInput([("k", 1), ("j", 2), ("k", 3), ("m", 4)])
    gen = order(source, PortOnlyProxy(inner), CustomRules(), 1, 2)
    next(gen)
    gen.close()
    assert inner.open_tokens == {}
    assert inner.data and source.closed is False
    print("PART2_ORDER_OPAQUE_OK")


# --------------------------------------------------------------------------
# Part 3: driver ownership, reverse cleanup and error collection
# --------------------------------------------------------------------------

def part_driver_ownership():
    # allocate registers ownership; tokens are opaque hashables, not handles.
    ws = MemoryWorkspace()
    driver = Driver(ws)
    name = driver.allocate()
    token = driver.open_write(name)
    assert isinstance(token, Hashable) and not isinstance(token, Handle)
    driver.write(token, ["g", None, 1, 0])
    driver.close(token)
    try:
        driver.read(token)
    except KeyError:
        pass
    else:
        raise AssertionError("closed token still accepted")
    driver.remove(name)
    assert driver.cleanup() == []  # idempotent, nothing left registered

    # A failing close still releases the token; the handle itself is closed.
    ws = MemoryWorkspace(trace=[], fail={"close": [1]})
    driver = Driver(ws)
    name = driver.allocate()
    token = driver.open_write(name)
    driver.write(token, ["g", "d", 1, 1])
    try:
        driver.close(token)
    except OSError as error:
        assert str(error) == "close:1"
    else:
        raise AssertionError("close fault did not propagate")
    assert ws.live == [] and token not in driver._tokens
    errors = driver.cleanup()
    assert [str(e) for e in errors] == [] and ws.remaining() == []

    # A failing remove still drops ownership and is never retried.
    ws = MemoryWorkspace(trace=[], fail={"remove": [1]})
    driver = Driver(ws)
    name = driver.allocate()
    token = driver.open_write(name)
    driver.close(token)
    try:
        driver.remove(name)
    except OSError as error:
        assert str(error) == "remove:1"
    else:
        raise AssertionError("remove fault did not propagate")
    assert name not in driver._owned
    remove_events = [event for event in ws.trace if event[0] == "remove"]
    assert len(remove_events) == 1  # the faulted remove, recorded once
    assert driver.cleanup() == []
    # cleanup adds no retry and the file is already gone.
    assert [event for event in ws.trace if event[0] == "remove"] == remove_events
    assert ws.remaining() == []

    # Reverse cleanup continues through every failure and returns errors in
    # occurrence order without replacing a primary exception of its own.
    ws = MemoryWorkspace(trace=[], fail={"close": [2], "remove": [2]})
    driver = Driver(ws)
    a, b, c = driver.allocate(), driver.allocate(), driver.allocate()
    ta, tc = driver.open_write(a), driver.open_write(c)
    driver.write(ta, ["a", None, 1, 0])
    driver.write(tc, ["c", None, 1, 0])
    errors = driver.cleanup()
    # Tokens close in reverse (c then a): the second close is faulted.
    # Names remove in reverse (c, b, a): the second remove is faulted.
    assert [str(e) for e in errors] == ["close:2", "remove:2"], errors
    assert ws.live == [] and ws.remaining() == []
    assert driver.cleanup() == []
    trace_ops = [event[0] for event in ws.trace]
    close_names = [event[1] for event in ws.trace if event[0] == "close"]
    remove_names = [event[1] for event in ws.trace if event[0] == "remove"]
    assert close_names == [c, a], close_names
    assert remove_names == [c, b, a], remove_names
    assert trace_ops.index("close") < trace_ops.index("remove")

    # Same guarantees hold against the real filesystem.
    import tempfile
    with tempfile.TemporaryDirectory(prefix="driver-owner-") as root:
        real = Workspace(root)
        driver = Driver(real)
        names = [driver.allocate() for _ in range(3)]
        tokens = [driver.open_write(n) for n in names]
        for token, name in zip(tokens, names):
            driver.write(token, ["g", "d", 1, 1])
        assert driver.cleanup() == []
        assert real.live == [] and real.remaining() == []
        assert list(Path(root).iterdir()) == []

    # Driver surface contains only resource operations, no rule or merge API.
    import types
    methods = {name for name, value in vars(Driver).items()
               if isinstance(value, types.FunctionType)}
    assert methods == {"__init__", "_open", "allocate", "open_read",
                       "open_write", "read", "write", "close", "remove",
                       "cleanup"}, methods
    public = {name for name in methods if not name.startswith("_")}
    assert public == {"allocate", "open_read", "open_write", "read",
                      "write", "close", "remove", "cleanup"}, public
    for forbidden in ("lift", "key", "combine", "finish"):
        assert not hasattr(Driver, forbidden), forbidden
    print("PART3_DRIVER_OWNERSHIP_OK")


# --------------------------------------------------------------------------
# Part 4: import and call dependency boundaries
# --------------------------------------------------------------------------

def _tree(module_file):
    return ast.parse((ROOT / module_file).read_text(encoding="utf-8"))


def _imports(tree):
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add(node.module or "")
    return names


def _identifiers(tree):
    """All Name identifiers and attribute names used by real code nodes."""
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
    return names


def _called_names(tree):
    called = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                called.add(func.id)
            elif isinstance(func, ast.Attribute):
                called.add(func.attr)
    return called


def part_dependency_boundaries():
    sources = {name: (ROOT / name).read_text(encoding="utf-8")
               for name in PRODUCTION}
    trees = {name: _tree(name) for name in PRODUCTION}
    ids = {name: _identifiers(tree) for name, tree in trees.items()}

    # No dynamic imports or evaluation anywhere in production code.
    for name, tree in trees.items():
        assert not (_called_names(tree)
                    & {"eval", "exec", "compile", "__import__"}), name
        assert "importlib" not in _imports(tree)

    # reduction: no imports; its identifiers never name I/O resources.
    assert _imports(trees["reduction.py"]) == set()
    assert not (ids["reduction.py"]
                & {"workspace", "Workspace", "Handle", "driver", "Driver",
                   "open", "read", "write", "cleanup", "order"})

    # driver: no imports; its identifiers never name rule or merge concepts.
    assert _imports(trees["driver.py"]) == set()
    assert not (ids["driver.py"]
                & {"reduction", "Rules", "sorter", "pipeline", "heapq",
                   "lift", "combine", "finish", "collapse", "merge"})
    assert "key" not in _called_names(trees["driver.py"])

    # sorter: only heapq from the stdlib; never names workspace/handle or
    # rule/driver types, never calls finish or a cleanup port.
    assert _imports(trees["sorter.py"]) == {"heapq"}
    assert not (ids["sorter.py"]
                & {"workspace", "Workspace", "MemoryWorkspace", "Handle",
                   "Rules", "Driver", "reduction", "pipeline", "spill"})
    called_attrs = _called_names(trees["sorter.py"])
    assert "finish" not in called_attrs
    assert "cleanup" not in called_attrs
    # The only driver object ever called is exactly the mandated port set.
    driver_ports = {"open_read", "open_write", "read", "write", "close",
                    "remove", "allocate"}
    driver_calls = {
        node.func.attr for node in ast.walk(trees["sorter.py"])
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "driver"
    }
    assert driver_calls <= driver_ports, driver_calls
    # Subscripting happens only on local lists (chunk, runs, part, tokens and
    # heap tuples); cells flow through rules, never through literal cell[..].
    for node in ast.walk(trees["sorter.py"]):
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
            assert node.value.id in {"chunk", "runs", "part", "tokens",
                                     "following", "names", "errors",
                                     "actions"}, node.value.id

    # pipeline: wires exactly the three collaborators; no sorting/merge code.
    assert _imports(trees["pipeline.py"]) == {
        "driver", "reduction", "sorter"}
    assert not (ids["pipeline.py"]
                & {"heapq", "Workspace", "MemoryWorkspace", "Handle",
                   "heappush", "heappop", "collapse", "merge"})
    pipeline_calls = _called_names(trees["pipeline.py"])
    assert "sort" not in pipeline_calls
    # pipeline interprets no business fields: the only subscripts are on the
    # local cleanup-error list, never on records, cells or results.
    for node in ast.walk(trees["pipeline.py"]):
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
            assert node.value.id == "errors", node.value.id
    assert not any(
        isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name)
        and node.value.id in {"cell", "record", "result", "row"}
        for node in ast.walk(trees["pipeline.py"]))
    # The only rules method it drives is the streaming finish.
    rule_calls = {
        node.func.attr for node in ast.walk(trees["pipeline.py"])
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "rules"
    }
    assert rule_calls == {"finish"}, rule_calls

    # spill: pure forwarding shim, no retained executor vocabulary.
    assert _imports(trees["spill.py"]) == {"pipeline"}
    assert not (ids["spill.py"]
                & {"heapq", "workspace", "merge", "collapse", "Driver",
                   "Rules", "order"})
    assert "execute" not in ids["spill.py"]

    # No hardcoded business answers copied from the frozen sample.
    for name, text in sources.items():
        for answer in ('("a", 6, 11, 3)', "('a', 6, 11, 3)",
                       '["a", 6, 11, 3]'):
            assert answer not in text, (name, answer)

    # Runtime globals confirm the scheduler never loads file/rule types and
    # the rules instance carries no stateful collaborators.
    import driver as driver_mod
    import reduction as reduction_mod
    import sorter as sorter_mod
    import pipeline as pipeline_mod
    sorter_globals = set(vars(sorter_mod))
    for leaked in ("Workspace", "MemoryWorkspace", "Handle", "Driver",
                   "Rules"):
        assert leaked not in sorter_globals, leaked
    assert vars(Rules()) == {}
    assert hasattr(pipeline_mod, "aggregate")
    assert callable(getattr(driver_mod.Driver, "cleanup"))
    print("PART4_DEPENDENCY_BOUNDARIES_OK")


def main():
    part_rules_purity()
    part_order_boundaries()
    part_driver_ownership()
    part_dependency_boundaries()
    print("STRUCTURE_OK")


if __name__ == "__main__":
    main()
