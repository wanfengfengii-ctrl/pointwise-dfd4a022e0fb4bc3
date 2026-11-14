"""Bounded external ordering and same-key merging.

``order`` owns chunk sizing, in-memory sorting, round/batch scheduling and the
heap merge, but never interprets record or cell fields: every value decision
goes through the injected rules, and every segment access goes through the
injected driver's opaque-token ports. It holds segment names, tokens, cells and
sort keys, never physical handles.

It does not own the input iterator's close and never calls driver.cleanup;
the input and the final segments stay alive until the outer lifecycle ends.
"""
import heapq


def _run_cleanup(actions, failed):
    errors = []
    for action in actions:
        if action is not None:
            try:
                action()
            except BaseException as error:
                errors.append(error)
    if errors and not failed:
        raise errors[0]


def order(records, driver, rules, rows_per_run, fanin):
    def collapse(cells):
        pending = None
        for cell in cells:
            if pending is not None and rules.key(pending) == rules.key(cell):
                pending = rules.combine(pending, cell)
            else:
                if pending is not None:
                    yield pending
                pending = cell
        if pending is not None:
            yield pending

    def merge(names):
        tokens, heap = [], []
        failed = False
        try:
            for index, name in enumerate(names):
                token = driver.open_read(name)
                tokens.append(token)
                cell = driver.read(token)
                if cell is not None:
                    heapq.heappush(heap, (rules.key(cell), index, cell))
            pending = None
            while heap:
                _, index, cell = heapq.heappop(heap)
                following = driver.read(tokens[index])
                if following is not None:
                    heapq.heappush(heap, (rules.key(following), index, following))
                if pending is not None and rules.key(pending) == rules.key(cell):
                    pending = rules.combine(pending, cell)
                else:
                    if pending is not None:
                        yield pending
                    pending = cell
            if pending is not None:
                yield pending
        except BaseException:
            failed = True
            raise
        finally:
            _run_cleanup([lambda token=t: driver.close(token)
                          for t in reversed(tokens)], failed)

    def save(cells):
        name = driver.allocate()
        token = None
        failed = False
        try:
            token = driver.open_write(name)
            for cell in cells:
                driver.write(token, cell)
        except BaseException:
            failed = True
            raise
        finally:
            _run_cleanup([getattr(cells, "close", None),
                          (lambda: driver.close(token))
                          if token is not None else None], failed)
        return name

    source = iter(records)
    runs, chunk = [], []
    for record in source:
        cell = rules.lift(record)
        chunk.append(cell)
        if len(chunk) == rows_per_run:
            chunk.sort(key=rules.key)
            runs.append(save(collapse(chunk)))
            chunk = []
    if chunk:
        chunk.sort(key=rules.key)
        runs.append(save(collapse(chunk)))
    while len(runs) > fanin:
        following = []
        for start in range(0, len(runs), fanin):
            part = runs[start:start + fanin]
            following.append(save(merge(part)))
            for name in part:
                driver.remove(name)
        runs = following
    final = merge(runs)
    for cell in final:
        yield cell
    # Natural end does not close the source or remove the final segments:
    # the outer lifecycle keeps the input and final run until the next pull
    # or close, and driver.cleanup is its exclusive responsibility.
