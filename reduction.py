"""Pure reduction rules for external group aggregation.

The rules only read and return values: no workspace, handles, I/O callbacks,
mutable globals or any scheduling/cleanup responsibility.
"""


class Rules:
    def lift(self, record):
        """Validate one input row and return the segment cell it maps to."""
        if (not isinstance(record, (tuple, list)) or len(record) != 3
                or not isinstance(record[0], str)
                or not (record[1] is None or isinstance(record[1], str))
                or type(record[2]) is not int):
            raise ValueError("record")
        return [record[0], record[1], 1, record[2]]

    def key(self, cell):
        """Sortable key; None devices order before every string device."""
        device = cell[1]
        return cell[0], device is not None, device or ""

    def combine(self, left, right):
        """Merge two cells sharing one key into a fresh cell."""
        return [left[0], left[1], left[2] + right[2], left[3] + right[3]]

    def finish(self, cells):
        """Stream cells already merged by key into public output tuples."""
        group = None
        count, amount, devices = 0, 0, 0
        for cell in cells:
            if group is not None and group != cell[0]:
                yield (group, count, amount, devices)
                count, amount, devices = 0, 0, 0
            group = cell[0]
            count += cell[2]
            amount += cell[3]
            devices += cell[1] is not None
        if group is not None:
            yield (group, count, amount, devices)
