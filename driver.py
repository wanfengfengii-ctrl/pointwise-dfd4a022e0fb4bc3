"""Exclusive file-resource owner for one aggregation run.

The driver is the only component that holds Workspace Handles, live handle
registrations and the registry of segments owned by this job. The scheduling
layer only ever sees opaque, hashable tokens; it must not inspect them.
"""


class Driver:
    def __init__(self, workspace):
        self._workspace = workspace
        self._owned = []
        self._tokens = {}
        self._serial = 0

    def allocate(self):
        """Reserve a segment name and register this job's ownership of it."""
        name = self._workspace.allocate()
        self._owned.append(name)
        return name

    def open_read(self, name):
        return self._open(name, "r")

    def open_write(self, name):
        return self._open(name, "w")

    def _open(self, name, mode):
        handle = self._workspace.open(name, mode)
        self._serial += 1
        token = self._serial
        self._tokens[token] = handle
        return token

    def read(self, token):
        return self._tokens[token].read()

    def write(self, token, cell):
        self._tokens[token].write(cell)

    def close(self, token):
        """Release the token even when closing the handle raises."""
        handle = self._tokens.pop(token)
        handle.close()

    def remove(self, name):
        """Drop ownership even when the workspace delete raises."""
        try:
            self._workspace.remove(name)
        finally:
            self._owned.remove(name)

    def cleanup(self):
        """Close remaining handles and remove owned segments in reverse order.

        Every action is attempted; exceptions are returned in occurrence
        order and never replace an exception handled by the outer lifecycle.
        """
        errors = []
        for token in reversed(list(self._tokens)):
            try:
                self.close(token)
            except BaseException as error:
                errors.append(error)
        for name in reversed(list(self._owned)):
            try:
                self.remove(name)
            except BaseException as error:
                errors.append(error)
        return errors
