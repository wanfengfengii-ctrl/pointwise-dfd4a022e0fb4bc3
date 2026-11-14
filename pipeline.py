"""Outer assembly and lifecycle for the external group aggregation.

Mirrors the legacy ``aggregate`` signature. This module only validates
parameters, wires the pure rules, the file-owning driver and the ordering
scheduler together, and owns the input iterator, the final output and the
cleanup error priority. It interprets no business fields and contains no
sorting or merge loop.
"""
from driver import Driver
from reduction import Rules
from sorter import order


def aggregate(records, workspace, rows_per_run=1000, fanin=8):
    if type(rows_per_run) is not int or rows_per_run < 1:
        raise ValueError("rows_per_run")
    if type(fanin) is not int or fanin < 2:
        raise ValueError("fanin")

    def execute():
        rules = Rules()
        driver = Driver(workspace)
        source = None
        ordered = None
        finished = None
        failed = False
        try:
            source = iter(records)
            ordered = order(source, driver, rules, rows_per_run, fanin)
            finished = rules.finish(ordered)
            for result in finished:
                yield result
        except BaseException:
            failed = True
            raise
        finally:
            errors = []
            close_input = getattr(source, "close", None)
            if close_input is not None:
                try:
                    close_input()
                except BaseException as error:
                    errors.append(error)
            if ordered is not None:
                try:
                    ordered.close()
                except BaseException as error:
                    errors.append(error)
            if finished is not None:
                try:
                    finished.close()
                except BaseException as error:
                    errors.append(error)
            errors.extend(driver.cleanup())
            if errors and not failed:
                raise errors[0]

    return execute()
