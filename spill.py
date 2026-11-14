"""Compatibility entry point.

The implementation now lives in pipeline.py, assembled from the pure rules
(reduction.py), the opaque-token scheduler (sorter.py) and the file-resource
owner (driver.py). Only this forwarding shim remains here; there is no legacy
closure, duplicated executor or conditional fallback.
"""
from pipeline import aggregate

__all__ = ["aggregate"]
