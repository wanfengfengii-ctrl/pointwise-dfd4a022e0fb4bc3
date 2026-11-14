"""Unified verification entry.

Runs, in one process:
  1. the original frozen behavior/trace/fault-priority verification (checks.py);
  2. the real-filesystem disk comparison with residue check (checks.py disk);
  3. the new structural responsibility/ownership/boundary verification
     (structure_checks.py).

checks.py, samples.json, expected.json and workspace.py stay untouched; this
runner only invokes their existing public functions.
"""
import checks
import structure_checks


def main():
    checks.verify()
    checks.disk()
    structure_checks.main()
    print("UNIFIED_VERIFY_OK")


if __name__ == "__main__":
    main()
