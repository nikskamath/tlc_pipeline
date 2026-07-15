#!/usr/bin/env python3
"""
verify_versions.py — dbt Fusion edition
────────────────────
Run this in ANY environment (local machine, inside the Airflow
Docker container, in CI) to confirm the dbt Fusion engine matches
the version pinned in FUSION_VERSION.txt.

Usage:
  python scripts/verify_versions.py

Exit code 0 = version matches. Exit code 1 = mismatch or Fusion missing.
"""

import sys
import re
import subprocess
from pathlib import Path


def get_pinned_version():
    version_file = Path(__file__).resolve().parent.parent / "FUSION_VERSION.txt"
    if not version_file.exists():
        print(f"FAIL: FUSION_VERSION.txt not found at {version_file}")
        sys.exit(1)
    return version_file.read_text().strip()


def get_installed_version():
    try:
        result = subprocess.run(
            ["dbt", "--version"], capture_output=True, text=True, timeout=15
        )
    except FileNotFoundError:
        print("FAIL: 'dbt' command not found on PATH.")
        print("  Install Fusion: curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update")
        return None

    output = result.stdout + result.stderr

    if "Fusion" not in output and "fusion" not in output and "preview" not in output:
        # Could still be Fusion without the word appearing — fall back to regex only
        pass

    match = re.search(r"(\d+\.\d+\.\d+(?:-preview\.\d+)?)", output)
    if not match:
        print(f"FAIL: Could not parse a version number from 'dbt --version' output:\n{output}")
        return None

    return match.group(1)


def check_not_core():
    """
    Warns if dbt-core (the Python package) is also importable in this
    environment — a leftover from the old pip-based approach that could
    shadow Fusion depending on PATH order.
    """
    try:
        import importlib.metadata as md
        md.version("dbt-core")
        print("WARNING: dbt-core is also pip-installed in this environment.")
        print("  This project now uses dbt Fusion exclusively. Run:")
        print("  pip uninstall dbt-core dbt-snowflake dbt-databricks -y")
        return False
    except Exception:
        return True


def main():
    print("=" * 60)
    print("dbt Fusion version verification")
    print("=" * 60)

    pinned = get_pinned_version()
    installed = get_installed_version()
    core_clean = check_not_core()

    if installed is None:
        print("VERSION CHECK FAILED — Fusion not found or unparseable.")
        sys.exit(1)

    print(f"Pinned (FUSION_VERSION.txt): {pinned}")
    print(f"Installed (dbt --version):   {installed}")

    if installed != pinned:
        print("\nFAIL: Fusion version mismatch.")
        print(f"  Fix locally with: dbt system update --version {pinned}")
        sys.exit(1)

    print("\nOK: Fusion version matches pin.")
    if not core_clean:
        print("(Non-fatal warning above about leftover dbt-core — clean it up when convenient.)")

    print("=" * 60)
    print("ALL CHECKS PASSED")
    sys.exit(0)


if __name__ == "__main__":
    main()
