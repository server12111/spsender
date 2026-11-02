"""Compatibility launcher for hosts configured to execute python main.py."""

import sys

from app.__main__ import main

if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv.append("run")
    main()
