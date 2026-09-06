"""Lossless raw frame sink, separated from the client's event loop."""

import gzip
from pathlib import Path
import sys


if __name__ == "__main__":
    with gzip.open(Path(sys.argv[1]), "wb", compresslevel=1) as output:
        for line in sys.stdin.buffer:
            output.write(line)
