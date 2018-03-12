#!/usr/bin/env python3
import sys, re, time


CSK_RE = re.compile('CSKS(?P<sat>\d)_.*?_(?P<start_date>\d{8})(?P<start_time>\d{6})_(?P<end_date>\d{8})(?P<end_time>\d{6})')


def main(f):
    """Get ASI tarball name."""

    m = CSK_RE.search(f)
    if m is None: raise RuntimeError("Failed to match %s." % f)
    return "EL{}_{}_{}{}.tar.gz".format(m.group('start_date'), m.group('start_time'),
                                            m.group('sat'), m.group('end_time'))


if __name__ == "__main__":
    print(main(sys.argv[1]))
