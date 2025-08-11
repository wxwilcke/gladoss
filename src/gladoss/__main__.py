#!/usr/bin/env python

import sys

from gladoss.run import __main__ as run
from gladoss.demo.demo_device import __main__ as demo


if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        demo(sys.argv[2:])
    else:
        run()
