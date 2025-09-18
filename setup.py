import platform
import sys

import setuptools

if platform.system() != "Windows":
    sys.exit("This package is only supported on Windows.")

setuptools.setup()
