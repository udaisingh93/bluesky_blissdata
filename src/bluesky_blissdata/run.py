"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following lines in the
``[options.entry_points]`` section in ``setup.cfg``::

    console_scripts =
         fibonacci = bluesky_blissdata.skeleton:run

Then run ``pip install .`` (or ``pip install -e .`` for editable mode)
which will install the command ``fibonacci`` inside your current environment.

Besides console scripts, the header (i.e. until ``_logger``...) of this file can
also be used as template for Python modules.

Note:
    This file can be renamed depending on your needs or safely removed if not needed.

References:
    - https://setuptools.pypa.io/en/latest/userguide/entry_point.html
    - https://pip.pypa.io/en/stable/reference/pip_install
"""

import argparse
import logging
import sys
from bluesky.callbacks.zmq import RemoteDispatcher
from bluesky_blissdata.dispacher import blissdata_dispacher
from bluesky_blissdata import __version__
from docopt import docopt, DocoptExit
__author__ = "Udai Singh"
__copyright__ = "Udai Singh"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


# ---- Python API ----
# The functions defined in this section can be imported by users in their
# Python scripts/interactive interpreter, e.g. via
# `from bluesky_blissdata.skeleton import fib`,
# when using this Python module as a library.

# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.
def parse_args(args):
        """ Parse command line parameters

        Args:
        args (List[str]): command line parameters as list of strings
            (for example  ``["--help"]``).

        Returns:
            :obj:`argparse.Namespace`: command line parameters namespace
            """
        parser = argparse.ArgumentParser(description="Blusesky blissdata interface")
        parser.add_argument(
            "--version",
            action="version",
            version=f"bluesky-blissdata {__version__}",
        )
        parser.add_argument(
            "--redis-host",
            "--redis_host",
            dest="redis_host",
            default="localhost", 
            help="redis host for bliss data")
        parser.add_argument(
            "--redis-port",
            "--redis_port",
            dest="redis_port",
            default=6379,
            type=int,
            help="redis connection port",
            )
        parser.add_argument(
            "--zmq-host",
            "--zmq_host",
            dest="zmq_host",
            default="localhost", 
            help="zmq host for bluesky RemoteDispacher")
        parser.add_argument(
            "--zmq-port",
            "--zmq_port",
            dest="zmq_port",
            default=5578,
            type=int,
            help="zmq port for bluesky RemoteDispachert",
            )
        parser.add_argument(
            "-v",
            "--verbose",
            dest="loglevel",
            help="set loglevel to INFO",
            action="store_const",
            const=logging.INFO,
        )
        parser.add_argument(
            "-vv",
            "--very-verbose",
            dest="loglevel",
            help="set loglevel to DEBUG",
            action="store_const",
            const=logging.DEBUG,
        )
        return parser.parse_args(args)



def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )

def main(argv=None) -> int:
    args = parse_args(sys.argv[1:])
    setup_logging(args.loglevel)
    _logger.debug("Starting bluesky_blissdata...")
    d = RemoteDispatcher((args.zmq_host, args.zmq_port))
    
    post_document=blissdata_dispacher(args.redis_host,args.redis_port)
    d.subscribe(post_document)
    d.start()
    _logger.info("stoping bluesky_blissdata")

if __name__ == "__main__":
    sys.exit(main())

