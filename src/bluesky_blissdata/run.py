"""
Bluesky Blissdata Interface

This script serves as a command-line interface (CLI) for connecting a Bluesky-based data acquisition system with a Redis server. It enables seamless data streaming by managing Redis and ZMQ connections and integrating with the BlissdataDispatcher class for scan lifecycle management.

Key Features:
    - Initializes connections to Redis and ZMQ servers for data handling.
    - Integrates with Bluesky's RemoteDispatcher for data streaming.
    - Utilizes BlissdataDispatcher to handle scan documents and manage data flow to Redis streams.

Command-line Arguments:
    - --version: Displays the version of the bluesky-blissdata package.
    - --redis-host: Specifies the Redis server host (default: localhost).
    - --redis-port: Specifies the Redis server port (default: 6379).
    - --zmq-host: Specifies the ZMQ host for the Bluesky RemoteDispatcher (default: localhost).
    - --zmq-port: Specifies the ZMQ port for the Bluesky RemoteDispatcher (default: 5578).
    - -v / --verbose: Sets the logging level to INFO.
    - -vv / --very-verbose: Sets the logging level to DEBUG.

Usage Example:
    To run the script, install it via pip:
        pip install .
    
    Then execute:
        fibonacci

    The script will:
        1. Parse command-line arguments.
        2. Set up logging.
        3. Initialize connections to the Redis and ZMQ servers.
        4. Start listening for scan documents and push data to Redis streams using the BlissdataDispatcher.

Dependencies:
    - Bluesky
    - ZMQ
    - Redis
    - Logging

Author: Udai Singh
License: MIT
"""

import argparse
import logging
import sys
from bluesky.callbacks.zmq import RemoteDispatcher
from bluesky_blissdata.dispatcher import BlissdataDispatcher
from bluesky_blissdata import __version__

__author__ = "Udai Singh"
__copyright__ = "Udai Singh"
__license__ = "MIT"

_logger = logging.getLogger(__name__)

def parse_args(args):
    """ Parse command line parameters

    Args:
        args (List[str]): command line parameters as list of strings
            (for example  ``["--help"]``).

    Returns:
        :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="Bluesky blissdata interface")

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
        help="redis host for bliss data"
    )

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
        help="zmq host for bluesky RemoteDispatcher"
    )

    parser.add_argument(
        "--zmq-port",
        "--zmq_port",
        dest="zmq_port",
        default=5578,
        type=int,
        help="zmq port for bluesky RemoteDispatcher",
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
        level=loglevel,
        stream=sys.stdout,
        format=logformat,
        datefmt="%d-%m-%Y %H:%M:%S"
    )


def main() -> None:
    """Main entry point for the script

    This function parses command-line arguments, sets up logging, initializes the
    Redis and ZMQ connections, and starts listening for scan documents. It integrates
    the Bluesky RemoteDispatcher and BlissdataDispatcher to push scan data to Redis streams.

    Returns:
        None
    """
    args = parse_args(sys.argv[1:])
    setup_logging(args.loglevel)

    _logger.info("starting bluesky_blissdata")
    _logger.info(f"Connection to redis sever: {args.redis_host}:{args.redis_port}")
    _logger.info(f"Connection to zmq sever: {args.zmq_host}:{args.zmq_port}")

    # Initialize RemoteDispatcher for Bluesky
    d = RemoteDispatcher((args.zmq_host, args.zmq_port))
    post_document = BlissdataDispatcher(args.redis_host, args.redis_port)

    # Subscribe the BlissdataDispatcher to the RemoteDispatcher
    d.subscribe(post_document)
    d.start()

    _logger.info("stopping bluesky_blissdata")


if __name__ == "__main__":
    sys.exit(main())
