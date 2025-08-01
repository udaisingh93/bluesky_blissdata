"""
Bluesky Blissdata Interface

This script serves as a command-line interface (CLI) for connecting a Bluesky-based data
acquisition system with a Redis server. It enables seamless data streaming by managing
Redis and ZMQ connections and integrating with the BlissdataDispatcher class for scan
lifecycle management.

Key Features:
    - Initializes connections to Redis and ZMQ servers for data handling.
    - Integrates with Bluesky's RemoteDispatcher for data streaming.
    - Utilizes BlissdataDispatcher to handle scan documents and manage data flow to
      Redis streams.

Command-line Arguments:
    - --version: Displays the version of the bluesky-blissdata package.
    - --redis-host: Specifies the Redis server host (default: localhost).
    - --redis-port: Specifies the Redis server port (default: 6379).
    - --zmq-host: Specifies the ZMQ host for the Bluesky RemoteDispatcher
        (default: localhost).
    - --zmq-port: Specifies the ZMQ port for the Bluesky RemoteDispatcher
        (default: 5578).
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
        4. Start listening for scan documents and push data to Redis streams using the
           BlissdataDispatcher.

Dependencies:
    - Bluesky
    - ZMQ
    - Redis
    - Logging

Author: Udai Singh
License: MIT
"""

import logging
import sys
from typing import Optional

import typer
from pydantic import BaseModel, Field, field_validator
from bluesky.callbacks.zmq import RemoteDispatcher
from bluesky_blissdata.dispatcher import BlissdataDispatcher
from bluesky_blissdata import __version__

__author__ = "Udai Singh"
__copyright__ = "Udai Singh"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


class RedisConfig(BaseModel):
    """Redis server configuration"""
    host: str = Field(default="localhost", description="Redis host for bliss data")
    port: int = Field(default=6379, ge=1, le=65535, description="Redis connection port")


class ZmqConfig(BaseModel):
    """ZMQ server configuration"""
    host: str = Field(
        default="localhost", description="ZMQ host for bluesky RemoteDispatcher"
    )
    port: int = Field(
        default=5578,
        ge=1,
        le=65535,
        description="ZMQ port for bluesky RemoteDispatcher"
    )


class AppConfig(BaseModel):
    """Application configuration"""
    redis: RedisConfig = Field(default_factory=RedisConfig)
    zmq: ZmqConfig = Field(default_factory=ZmqConfig)
    log_level: Optional[int] = Field(default=None, description="Logging level")

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]
        if v is not None and v not in valid_levels:
            raise ValueError('Invalid log level')
        return v


app = typer.Typer(help="Bluesky blissdata interface")


def version_callback(value: bool):
    """Show version and exit"""
    if value:
        typer.echo(f"bluesky-blissdata {__version__}")
        raise typer.Exit()


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%d-%m-%Y %H:%M:%S"
    )


@app.command()
def main(
    redis_host: str = typer.Option(
        "localhost",
        "--redis-host", "--redis_host",
        help="Redis host for bliss data"
    ),
    redis_port: int = typer.Option(
        6379,
        "--redis-port", "--redis_port",
        help="Redis connection port",
        min=1, max=65535
    ),
    zmq_host: str = typer.Option(
        "localhost",
        "--zmq-host", "--zmq_host",
        help="ZMQ host for bluesky RemoteDispatcher"
    ),
    zmq_port: int = typer.Option(
        5578,
        "--zmq-port", "--zmq_port",
        help="ZMQ port for bluesky RemoteDispatcher",
        min=1, max=65535
    ),
    verbose: bool = typer.Option(
        False,
        "-v", "--verbose",
        help="Set loglevel to INFO"
    ),
    very_verbose: bool = typer.Option(
        False,
        "-vv", "--very-verbose",
        help="Set loglevel to DEBUG"
    ),
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit"
    )
) -> None:
    """Main entry point for the script

    This function parses command-line arguments, sets up logging, initializes the
    Redis and ZMQ connections, and starts listening for scan documents. It integrates
    the Bluesky RemoteDispatcher and BlissdataDispatcher to push scan data to Redis
    streams.
    """
    try:
        config = AppConfig(
            redis=RedisConfig(host=redis_host, port=redis_port),
            zmq=ZmqConfig(host=zmq_host, port=zmq_port),
            log_level=(
                logging.DEBUG if very_verbose
                else (logging.INFO if verbose else None)
            )
        )
    except Exception as e:
        typer.echo(f"Configuration error: {e}", err=True)
        raise typer.Exit(1)

    setup_logging(config.log_level)

    _logger.info("starting bluesky_blissdata")
    _logger.info(f"Connection to redis server: {config.redis.host}:{config.redis.port}")
    _logger.info(f"Connection to zmq server: {config.zmq.host}:{config.zmq.port}")

    d = RemoteDispatcher((config.zmq.host, config.zmq.port))
    post_document = BlissdataDispatcher(config.redis.host, config.redis.port)

    d.subscribe(post_document)
    d.start()

    _logger.info("stopping bluesky_blissdata")


if __name__ == "__main__":
    app()
