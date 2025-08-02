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
from pathlib import Path
from typing import Optional

import typer
import yaml
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

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "AppConfig":
        """Load configuration from YAML file"""
        try:
            with open(yaml_path, 'r') as f:
                yaml_data = yaml.safe_load(f)

            if yaml_data is None:
                yaml_data = {}

            config_data = {}

            if 'redis' in yaml_data:
                config_data['redis'] = RedisConfig(**yaml_data['redis'])

            if 'zmq' in yaml_data:
                config_data['zmq'] = ZmqConfig(**yaml_data['zmq'])

            if 'log_level' in yaml_data:
                log_level_str = yaml_data['log_level']
                if isinstance(log_level_str, str):
                    level_map = {
                        'DEBUG': logging.DEBUG,
                        'INFO': logging.INFO,
                        'WARNING': logging.WARNING,
                        'ERROR': logging.ERROR
                    }
                    config_data['log_level'] = level_map.get(log_level_str.upper())
                else:
                    config_data['log_level'] = log_level_str

            return cls(**config_data)
        except FileNotFoundError:
            raise typer.BadParameter(f"Configuration file not found: {yaml_path}")
        except yaml.YAMLError as e:
            raise typer.BadParameter(f"Invalid YAML in configuration file: {e}")
        except Exception as e:
            raise typer.BadParameter(f"Error loading configuration: {e}")


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
    config: Optional[Path] = typer.Option(
        None,
        "--config", "-c",
        help="Path to YAML configuration file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True
    ),
    redis_host: Optional[str] = typer.Option(
        None,
        "--redis-host", "--redis_host",
        help="Redis host for bliss data (overrides config file)"
    ),
    redis_port: Optional[int] = typer.Option(
        None,
        "--redis-port", "--redis_port",
        help="Redis connection port (overrides config file)",
        min=1, max=65535
    ),
    zmq_host: Optional[str] = typer.Option(
        None,
        "--zmq-host", "--zmq_host",
        help="ZMQ host for bluesky RemoteDispatcher (overrides config file)"
    ),
    zmq_port: Optional[int] = typer.Option(
        None,
        "--zmq-port", "--zmq_port",
        help="ZMQ port for bluesky RemoteDispatcher (overrides config file)",
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

    Configuration precedence (highest to lowest):
    1. Command-line arguments
    2. YAML configuration file
    3. Default values
    """
    try:
        if config:
            app_config = AppConfig.from_yaml(config)
        else:
            app_config = AppConfig()

        redis_config_data = {}
        if redis_host is not None:
            redis_config_data['host'] = redis_host
        else:
            redis_config_data['host'] = app_config.redis.host

        if redis_port is not None:
            redis_config_data['port'] = redis_port
        else:
            redis_config_data['port'] = app_config.redis.port

        zmq_config_data = {}
        if zmq_host is not None:
            zmq_config_data['host'] = zmq_host
        else:
            zmq_config_data['host'] = app_config.zmq.host

        if zmq_port is not None:
            zmq_config_data['port'] = zmq_port
        else:
            zmq_config_data['port'] = app_config.zmq.port

        cli_log_level = (
            logging.DEBUG if very_verbose
            else (logging.INFO if verbose else None)
        )
        final_log_level = (
            cli_log_level if cli_log_level is not None else app_config.log_level
        )

        final_config = AppConfig(
            redis=RedisConfig(**redis_config_data),
            zmq=ZmqConfig(**zmq_config_data),
            log_level=final_log_level
        )
    except Exception as e:
        typer.echo(f"Configuration error: {e}", err=True)
        raise typer.Exit(1)

    setup_logging(final_config.log_level)

    _logger.info("starting bluesky_blissdata")
    _logger.info(
        f"Connection to redis server: "
        f"{final_config.redis.host}:{final_config.redis.port}"
    )
    _logger.info(
        f"Connection to zmq server: "
        f"{final_config.zmq.host}:{final_config.zmq.port}"
    )

    d = RemoteDispatcher((final_config.zmq.host, final_config.zmq.port))
    post_document = BlissdataDispatcher(
        final_config.redis.host, final_config.redis.port
    )

    d.subscribe(post_document)
    d.start()

    _logger.info("stopping bluesky_blissdata")


if __name__ == "__main__":
    app()
