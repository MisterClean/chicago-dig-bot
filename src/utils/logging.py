"""Logging configuration for the Chicago Dig Bot."""
import logging
import logging.handlers
import os
from pathlib import Path
import colorlog
from src.config import config

def setup_logging():
    """Configure logging based on settings in config.yaml."""
    # Create logs directory if it doesn't exist
    log_file = Path(config.logging_config['file'])
    log_dir = log_file.parent
    if not log_dir.exists():
        os.makedirs(log_dir)

    # Create standard formatter for file output
    file_formatter = logging.Formatter(config.logging_config['format'])

    # Create color formatter for console output
    console_formatter = colorlog.ColoredFormatter(
        "%(log_color)s" + config.logging_config['format'],
        log_colors={
            'DEBUG': 'blue',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bold',
        }
    )

    # Configure rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(
        filename=config.logging_config['file'],
        maxBytes=config.logging_config['rotation']['max_bytes'],
        backupCount=config.logging_config['rotation']['backup_count']
    )
    file_handler.setFormatter(file_formatter)

    # Configure colored console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(config.logging_config['level'])
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Log startup message
    root_logger.info("Chicago Dig Bot logging initialized")

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a specific module.
    
    Args:
        name: The name of the module requesting the logger.
        
    Returns:
        A configured logger instance.
    """
    return logging.getLogger(name)
