"""Utility functions for end-to-end tests."""

import logging

def log_start_and_finish(func: callable) -> callable:
    """Add the start and end events of a function to the log file."""
    def logger(*args, **kwargs):
        logging.debug(f"Starting {func.__name__}.")
        output = func(*args, **kwargs)
        logging.debug(f"Finished {func.__name__}.")
        return output
    return logger


def _setup_logging(filename: str):
    """Configure Python logging.
    
    Args:
      filename (str): filename to write logs to.
    """
    logging.basicConfig(
        filename=filename,
        filemode="a+",
        format="|%(levelname)s\t| %(asctime)s -- %(message)s",
        datefmt="20%y-%m-%d %I:%M:%S %p",
        level=logging.DEBUG,
        )
    logging.debug("Logging started.")