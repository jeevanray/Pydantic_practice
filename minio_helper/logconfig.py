import logging
 
DEFAULT_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - PID:%(process)d - TID:%(thread)d - %(message)s"
 
 
def configure_logging(level: int = logging.INFO, fmt: str = DEFAULT_FORMAT) -> None:
    """Configure root logging once. Safe to call multiple times."""
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        root.addHandler(handler)
        root.setLevel(level)
 
 
def get_logger(name: str | None = None, level: int = logging.INFO) -> logging.Logger:
    """Return a logger configured to use the single root handler.
 
    Ensures logging is configured once and returns a named logger (or root when name is None).
    """
    configure_logging(level)
    return logging.getLogger(name)