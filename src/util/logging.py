"""
Purpose:
    Provides centralized logging utilities for all modules.
 
Responsibilities:
    - Configure structured logging with timestamps, log levels, and module names.
    - Standardize logging format across streaming, batch, and Kafka modules.
    - Optionally log to console, file, or external monitoring system.
 
Important Behavior:
    - Supports different log levels: DEBUG, INFO, WARNING, ERROR.
    - Captures exceptions and errors consistently for observability.
    - Can be integrated with Airflow or standalone Spark jobs.
 
Design Notes:
    - Ensures consistent logging across the pipeline.
    - Makes it easier to monitor, debug, and audit the Amazon pipeline in production.
"""
 
import logging
import sys
 
def get_logger(name: str, level: str = "INFO", log_file: str = None) -> logging.Logger:
    """
    Returns a configured logger.
   
    Args:
        name: Logger name (usually __name__ of the module)
        level: Log level as string: DEBUG, INFO, WARNING, ERROR
        log_file: Optional file path to save logs; if None, logs to console
   
    Returns:
        Configured logging.Logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
   
    # Avoid adding multiple handlers if logger already configured
    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
       
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
       
        # File handler (optional)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
   
    return logger