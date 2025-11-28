import logging
import logging.handlers
import os

def setup_logger(log_file="app.log"):
    logger = logging.getLogger("myApp")
    logger.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s"
    )

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File Handler (Rotating files)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5_000_000, backupCount=5
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()

logger.info("Application started")
logger.warning("Low disk space")
logger.error("Failed to read input file")