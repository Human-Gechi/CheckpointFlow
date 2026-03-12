import logging
import sys
from pathlib import Path


def setup_logger():
    logger = logging.getLogger("PipelineLogger")
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers if module reloads
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    terminal_handler = logging.StreamHandler(sys.stdout)
    terminal_handler.setLevel(logging.INFO)
    terminal_handler.setFormatter(formatter)

    log_path = Path(__file__).resolve().parent / "pipeline.log"
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(terminal_handler)
    logger.addHandler(file_handler)
    logger.propagate = False

    return logger


logger = setup_logger()
