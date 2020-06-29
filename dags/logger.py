import logging
import sys


FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")

logger = logging.getLogger()
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(FORMATTER)
logger.addHandler(console_handler)
