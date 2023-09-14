import sys
from simple_settings import settings
from loguru import logger


def logger_filter(record):
    return record["level"].no >= logger.level(settings.log_level).no


logger.remove()
logger.add(sys.stderr, filter=logger_filter)
