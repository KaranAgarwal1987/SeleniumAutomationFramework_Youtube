import logging.config
import logging.handlers
import configparser
from os import makedirs
from os.path import exists, join
from pathlib import Path

logger_config_file_path = Path(__file__).parent.parent / 'logging_config_files/logging.conf'
LOG_FOLDER = Path(__file__).parent.parent / 'logs'
LOG_FILENAME = LOG_FOLDER /'logconfig.log'
if not exists(LOG_FOLDER):
    makedirs(LOG_FOLDER)
logging.config.fileConfig(logger_config_file_path)
logger = logging.getLogger('root')
handler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=1000000, backupCount=3)

logger.addHandler(handler)
