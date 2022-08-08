from datetime import date
from importlib import import_module

from migration_exceptions import WrongReaderException
from migration_logging import logger


class RederFactory:
    @staticmethod
    def get_reader(reader_type, **kwargs):
        try:
            """
            Factory method creates instance of reader class.
            """
            if reader_type.lower() == 'download':
                module, main_class = 'download_reader', 'DownloadReader'
            elif reader_type.lower() == 'downloaddetails':
                module, main_class = 'download_detail_reader', 'DownloadDetailReader'
            else:
                raise WrongReaderException(
                    "Please specify correct reader in Equity.csv File should be in Download and DownloadDetails")
            module = import_module('reader.' + module, package=__package__)
            rpt = getattr(module, main_class)(**kwargs)
            return rpt
        except Exception as exp:
            logger.error(f'{str(exp)} creating reader.')
            raise Exception(f'{str(exp)} creating reader.')
