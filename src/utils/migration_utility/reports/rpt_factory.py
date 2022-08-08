from datetime import date
from importlib import import_module

from migration_exceptions import ConfigException
from migration_logging import logger


class ReportFactory:
    @staticmethod
    def get_report(ind_config, **kwargs):
        try:
            """
            Factory method creates instance of report class.
            """
            #if ind_config.rpt_type.lower() == 'alldates_allbasket':
            module, main_class = 'allDates_allbasket', 'AllDatesAllBasket'

            # elif ind_config.rpt_type.lower() == 'singledate_allbasket':
            #     module, main_class = 'singleDate_allbasket', 'SingleDateAllBasket'
            # else:
            #     logger.error("Wrong argument is given for report type, should be in : alldates_allbasket,singledate_allbasket")
            #     raise ConfigException("Wrong argument is given for report type, should be in : alldates_allbasket,singledate_allbasket")

            module = import_module('reports.' + module, package=__package__)
            rpt = getattr(module, main_class)(ind_config,**kwargs)
            return rpt
        except Exception as exp:
            logger.error(f'{str(exp)} creating report.')
            raise Exception(f'{str(exp)} creating report.')
