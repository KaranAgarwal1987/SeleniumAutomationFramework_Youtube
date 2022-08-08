import pandas as pd

from tests.migration_utility.migration_logging import logger
from tests.migration_utility.migration_runner.migration_base import MigrationBase
from tests.migration_utility.reader import reader_factory
from tests.migration_utility.reports import rpt_factory


class MigrationRunner(MigrationBase):
    def __init__(self, config_objects: list):
        super(MigrationRunner, self).__init__(config_objects)

    def run_migration(self):

        for ind_config in self._config_objects:
            if (not ind_config.status) or (ind_config.status.upper() == 'PASS'):
                is_single_date = False
                self.process_migration_report(ind_config, ind_config.date_list, is_single_date)
            else:
                date_list_str = ','.join(ind_config.date_list)
                logger.info(
                    f'Not processing tikcer: {ind_config.old_ticker} for dates: {date_list_str} as its status is {ind_config.status}')

    def process_migration_report(self, ind_config, date_list, is_single_date):
        summary_df_columns = ind_config.summary_df_clm
        summary_df = pd.DataFrame(columns=summary_df_columns)
        reader_obj = reader_factory.RederFactory.get_reader(ind_config.file_type, ind_config=ind_config,
                                                            summary_df=summary_df, date_list=date_list)
        combined_df, summary_df = reader_obj.read_basket()
        rpt_obj = self.ret_report_object(ind_config, combined_df, summary_df, date_list, is_single_date)
        rpt_obj.process()

    def ret_report_object(self, ind_config, combined_df, summary_df, date_list, is_single_date):
        if is_single_date:
            rpt_obj = rpt_factory.ReportFactory.get_report(ind_config, merged_combined_df=combined_df,
                                                           summary_df=summary_df, cur_date=date_list[0])
        else:
            rpt_obj = rpt_factory.ReportFactory.get_report(ind_config, merged_combined_df=combined_df,
                                                           summary_df=summary_df)
        return rpt_obj
