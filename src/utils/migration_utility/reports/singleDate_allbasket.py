import datetime

import pandas as pd

from migration_logging import logger
from .rpt_base import Report_Base


class SingleDateAllBasket(Report_Base):
    def __init__(self, ind_config, merged_combined_df, summary_df, cur_date):
        super(SingleDateAllBasket, self).__init__(ind_config, merged_combined_df, summary_df, cur_date)

    def process(self):

        summary_df_basket_list = []
        if not self.merged_combined_df.empty:
            unique_baskets = self.merged_combined_df[self.basket_clm].unique()
            for val in unique_baskets:
                filtered_df = self.merged_combined_df[self.merged_combined_df[self.basket_clm] == val]
                summary_df_basket_list.append(self.migration_utility_obj.ret_clm_per_failed(filtered_df, val))

        self._write_results(self.summary_df, summary_df_basket_list, self.merged_combined_df)

    def _write_results(self, summary_df, summary_df_basket_list, result_df):
        if summary_df.empty:
            summary_df.at[0, 'date'] = self.current_dt
            summary_df.at[0, 'basket'] = "No data for old and new ticker"

        if summary_df_basket_list:
            summary_df_by_basket = pd.concat(summary_df_basket_list, axis=1)
        else:
            summary_df_by_basket = pd.DataFrame()

        now_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = "{0}_Summary_{1}_{2}.xlsx".format(self.ind_config.old_ticker, self.current_dt, now_datetime)

        if self.ind_config.onlymismatchrows:
            result_df = self.migration_utility_obj.return_mismatchrows(result_df)
        logger.info(f'Writing result file for date:{self.current_dt}.............')
        self.migration_utility_obj.write_result(summary_df, summary_df_by_basket, result_df, file_name)
        logger.info("Done.............---------------")
