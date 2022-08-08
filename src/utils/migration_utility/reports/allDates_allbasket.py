import datetime

from migration_logging import logger
from .rpt_base import Report_Base
import pandas as pd


class AllDatesAllBasket(Report_Base):
    def __init__(self, ind_config, merged_combined_df, summary_df):
        super(AllDatesAllBasket, self).__init__(ind_config, merged_combined_df, summary_df)

    def process(self):
        unique_baskets = self.merged_combined_df[self.basket_clm].unique()
        summary_df_basket_list = []

        for val in unique_baskets:
            filtered_df = self.merged_combined_df[self.merged_combined_df[self.basket_clm] == val]
            summary_df_basket_list.append(self.migration_utility_obj.ret_clm_per_failed(filtered_df, val))

        summary_df_by_basket = pd.concat(summary_df_basket_list, axis=1)
        start_date, end_date = self.ind_config.date_list[0], self.ind_config.date_list[-1]
        now_datetime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = "{0}_Summary_{1}_{2}_{3}.xlsx".format(self.ind_config.old_ticker, start_date, end_date,
                                                          now_datetime)
        if self.ind_config.onlymismatchrows:
            self.merged_combined_df = self.migration_utility_obj.return_mismatchrows(self.merged_combined_df)
        #self.add_dates_with_no_data_to_summary_df()
        logger.info("Writing result file.............")
        self.migration_utility_obj.write_result(self.summary_df, summary_df_by_basket, self.merged_combined_df, file_name)

        logger.info("Done.............---------------")

    def add_dates_with_no_data_to_summary_df(self):
        date_with_no_data = set(self.ind_config.date_list) - set(self.summary_df['date'].unique())
        summary_df_len = len(self.summary_df.index)
        if date_with_no_data:
            for index, date in enumerate(date_with_no_data):
                at_index = summary_df_len + index
                self.summary_df.at[at_index, 'date'] = date
                self.summary_df.at[at_index, 'basket'] = 'No file for both old and new at this date'
