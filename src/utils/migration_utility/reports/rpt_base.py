from abc import ABC, abstractmethod
from common.migration_util import MigrationUtiliy


class Report_Base(ABC):
    def __init__(self, ind_config, merged_combined_df, summary_df, cur_date= None):
        self.merged_combined_df = merged_combined_df
        self.summary_df = summary_df
        self.ind_config = ind_config
        self.migration_utility_obj = MigrationUtiliy()
        self._set_date_basket_clm()
        self.current_dt = cur_date


    @abstractmethod
    def process(self):
        pass

    def _set_date_basket_clm(self):
        if self.ind_config.file_type == 'Download':
            self.basket_clm = 'Open or Close'
            self.date_clm = 'From Date'
        else:
            self.basket_clm = 'open_or_close'
            self.date_clm = 'from_date'
