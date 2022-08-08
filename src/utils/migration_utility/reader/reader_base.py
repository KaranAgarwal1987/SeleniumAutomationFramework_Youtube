from abc import ABC, abstractmethod
from common.migration_util import MigrationUtiliy

class Reder_Base(ABC):
    def __init__(self, ind_config, summary_df, date_list):
        self.ind_config = ind_config
        self.summary_df = summary_df
        self.migration_utility_obj = MigrationUtiliy()
        self.date_list = date_list


    @abstractmethod
    def read_basket(self):
        pass