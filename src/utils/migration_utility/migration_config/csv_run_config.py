import datetime

from migration_exceptions import ConfigException
from migration_logging import logger


class CSV_Run_Config:
    def __init__(self, config_dict: dict):
        try:
            self.old_ticker_folder = config_dict.get('input_old_ticker_folder', None)
            self.new_ticker_folder = config_dict.get('input_new_ticker_folder', None)
            self.archive_old_ticker_folder = config_dict.get('archive_old_ticker_folder', None)
            self.archive_new_ticker_folder = config_dict.get('archive_new_ticker_folder', None)
            self.basket_merge_column = self._get_split_clm_list(config_dict, 'basket_downlaod')
            self.basket_merge_column_detail = self._get_split_clm_list(config_dict, 'basket_download_detail')
            self.dailyparam_merge_column = self._get_split_clm_list(config_dict, 'dailyparam')
            self.old_ticker = config_dict.get('oldticker', None)
            self.new_ticker = config_dict.get('newticker', None)
            self.file_type = config_dict.get('filetype', None)
            #self.rpt_type = config_dict.get('reporttype', None)
            self.status = config_dict.get('status', None)
            self.baskets_to_cmp = self._get_split_clm_list(config_dict, 'basket', '|')
            self.onlymismatchrows = self._get_boolean_val(config_dict, 'onlymismtachrows')
            self.date_list = self._create_date_list(config_dict)
            self.summary_df_clm = self._get_split_clm_list(config_dict, 'summary_df_columns')

        except Exception as exp:
            logger.error(str(ConfigException(exp)))
            raise ConfigException(exp)

    @staticmethod
    def _get_boolean_val(config_dict, config_name):
        val = config_dict.get(config_name, None)
        bool_flag = False
        if val and val.lower() == 'true':
            bool_flag = True
        return bool_flag

    @staticmethod
    def _get_split_clm_list(config_dict, config_name, delimiter=','):
        val_clm_str = config_dict.get(config_name, None)
        if val_clm_str:
            return [clm.strip() for clm in val_clm_str.split(delimiter)]

    @staticmethod
    def _create_date_list(config_dict):
        start_date = config_dict.get('startdate', None)
        end_date = config_dict.get('enddate', None)
        date_str = config_dict.get('daterange', None)
        date_list = []
        if date_str:
            date_list.extend(date_str.split('|'))
        else:
            start_date_dt = datetime.datetime.strptime(start_date, '%Y%m%d')
            end_date_dt = datetime.datetime.strptime(end_date, '%Y%m%d')
            while start_date_dt <= end_date_dt:
                date_list.append(start_date_dt.date().strftime('%Y%m%d'))
                start_date_dt = start_date_dt + datetime.timedelta(days=1)

        return date_list
