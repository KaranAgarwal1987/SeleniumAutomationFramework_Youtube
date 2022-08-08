from os.path import join, exists

import pandas as pd

from migration_logging import logger
from .reader_base import Reder_Base


class DownloadReader(Reder_Base):

    def __init__(self, ind_config, summary_df, date_list):
        super(DownloadReader, self).__init__(ind_config, summary_df, date_list)
        self.extension = '.csv'

    def filter_files_based_on_basket_in_config(self, filelist):
        basket_to_cmp = self.ind_config.baskets_to_cmp
        if basket_to_cmp:
            filelist = [file for file in filelist for basket in basket_to_cmp if basket in file]
        return filelist

    def get_file(self):
        old_ticker_files, new_ticker_files = self.migration_utility_obj.get_files(self.ind_config, self.extension,
                                                                                  self.date_list)
        #old_ticker_files = self.filter_files_based_on_basket_in_config(old_ticker_files)
        #new_ticker_files = self.filter_files_based_on_basket_in_config(new_ticker_files)
        return old_ticker_files, new_ticker_files

    def read_basket(self):
        old_ticker_files, new_ticker_files = self.get_file()
        old_ticker = self.ind_config.old_ticker
        new_ticker = self.ind_config.new_ticker
        old_ticker_folder = self.ind_config.old_ticker_folder
        new_ticker_folder = self.ind_config.new_ticker_folder
        basket_merge_clm = self.ind_config.basket_merge_column

        baskets_in_old_dict = self.get_basket_names(old_ticker, old_ticker_files)
        baskets_in_new_dict = self.get_basket_names(new_ticker, new_ticker_files)

        baskets_only_in_old_dict, baskets_only_in_new_dict = self.get_baskets_unique_to_ticker(baskets_in_old_dict,
                                                                                               baskets_in_new_dict
                                                                                               )
        merged_cmp_combined_df = pd.DataFrame()
        for index, baskest_file in enumerate(old_ticker_files):
            date = self.get_date_from_filename(baskest_file)
            old_ticker_file_path = join(old_ticker_folder, baskest_file)
            new_ticker_file_path = join(new_ticker_folder, baskest_file.replace(old_ticker, new_ticker))
            old_ticker_df = pd.read_csv(old_ticker_file_path)
            if exists(new_ticker_file_path):
                logger.info(f'Proceesing file: {baskest_file} for date: {date}')
                new_ticker_df = pd.read_csv(new_ticker_file_path)
                new_ticker_df['Ticker'] = old_ticker
                new_ticker_df['Calculation Ticker'] = old_ticker
                merged_df = self.migration_utility_obj.merge_and_compare(old_ticker_df, new_ticker_df, basket_merge_clm)
                merged_cmp_combined_df = merged_cmp_combined_df.append(merged_df, ignore_index=True)
                self.fill_summary_df(index, old_ticker_df, new_ticker_df, baskest_file, old_ticker)

        self.add_diff_basket_summary(baskets_only_in_old_dict, 'baskets_only_in_old')
        self.add_diff_basket_summary(baskets_only_in_new_dict, 'baskets_only_in_new')
        self.summary_df['old_ticker'] = old_ticker
        self.summary_df['new_ticker'] = new_ticker
        return merged_cmp_combined_df, self.summary_df

    def get_baskets_unique_to_ticker(self, baskets_in_old_dict, baskets_in_new_dict):
        basket_only_in_new_dict = {}
        basket_only_in_old_dict = {}
        for date in self.date_list:
            basket_only_in_old_dict[date] = self.substract_set(baskets_in_old_dict[date], baskets_in_new_dict[date])
            basket_only_in_new_dict[date] = self.substract_set(baskets_in_new_dict[date], baskets_in_old_dict[date])
        return basket_only_in_old_dict, basket_only_in_new_dict

    def substract_set(self, set_a, set_b):
        sub_result = set_a - set_b
        result = None
        if sub_result:
            result = sub_result
        return result

    def add_diff_basket_summary(self, baskets_date_dict, prsent_in):
        not_present_basket_df = pd.DataFrame(columns=self.summary_df.columns)
        index = 0
        for key, value in baskets_date_dict.items():
            if value:
                not_present_basket_df.at[index, 'basket'] = value
                not_present_basket_df.at[index, 'date'] = key
                not_present_basket_df.at[index, 'present_in'] = prsent_in
                index += 1
        if not not_present_basket_df.empty:
            self.summary_df = self.summary_df.append(not_present_basket_df, ignore_index=True)

    def get_date_from_filename(self, file_name):
        return file_name.split('_')[-1].split(".")[0]

    def fill_summary_df(self, index_val, old_ticker_df, new_ticker_df, file_name, ticker_name):
        constituent_in_old_ticker = set(old_ticker_df['Constituent'].unique())
        constituent_in_new_ticker = set(new_ticker_df['Constituent'].unique())
        old_ticker_columns = set(old_ticker_df.columns)
        new_ticker_columns = set(new_ticker_df.columns)
        start_date, end_date = self.ind_config.date_list[0], self.ind_config.date_list[-1]
        constituent_onlyin_old_ticker = self.substract_set(constituent_in_old_ticker, constituent_in_new_ticker)
        constituent_onlyin_new_ticker = self.substract_set(constituent_in_new_ticker, constituent_in_old_ticker)
        column_onlyin_new = self.substract_set(new_ticker_columns, old_ticker_columns)
        column_onlyin_old = self.substract_set(old_ticker_columns, new_ticker_columns)
        self.summary_df.at[index_val, 'start_date'] = start_date
        self.summary_df.at[index_val, 'end_date'] = end_date
        self.summary_df.at[index_val, 'constituent_old_ticker_length'] = len(old_ticker_df.index)
        self.summary_df.at[index_val, 'constituent_new_ticker_length'] = len(new_ticker_df.index)
        self.summary_df.at[
            index_val, 'constituent_onlyin_old_ticker'] = constituent_onlyin_old_ticker
        self.summary_df.at[
            index_val, 'constituent_onlyin_new_ticker'] = constituent_onlyin_new_ticker
        self.summary_df.at[index_val, 'present_in'] = 'Both'
        self.summary_df.at[index_val, 'basket'] = file_name.replace(ticker_name, '').replace(start_date+'_'+end_date, '').replace(
            self.extension, '')[1:-1]
        self.summary_df.at[index_val, 'column_onlyin_new'] = column_onlyin_new
        self.summary_df.at[index_val, 'column_onlyin_old'] = column_onlyin_old

    def get_basket_names(self, ticker_name, file_names):
        date_baskets_dict = {}
        for date in self.date_list:
            files_to_be_checked = [file for file in file_names if date in file]
            baskets = set()
            for ind_file in files_to_be_checked:
                baskets.add(ind_file.replace(ticker_name, '').replace(date, '').replace(self.extension, '')[1:-1])
            date_baskets_dict[date] = baskets
        return date_baskets_dict
