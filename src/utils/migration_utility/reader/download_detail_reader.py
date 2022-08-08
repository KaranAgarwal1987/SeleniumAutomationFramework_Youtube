import pandas as pd
import xlrd
from os.path import join, exists

from migration_exceptions import LocalFileHandlingException
from migration_logging import logger
from .reader_base import Reder_Base


class DownloadDetailReader(Reder_Base):
    def __init__(self, ind_config, summary_df, date_list):
        super(DownloadDetailReader, self).__init__(ind_config, summary_df, date_list)
        self.extension = '.xlsx'

    def filter_files_based_on_basket_in_config(self, basket_list):
        basket_to_cmp = self.ind_config.baskets_to_cmp
        if basket_to_cmp:
            basket_list = [basket2 for basket2 in basket_list for basket1 in basket_to_cmp if basket1.lower() == basket2.lower()]
        return basket_list

    def read_basket(self):
        old_ticker_files, new_ticker_files = self.migration_utility_obj.get_files(self.ind_config, self.extension,self.date_list)
        old_ticker = self.ind_config.old_ticker
        new_ticker = self.ind_config.new_ticker
        old_ticker_folder = self.ind_config.old_ticker_folder
        new_ticker_folder = self.ind_config.new_ticker_folder
        basket_merge_clm = self.ind_config.basket_merge_column_detail
        merged_cmp_combined_df = pd.DataFrame()
        basket_index = 0
        for baskest_file in old_ticker_files:
            old_ticker_file_path = join(old_ticker_folder, baskest_file)
            new_ticker_file_path = join(new_ticker_folder, baskest_file.replace(old_ticker, new_ticker))
            try:
                old_ticker_wb = xlrd.open_workbook(old_ticker_file_path, on_demand=True)
                new_ticker_wb = xlrd.open_workbook(new_ticker_file_path, on_demand=True)
            except  Exception as exp:
                raise LocalFileHandlingException(
                    f'{exp} error while reading excel file , please check if these files are properly created:{old_ticker_file_path},{new_ticker_file_path}')
            baskets_in_old = old_ticker_wb.sheet_names()
            baskets_in_new = new_ticker_wb.sheet_names()

            baskets_only_in_old, baskets_only_in_new, baskets_in_both = self.get_basket_uniq_to_tickers(baskets_in_old,
                                                                                                        baskets_in_new)
            date = self.get_date_from_filename(baskest_file)
           # baskets_in_both = self.filter_files_based_on_basket_in_config(baskets_in_both)
            for basket in baskets_in_both:
                logger.info(f'Proceesing file: {baskest_file} on basket: {basket} for date: {date}')
                new_ticker_df = pd.read_excel(new_ticker_wb, sheetname=basket, engine='xlrd')
                old_ticker_df = pd.read_excel(old_ticker_wb, sheetname=basket, engine='xlrd')

                new_ticker_df['ticker'] = old_ticker
                merged_df = self.migration_utility_obj.merge_and_compare(old_ticker_df, new_ticker_df, basket_merge_clm)
                merged_cmp_combined_df = merged_cmp_combined_df.append(merged_df, ignore_index=True)
                self.fill_summary_df(basket_index, old_ticker_df, new_ticker_df, baskest_file, basket)
                basket_index += 1

            self.add_diff_basket_summary(baskets_only_in_old, 'baskets_only_in_old', date)
            self.add_diff_basket_summary(baskets_only_in_new, 'baskets_only_in_new', date)
            basket_index += len(baskets_only_in_old) + len(baskets_only_in_new)
            old_ticker_wb.release_resources()
            new_ticker_wb.release_resources()
        self.summary_df['old_ticker'] = old_ticker
        self.summary_df['new_ticker'] = new_ticker
        return merged_cmp_combined_df, self.summary_df

    def add_diff_basket_summary(self, baskets_list, prsent_in, date):
        not_present_basket_df = pd.DataFrame(columns=self.summary_df.columns)
        for index, value in enumerate(baskets_list):
            not_present_basket_df.at[index, 'basket'] = value
            not_present_basket_df.at[index, 'present_in'] = prsent_in
            not_present_basket_df.at[index, 'date'] = date
        self.summary_df = self.summary_df.append(not_present_basket_df, ignore_index=True)

    def get_date_from_filename(self, file_name):
        return file_name.split('_')[-1].split(".")[0]

    def fill_summary_df(self, index_val, old_ticker_df, new_ticker_df, file_name, basket_name):
        constituent_in_old_ticker = set(old_ticker_df['constituent'].unique())
        constituent_in_new_ticker = set(new_ticker_df['constituent'].unique())
        old_ticker_columns = set(old_ticker_df.columns)
        new_ticker_columns = set(new_ticker_df.columns)
        #date = self.get_date_from_filename(file_name)
        start_date, end_date = self.ind_config.date_list[0], self.ind_config.date_list[-1]
        constituent_onlyin_old_ticker = constituent_in_old_ticker - constituent_in_new_ticker
        constituent_onlyin_new_ticker = constituent_in_new_ticker - constituent_in_old_ticker
        column_onlyin_new = new_ticker_columns - old_ticker_columns
        column_onlyin_old = old_ticker_columns - new_ticker_columns
        self.summary_df.at[index_val, 'start_date'] = start_date
        self.summary_df.at[index_val, 'end_date'] = end_date
        self.summary_df.at[index_val, 'constituent_old_ticker_length'] = len(old_ticker_df.index)
        self.summary_df.at[index_val, 'constituent_new_ticker_length'] = len(new_ticker_df.index)
        self.summary_df.at[
            index_val, 'constituent_onlyin_old_ticker'] = constituent_onlyin_old_ticker if constituent_onlyin_old_ticker else ""
        self.summary_df.at[
            index_val, 'constituent_onlyin_new_ticker'] = constituent_onlyin_new_ticker if constituent_onlyin_new_ticker else ""
        self.summary_df.at[index_val, 'present_in'] = 'Both'
        self.summary_df.at[index_val, 'basket'] = basket_name
        self.summary_df.at[index_val, 'column_onlyin_new'] = column_onlyin_new if column_onlyin_new else ""
        self.summary_df.at[index_val, 'column_onlyin_old'] = column_onlyin_old if column_onlyin_old else ""

    def get_basket_uniq_to_tickers(self, baskets_in_old, baskets_in_new):

        baskets_only_in_old = set(baskets_in_old) - set(baskets_in_new)
        baskets_only_in_new = set(baskets_in_new) - set(baskets_in_old)
        baskets_in_both = set(baskets_in_old).intersection(set(baskets_in_new))
        baskets_in_both.discard('EVENTS')
        return baskets_only_in_old, baskets_only_in_new, baskets_in_both
