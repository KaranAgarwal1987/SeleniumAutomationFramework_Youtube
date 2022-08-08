import copy
import math
from decimal import Decimal
from os import listdir, makedirs
from os.path import isfile, join, dirname, exists
from pathlib import Path

import pandas as pd


class MigrationUtiliy:

    def __init__(self):
        self.boolean_dlt_clm_suufix = '_dlta'
        self.merge_suffix = ['_oldtickedata', '_newtickedata']
        self._init_result_folder()

    def get_files(self, ind_config, extension, date_list):
        old_ticker_folder = ind_config.old_ticker_folder
        new_ticker_folder = ind_config.new_ticker_folder
        old_ticker_file = self._list_file_based_on_extension(old_ticker_folder,
                                                             ind_config.old_ticker, extension, date_list)
        new_ticker_file = self._list_file_based_on_extension(new_ticker_folder,
                                                             ind_config.new_ticker, extension, date_list)

        return old_ticker_file, new_ticker_file

    def _list_file_based_on_extension(self, folder_path, tickername, extension, date_list):
        files = [f for f in listdir(folder_path) if isfile(join(folder_path, f))
                 and (extension in f) and (tickername in f)]
        return [ind_file for ind_file in files for date in date_list if date in ind_file]

    def merge_and_compare(self, old_ticker_df, new_ticker_df, merge_column, merge_type='inner'):
        merge_df = old_ticker_df.merge(new_ticker_df, how=merge_type, left_on=merge_column, right_on=merge_column,
                                       suffixes=self.merge_suffix, indicator=True)
        final_clm_list = copy.copy(merge_column)

        for col in old_ticker_df.columns:
            if (col not in merge_column):
                actdelta, delta_clm, act_clm, ben_clm = col + "_actdlta", col + self.boolean_dlt_clm_suufix, col + \
                                                        self.merge_suffix[0], col + self.merge_suffix[1]

                if ben_clm in merge_df.columns:
                    final_clm_list.extend([act_clm, ben_clm, actdelta, delta_clm])
                    try:
                        merge_df[actdelta] = abs(merge_df[ben_clm].fillna(0) - merge_df[act_clm].fillna(0))
                        merge_df[delta_clm] = pd.np.isclose(merge_df[act_clm], merge_df[ben_clm],
                                                            rtol=1e-12, atol=1e-12, equal_nan=True)
                    except:
                        merge_df[actdelta] = 'N/A'

                        merge_df[delta_clm] = merge_df[act_clm].fillna("nan") == merge_df[ben_clm].fillna("nan")

        return merge_df[final_clm_list]

    def _return_df_len(self, df):
        dflen = 0
        if not df.empty:
            dflen = len(df.index)
        return dflen

    def _get_false_per_delta(self, mismatchdf, mismatchdflen, clm):
        percentage = 0
        if not mismatchdf.empty:
            falserows = mismatchdf[mismatchdf[clm] == False]
            flaserowlen = self._return_df_len(falserows)
            percentage = str(round(Decimal((flaserowlen * 100.0) / mismatchdflen), 2)) + "%"
        return percentage

    def ret_clm_per_failed(self, mismatchdf, basket):
        mismatch_row_len = self._return_df_len(mismatchdf)
        clm_name = 'clm_name_' + basket
        mismatch_percentage = 'mismatch_percentage_' + basket
        clm_percentage_df = pd.DataFrame(columns=[clm_name, mismatch_percentage])
        index_count = 0
        for clm in mismatchdf.columns:
            if self.boolean_dlt_clm_suufix in clm:
                clmname = clm.replace(self.boolean_dlt_clm_suufix, "")
                clm_percentage_df.at[index_count, clm_name] = clmname
                clm_percentage_df.at[index_count, mismatch_percentage] = self._get_false_per_delta(mismatchdf,
                                                                                                   mismatch_row_len,
                                                                                                   clm)
                index_count += 1

        return clm_percentage_df

    def _init_result_folder(self):

        self.result_folder = join(Path(__file__).parent.parent, 'results')
        if not exists(self.result_folder):
            makedirs(self.result_folder)

    def write_result(self, summary_df, summary_bybasket, result_df, result_file):
        result_file_path = join(self.result_folder, result_file)
        writer = pd.ExcelWriter(result_file_path, engine='openpyxl')
        summary_df.to_excel(writer, "Summary", index=False)
        summary_bybasket.to_excel(writer, "summary_bybasket", index=False)
        result_df.to_excel(writer, "detail_Comparison", index=False)
        writer.save()

    def return_mismatchrows(self, result_df):
        return result_df[~result_df.all(1, bool_only=True, skipna=True)]
