import configparser
import unittest
import traceback, sys
from enum import Enum
import logging

from tests.backtest_util import util
from tests.pat_util import util_input

class BackTestParameter:
    def __init__(self, attribute, precision):
        self.attribute = attribute
        self.precision = precision


class BACKTESTING_ENV(Enum):
    PROD="PROD"
    QA="QA"
    DEV="DEV"

class TestUtils(unittest.TestCase):


    def init_back_dict(self,output_header,benchmarks_header,precesion):
        back_test_parameters = None
        column_dict = None
        message = ""
        uselessparm = ['final_results','index_master']
        output_header_coloumn = [value.lower() for value in output_header if value not in uselessparm ]
        benchmarks_header_column = [value.lower() for value in benchmarks_header if value not in uselessparm]

        if "index_level" in benchmarks_header_column and "index_level" not in output_header_coloumn:
            message = "index_level is present in benchmark but not in calculated daily parameter by methodology"
        # removing ticker and date to get into backtestparameter
        common_elem =  set(output_header_coloumn[4:]).intersection(set(benchmarks_header_column[2:]))

        if common_elem:
            back_test_parameters = [BackTestParameter(value,precesion) for value in
                                    common_elem]
            column_dict = {value:value for value in common_elem}
        else:
            message = "No Common element found in benchmark and calculated daily param for comparison"
        return back_test_parameters,column_dict,message

    def back_test_new_base_calc(self, methodology, back_test_parameters, Column_dict, ticket, calc_api, **kwargs):
        #attribut to determine comparison on all parameter or manual parameter defined in input file
        isallcompare = True
        if back_test_parameters and Column_dict:
            isallcompare = False
        message = None
        util_obj = util()
        util_obj.get_sanity( methodology, **kwargs)
        precesion = kwargs["precision_level"]
        # calc_api, ticket = util_input.initialize_environment(self, **kwargs)
        # Get files for which you want to execute your test
        input_data = util_obj.load_data( methodology, **kwargs)
        # Loads the benchmarks for the respetive data for the respective tickers


        # Get input data from the files for the interested ticker for the respective dates
        output_header, output_data = util_obj.get_output_enhanced_new_base_calc( calc_api, ticket, methodology, input_data,
                                                                            **kwargs)

        benchmark_values, benchmarks_header = util_obj.get_benchmarks_values(methodology, **kwargs)
        # create backtest parameter and column dict in case all parameter need to compare
        if isallcompare:
            back_test_parameters,Column_dict,message = self.init_back_dict(output_header,benchmarks_header,precesion)

        # validate the output data against the expected values and get the error logs
        error_logs, custLogs, errorLogs_index = util_obj.validate_data_enhanced( methodology, back_test_parameters,
                                                                    Column_dict, output_data,
                                                                    output_header, benchmark_values, benchmarks_header,isallcompare)
        # mark the Test case as pass/fail and generate report
        util_obj.get_report_enhanced( methodology, output_data, output_header, error_logs,
                                 back_test_parameters,
                                 Column_dict, custLogs, errorLogs_index,isallcompare,**kwargs)
        if message:
            raise Exception(message)

    def back_test_pickle_files(self, methodology, back_test_parameters, Column_dict, ticket, calc_api, **kwargs):

        # attribut to determine comparison on all parameter or manual parameter defined in input file
        isallcompare = True
        if back_test_parameters and Column_dict:
            isallcompare = False
        message = None
        util_obj = util()
        util_input_obj = util_input()
        precesion = kwargs["precision_level"]
        util_obj.get_sanity( methodology, **kwargs)
        # Get files for which you want to execute your test
        files = util_obj.get_files_pickle(methodology, **kwargs)
        # Get input data from the files for the interested ticker for the respective dates
        input_data, input_header = util_obj.get_input_data_pickle( methodology, files, **kwargs)
        # Calculate the output for the respective data
        output_data, output_header = util_obj.get_output_enhanced(methodology, input_data, input_header, **kwargs)
        output_header_final = set()
        if 'final_results' in output_header:
            for data in output_data:
                try:
                    output_header_final.update(data[4].keys())
                except:
                    pass
            output_header_final =  ['methodology','ticker','date','status']+ list(output_header_final)
        # Loads the benchmarks for the respetive data for the respective tickers
        benchmark_values, benchmarks_header = util_obj.get_benchmarks_values( methodology,**kwargs)
        #create backtest parameter and column dict in case all parameter need to compare
        if isallcompare:
            if output_header_final:
                back_test_parameters,Column_dict,message = self.init_back_dict(output_header_final,benchmarks_header,precesion)
            else:
                back_test_parameters, Column_dict, message = self.init_back_dict(output_header, benchmarks_header,
                                                                                 precesion)
        # validate the output data against the expected values and get the error logs
        error_logs, custLogs, errorLogs_index = util_obj.validate_data_enhanced( methodology, back_test_parameters,
                                                                    Column_dict, output_data,
                                                                    output_header, benchmark_values, benchmarks_header,isallcompare)
        # mark the Test case as pass/fail and generate report
        util_obj.get_report_enhanced( methodology, output_data, output_header, error_logs,
                                 back_test_parameters,
                                 Column_dict, custLogs, errorLogs_index, isallcompare,**kwargs)
        if message:
            raise Exception(message)


    def back_test_covercall_files(self, methodology, back_test_parameters, Column_dict, ticket, calc_api, **kwargs):

        # attribut to determine comparison on all parameter or manual parameter defined in input file
        isallcompare = True
        if back_test_parameters and Column_dict:
            isallcompare = False
        message = None
        util_obj = util()
        util_input_obj = util_input()
        precesion = kwargs["precision_level"]
        util_obj.get_sanity( methodology, **kwargs)

        # Get files for which you want to execute your test
        files = util_obj.get_files_pickle(methodology, **kwargs)

        # Get input data from the files for the interested ticker for the respective dates
        input_data, input_header = util_obj.get_input_data_pickle( methodology, files, **kwargs)
        # Calculate the output for the respective data
        output_data, output_header = util_obj.get_output_enhanced_covercall(methodology, input_data, input_header, **kwargs)
        output_header_final = set()
        if 'final_results' in output_header:
            for data in output_data:
                try:
                    output_header_final.update(data[4].keys())
                except:
                    pass
            output_header_final =  ['methodology','ticker','date','status']+ list(output_header_final)
        # Loads the benchmarks for the respetive data for the respective tickers
        benchmark_values, benchmarks_header = util_obj.get_benchmarks_values( methodology,**kwargs)
        #create backtest parameter and column dict in case all parameter need to compare
        if isallcompare:
            if output_header_final:
                back_test_parameters,Column_dict,message = self.init_back_dict(output_header_final,benchmarks_header,precesion)
            else:
                back_test_parameters, Column_dict, message = self.init_back_dict(output_header, benchmarks_header,
                                                                                 precesion)
        # validate the output data against the expected values and get the error logs
        error_logs, custLogs, errorLogs_index = util_obj.validate_data_enhanced( methodology, back_test_parameters,
                                                                    Column_dict, output_data,
                                                                    output_header, benchmark_values, benchmarks_header,isallcompare)
        # mark the Test case as pass/fail and generate report
        util_obj.get_report_enhanced( methodology, output_data, output_header, error_logs,
                                 back_test_parameters,
                                 Column_dict, custLogs, errorLogs_index, isallcompare,**kwargs)
        if message:
            raise Exception(message)

    def equity_back_test_pickle_files(self, methodology, ticket, calc_api, **kwargs):
        util_obj = util()
        util_obj.get_sanity(methodology, **kwargs)
        precesion = kwargs["precision_level"]
        # # Get files for which you want to execute your test
        new_equity_files = util_obj.get_equity_files_pickle(methodology, "equity_new",**kwargs)
        old_equity_files = util_obj.get_equity_files_pickle(methodology, "equity_old",**kwargs)
        #
        # # Get input data from the files for the interested ticker for the respective dates
        data_new_equity = util_obj.get_input_equity_data_pickle(methodology, new_equity_files, **kwargs)
        data_old_equity = util_obj.get_input_equity_data_pickle(methodology, old_equity_files, **kwargs)


        #init output file
        util_obj.init_equity_param(writeheader=True,**kwargs)
        # util_obj.init_equity_param(writeheader=False, **kwargs)
        # Calculate the output for the respective data

        output_data_new = util_obj.get_equity_output_enhanced_new_base_calc(calc_api, ticket, methodology,data_new_equity,**kwargs)
        output_data_old = util_obj.get_equity_output_enhanced_old(calc_api, ticket, data_old_equity,**kwargs)



        util_obj.createDeltaFile(**kwargs)
        util_obj.createReportFile(precesion,**kwargs)

    def get_input_enhanced(self, type, methodology, back_test_parameters, Column_dict,ticket, calc_api, **kwargs):

        try:
            # appendwiththreadname = add_thread_name(threadname)
            # meth_data = appendwiththreadname("meth_data")
            # data = appendwiththreadname("data")
            # err_data = appendwiththreadname("err_data")
            # input_files = appendwiththreadname("input_files")
            if type == 'pickle':
                print("--------------pickle==============")
                logging.info("--------------pickle==============")
                self.back_test_pickle_files(methodology,back_test_parameters, Column_dict,ticket, calc_api, **kwargs)
                logging.info("--------------pickle==============")

            elif type == 'cover_call':
                print("--------------pickle==============")
                logging.info("--------------pickle==============")
                self.back_test_covercall_files(methodology,back_test_parameters, Column_dict,ticket, calc_api, **kwargs)
                logging.info("--------------pickle==============")

            elif (type == 'equity'):
                self.equity_back_test_pickle_files(methodology, ticket, calc_api,**kwargs)
            else:
                print("--------------base_calc==============")
                self.back_test_new_base_calc(methodology,back_test_parameters, Column_dict,ticket, calc_api, **kwargs)


        except Exception:
            errmsg = "got error in ticker {0}, of methodology {1}, where error is: {2}".format( kwargs['ticker'][0], methodology, traceback.format_exc())

            logging.error(errmsg)
            return "fail",traceback.format_exc()
        return "pass",""