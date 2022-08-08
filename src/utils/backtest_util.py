import configparser
import logging
from io import BytesIO
import os,fnmatch
import traceback
import unittest
import shutil
import csv
import time
from datetime import date, datetime, timedelta
from terminaltables import AsciiTable
import pandas as pd
import pandas.api.types as ptypes
from collections import namedtuple
from pickle import *
import boto3
import numpy as np

from calc_meth.models import CalculationRequirements,Holidays,StaticAttributes
from framework.calc_api import CalcApi
from framework.message_loop import MessageLoop
from calc_meth.base_calculator import *
from calc_meth.models import InputParameters


'''
    Created by  : Karan Agarwal
    Utilities for simple operations
'''
class util:


    def get_calculator(self,calc_api,ticker,ticket,date):
        calculator, calculation_failure_message = MessageLoop._get_calculator(ticket, calc_api, ticker,date)
        if calculation_failure_message is not None:
            print(calculation_failure_message)
        return calculator

    def get_sanity(self,methodology, **kwargs):
        bflag=True
        error=[]
        if 'env' not in kwargs.keys():
            bflag=False
            error.append("Environment not defined, please recheck the dictionary")

        if 'date_range' not in kwargs.keys():
            bflag=False
            error.append("Missing date labelling. Please recheck")

        if not bflag:
            raise Exception("Error in dictionary. Missing mandatrory parameter. Please check")


    def get_files_pickle(self, methodology, **kwargs):
        file_list=[]
        env=kwargs["env"]
        date_range=kwargs["date_range"]
        if kwargs["storage_location"].upper() == "AWS":
            filename = "calc-build-files/test/input/{0}/{1}/{2}_Input_{3}.pickle".format( env ,methodology.lower(), kwargs["ticker"][0], date_range )
            file_list.append(filename)

        else:
            for root, dir, files in os.walk(os.path.dirname(__file__) + '/backtesting/input/' + env +"/"+ methodology):
                if 'ticker' in kwargs.keys():
                    for ticker in kwargs["ticker"]:
                        filter = ticker + "_Input_" + date_range +  ".pickle"
                        items = fnmatch.filter(files, filter)
                        if items.__len__() != 0:
                            file_list.append(root + '/' + items[0])
                else:
                    filter = "*_Input_" + date_range + ".csv"
                    for items in fnmatch.filter(files, filter):
                        files = root + '/' + items
                        file_list.append(files)
        return file_list

    def get_equity_files_pickle(self, methodology,type, **kwargs):
        file_list = []
        env = kwargs["env"]
        date_range = kwargs["date_range"]
        if kwargs["storage_location"].upper() == "AWS":
            filename = "calc-build-files/test/input/{0}/{1}/{2}_Input_{3}.pickle".format(env, methodology.lower(), kwargs["ticker"][0],
                                                                        date_range)
            file_list.append(filename)

        else:
            input_dir = '/backtesting_equity/input/newframework/'
            if type == "equity_old":
                input_dir = '/backtesting_equity/input/oldframework/'
            for root, dir, files in os.walk(
                                    os.path.dirname(__file__) + input_dir + kwargs["ticker"][0]):
                if 'ticker' in kwargs.keys():
                    for ticker in kwargs["ticker"]:
                        filter = ticker + "_" + date_range + ".pickle"
                        items = fnmatch.filter(files, filter)
                        if items.__len__() != 0:
                            file_list.append(root + '/' + items[0])
                else:
                    filter = "*_Input_" + date_range + ".csv"
                    for items in fnmatch.filter(files, filter):
                        files = root + '/' + items
                        file_list.append(files)
        return file_list

    def load_data(self, methodology, **kwargs):
        filter = None
        data_dump={}

        if 'env' in kwargs.keys():
            env=kwargs["env"]

        if 'date_range' in kwargs.keys():
            date_range=kwargs["date_range"]

        if 'ticker' in kwargs.keys():
            for ticker in kwargs["ticker"]:
                if kwargs["storage_location"].upper() == "AWS":
                    path = "calc-build-files/test/input/" + env + "/" + methodology + "/" + ticker
                    data_dump[ticker] = self.load_InputParameter_aws(path, date_range,kwargs["s3resource"])
                else:
                    path = os.path.dirname(__file__) + "/backtesting/input/" + env + "/" + methodology + "/" + ticker
                    data_dump[ticker] = util.load_InputParameter(path,date_range)

                try:
                    setattr(data_dump[ticker].holidays, 'Holiday_Calendar', data_dump[ticker].holidays.HOLIDAY_CALENDAR)
                except:
                    pass


        return data_dump

    @staticmethod
    def load_dataframe_from_pickle(filename):
        df = pd.read_pickle(filename)
        return df

    @staticmethod
    def load_dict_from_pickle(filename):
        with open(filename, 'rb') as handle:
            dict = load(handle)
        return dict

    @staticmethod
    def load_namedtuple_from_pickle(filename, Name):
        dict={}
        with open(filename, 'rb') as handle:
            dict = load(handle)
        #tp = namedtuple(Name, dict.keys())(**dict)
        if Name=='Holidays':
            tp = Holidays(**dict)
        else:
            tp = StaticAttributes(**dict)

        return tp


    def returnpickledata(self,filepath,s3resource,type="dataframe",name="StaticAttributes"):
        data = None
        try:
            s3resource.Object('i6-files-eu-west-1', filepath).load()
            with BytesIO() as pickledata:
                s3resource.Bucket('i6-files-eu-west-1').download_fileobj(filepath, pickledata)
                pickledata.seek(0)
                data = load(pickledata)
            if type == "dataframe":
                return pd.DataFrame(data)
            elif type == "dict":
                return data
            else:
                tp = namedtuple(name, data.keys())(**data)
                if name == 'Holidays':
                    tp = Holidays(**data)
                return tp
        except:
            return data



    def load_InputParameter_aws(self,sPath, sDateRange,s3resource):
        sDateRange = '_' + sDateRange
        bond_ref_data = self.returnpickledata(sPath + '/bond_ref_data' + sDateRange + '.pickle',s3resource)
        index_dependencies = self.returnpickledata(sPath + '/index_dependencies' + sDateRange + '.pickle',s3resource)
        close_prices = self.returnpickledata(sPath + '/close_prices' + sDateRange + '.pickle',s3resource,"dict")
        constituents = self.returnpickledata(sPath + '/constituents' + sDateRange + '.pickle',s3resource)
        constituents_dep = self.returnpickledata(sPath + '/constituents_dep' + sDateRange + '.pickle',s3resource)
        daily_parameters = self.returnpickledata( sPath + '/daily_parameters' + sDateRange + '.pickle',s3resource)
        dep_daily_parameters = self.returnpickledata(sPath + '/dep_daily_parameters' + sDateRange + '.pickle',s3resource)
        equity_constituents_prices = self.returnpickledata(sPath + '/equity_constituents_prices' + sDateRange + '.pickle',s3resource)
        holidays =self.returnpickledata(sPath + '/holidays' + sDateRange + '.pickle',s3resource,"tuple",'Holidays')
        ref_data = self.returnpickledata(sPath + '/ref_data' + sDateRange + '.pickle',s3resource)
        schema_content = self.returnpickledata(sPath + '/schema_content' + sDateRange + '.pickle',s3resource)
        static_attributes = self.returnpickledata(sPath + '/static_attributes' + sDateRange + '.pickle',s3resource,"tuple",'StaticAttributes')
        dep_tickers = self.returnpickledata(sPath + '/dep_tickers' + sDateRange + '.pickle',s3resource)
        corp_actions = self.returnpickledata(sPath + '/corp_actions' + sDateRange + '.pickle',s3resource)
        universe = self.returnpickledata(sPath + '/universe' + sDateRange + '.pickle', s3resource)
        n6_data = self.returnpickledata(sPath + '/n6_data' + sDateRange + '.pickle', s3resource)
        fx_rates = self.returnpickledata(sPath + '/fx_rates' + sDateRange + '.pickle', s3resource)
        regions = self.returnpickledata(sPath + '/regions' + sDateRange + '.pickle', s3resource)
        sectors = self.returnpickledata(sPath + '/sectors' + sDateRange + '.pickle', s3resource)

        return InputParameters(static_attributes=static_attributes, holidays=holidays, index_dependencies=index_dependencies, close_prices=close_prices, daily_parameters=daily_parameters, dep_daily_parameters=dep_daily_parameters,
                               constituents_dep=constituents_dep, bond_ref_data=bond_ref_data, constituents=constituents, equity_constituents_prices=equity_constituents_prices, ref_data=ref_data, corp_actions=corp_actions,
                               fx_rates=fx_rates, regions=regions, sectors=sectors, dep_tickers=dep_tickers, n6_data=n6_data, universe=universe, schema_content=schema_content)

    @staticmethod
    def load_InputParameter(sPath,sDateRange):
        """

        :param sPath: get path of pickles file stored, either E# basket or local path
        :param sDateRange:  Date rabge to identify file
        :return: Return unpickled data
        """
        sDateRange = '_' +sDateRange
        bond_ref_data = util.load_dataframe_from_pickle(sPath + '/bond_ref_data' + sDateRange + '.pickle') if os.path.isfile(sPath + '/bond_ref_data' + sDateRange + '.pickle') else None
        index_dependencies = util.load_dataframe_from_pickle(sPath + '/index_dependencies' + sDateRange + '.pickle') if os.path.isfile(sPath + '/index_dependencies' + sDateRange + '.pickle') else None
        close_prices = util.load_dict_from_pickle(sPath + '/close_prices' + sDateRange + '.pickle') if os.path.isfile(sPath + '/close_prices' + sDateRange + '.pickle') else None
        constituents = util.load_dataframe_from_pickle(sPath + '/constituents' + sDateRange + '.pickle') if os.path.isfile(sPath + '/constituents' + sDateRange + '.pickle') else None
        constituents_dep = util.load_dataframe_from_pickle(sPath + '/constituents_dep' + sDateRange + '.pickle') if os.path.isfile(sPath + '/constituents_dep' + sDateRange + '.pickle') else None
        daily_parameters = util.load_dataframe_from_pickle(sPath + '/daily_parameters' + sDateRange + '.pickle') if os.path.isfile(sPath + '/daily_parameters' + sDateRange + '.pickle') else None
        dep_daily_parameters = util.load_dataframe_from_pickle(sPath + '/dep_daily_parameters' + sDateRange + '.pickle') if os.path.isfile(sPath + '/dep_daily_parameters' + sDateRange + '.pickle') else None
        equity_constituents_prices = util.load_dataframe_from_pickle(sPath + '/equity_constituents_prices' + sDateRange + '.pickle') if os.path.isfile(sPath + '/equity_constituents_prices' + sDateRange + '.pickle') else None
        holidays = util.load_namedtuple_from_pickle(sPath + '/holidays' + sDateRange + '.pickle','Holidays') if os.path.isfile(sPath + '/holidays' + sDateRange + '.pickle') else None
        ref_data = util.load_dataframe_from_pickle(sPath + '/ref_data' + sDateRange + '.pickle') if os.path.isfile(sPath + '/ref_data' + sDateRange + '.pickle') else None
        schema_content = util.load_dataframe_from_pickle(sPath + '/schema_content' + sDateRange + '.pickle') if os.path.isfile(sPath + '/schema_content' + sDateRange + '.pickle') else None
        static_attributes = util.load_namedtuple_from_pickle(sPath + '/static_attributes' + sDateRange + '.pickle','StaticAttributes') if os.path.isfile(sPath + '/static_attributes' + sDateRange + '.pickle') else None
        dep_tickers = util.load_dataframe_from_pickle(sPath + '/dep_tickers' + sDateRange + '.pickle') if os.path.isfile(sPath + '/dep_tickers' + sDateRange + '.pickle') else None
        corp_actions = util.load_dataframe_from_pickle(
            sPath + '/corp_actions' + sDateRange + '.pickle') if os.path.isfile(
            sPath + '/corp_actions' + sDateRange + '.pickle') else None
        universe = util.load_dataframe_from_pickle(
            sPath + '/universe' + sDateRange + '.pickle') if os.path.isfile(
            sPath + '/universe' + sDateRange + '.pickle') else None
        n6_data = util.load_dataframe_from_pickle(
            sPath + '/n6_data' + sDateRange + '.pickle') if os.path.isfile(
            sPath + '/n6_data' + sDateRange + '.pickle') else None
        fx_rates = util.load_dataframe_from_pickle(
            sPath + '/fx_rates' + sDateRange + '.pickle') if os.path.isfile(
            sPath + '/fx_rates' + sDateRange + '.pickle') else None
        regions = util.load_dataframe_from_pickle(
            sPath + '/regions' + sDateRange + '.pickle') if os.path.isfile(
            sPath + '/regions' + sDateRange + '.pickle') else None
        sectors = util.load_dataframe_from_pickle(
            sPath + '/sectors' + sDateRange + '.pickle') if os.path.isfile(
            sPath + '/sectors' + sDateRange + '.pickle') else None

        return InputParameters(static_attributes=static_attributes, holidays=holidays, index_dependencies=index_dependencies, close_prices=close_prices, daily_parameters=daily_parameters, dep_daily_parameters=dep_daily_parameters,
                               constituents_dep=constituents_dep, bond_ref_data=bond_ref_data, constituents=constituents, equity_constituents_prices=equity_constituents_prices, ref_data=ref_data, corp_actions=corp_actions,
                               fx_rates=fx_rates, regions=regions, sectors=sectors, dep_tickers=dep_tickers, n6_data=n6_data, universe=universe, schema_content=schema_content)


    def convert_to_inputfmt(self,date):
        #convert date fmt from 20180101 to 2018-01-01
        return date[:4] + '-' + date[4:6] + '-' + date[6:8]


    def get_input_data_pickle(self,methodology,files,**kwargs):
        calculator, error = MessageLoop._get_calculator_by_methodology(methodology)
        fheader = True
        data = []
        header = []
        #converting dates according to input fmt
        start_date = self.convert_to_inputfmt(kwargs["start_date"])
        end_date = self.convert_to_inputfmt(kwargs["end_date"])
        excludedates = [datetime.strftime(value,"%Y-%m-%d") for value in kwargs["exclude_dates"]]
        if kwargs["storage_location"].upper() == "AWS":
            s3resource = kwargs["s3resource"]
            with BytesIO() as pickledata:
                s3resource.Bucket('i6-files-eu-west-1').download_fileobj(files[0], pickledata)
                pickledata.seek(0)
                d = load(pickledata)
                header = d[0][0]
                data.extend(d[0][1:])
        else:
            for file in files:
                with open(file, 'rb') as handle:
                    d = load(handle)
                #d = pd.read_pickle(file)
                if fheader:
                    header = d[0][0]
                    data.extend(d[0][1:])
        data =  [v for v in data if v[2] >= start_date and v[2] <= end_date  and v[2] not in excludedates]
        return data,header

    def get_input_equity_data_pickle(self, methodology, files, **kwargs):
        calculator, error = MessageLoop._get_calculator_by_methodology(methodology)
        fheader = True
        data = []
        header = []
        # converting dates according to input fmt
        start_date = self.convert_to_inputfmt(kwargs["start_date"])
        end_date = self.convert_to_inputfmt(kwargs["end_date"])
        excludedates = [datetime.strftime(value, "%Y-%m-%d") for value in kwargs["exclude_dates"]]
        if kwargs["storage_location"].upper() == "AWS":
            s3resource = kwargs["s3resource"]
            with BytesIO() as pickledata:
                s3resource.Bucket('i6-files-eu-west-1').download_fileobj(files[0], pickledata)
                pickledata.seek(0)
                d = load(pickledata)
        else:
            for file in files:
                with open(file, 'rb') as handle:
                    d = load(handle)
                    # d = pd.read_pickle(file)

        return d

    def get_output_enhanced(self, methodology, input_data, input_header, **kwargs):
        bheader = True
        header_data = None
        data = []
        calculator, error = MessageLoop._get_calculator_by_methodology(methodology)
        if len(input_data)==0:
            raise Exception("No data found in the input file with the filter specified!! Please check....")
        Row = namedtuple('Input', input_header[4:])
        for r in input_data:
            if r[3]=="Success":
                i_data = r[4:]
                row = Row(*i_data)
                try:
                    output = calculator._calc(row)
                    if bheader:
                        header_data = util.get_input_header_enhanced(output)
                        bheader = False
                    line = util.save_get_data(output)
                    line = [r[0],r[1],r[2],r[3]] + line
                    data.append(line)
                except Exception as err:
                    data.append([r[0],r[1],r[2],traceback.format_exc()])
            else:
                line = [r[0],r[1],r[2],r[3]]
                data.append(line)
        if bheader:
            header_data = ["methodology","ticker","date","error"]
        if 'log_output_resut' in kwargs.keys():
            self.log_csv(methodology,"/backtesting/allreports/", data, header_data, kwargs['log_output_resut'],kwargs['env'],kwargs['date_range'],kwargs['rundir_name'])
        return data, header_data

    def get_output_enhanced_covercall(self, methodology, input_data, input_header,**kwargs):
        bheader = True
        header_data = None
        data = []
        calculator, error = MessageLoop._get_calculator_by_methodology(methodology)
        if len(input_data) == 0:
            raise Exception("No data found in the input file with the filter specified!! Please check....")
        Row = namedtuple('Input', input_header[4:])
        for r in input_data:
            if r[3] == "Success":
                i_data = r[5:]
                cal_reb_input= r[4]

                row = Row(*i_data)
                try:
                    output = calculator._calc(row,cal_reb_input)
                    if bheader:
                        header_data = util.get_input_header_enhanced(output)
                        bheader = False
                    line = util.save_get_data(output)
                    line = [r[0], r[1], r[2], r[3]] + line
                    data.append(line)
                except Exception as err:
                    data.append([r[0], r[1], r[2], traceback.format_exc()])
            else:
                line = [r[0], r[1], r[2], r[3]]
                data.append(line)
        if bheader:
            header_data = ["methodology", "ticker", "date", "error"]
        if 'log_output_resut' in kwargs.keys():
            self.log_csv(methodology, "/backtesting/allreports/", data, header_data, kwargs['log_output_resut'],
                         kwargs['env'], kwargs['date_range'], kwargs['rundir_name'])
        return data, header_data

    def get_output_enhanced_new_base_calc(self, calc_api,ticket,methodology, input_data, **kwargs):
        bheader = True
        header_data = ['methodology', 'ticker', 'date','Status']
        out_data = []
        start_date=CalcApi.string_to_date(kwargs["start_date"])
        end_date=CalcApi.string_to_date(kwargs["end_date"])

        for ticker in kwargs["ticker"]:
            date = start_date
            offset = (end_date - date).days + 1
            holidays = []


            try:
                holobjectname = [ val for val in dir(input_data[ticker].holidays) if val.lower() == "holiday_calendar"][0]
                holidays = [numpydate.astype(datetime) for numpydate in getattr(input_data[ticker].holidays, holobjectname).holidays]
            except:
                print("Holidays are not present in tuple")



            while date <= end_date:
                temp_data = []
                if date in kwargs["exclude_dates"]:
                    date = date + timedelta(days=1)
                    continue
                dateinInput = False
                if date in input_data[ticker].daily_parameters.index:
                    dateinInput = True

                if ((dateinInput or datetime.weekday(date) not in [5,6]) and date not in holidays):
                    #print('Processing ' + methodology + " ticker " + ticker + ', date ' + CalcApi.date_to_string(date))
                    logging.debug('Processing ' + ticker + ', date ' + CalcApi.date_to_string(date) + '\n\n')
                    self.calculator,error = MessageLoop._get_calculator_by_methodology(methodology)
                    self.calculator.calc_model.ticket = ticket
                    self.calculator.calc_model.index = ticker
                    self.calculator.calc_model.calculation_id = 12142445
                    self.calculator.calc_model.date_t = date
                    self.calculator.dep_tickers = input_data[ticker].dep_tickers

                    try:

                        output = self.calculator._calc(inputs=input_data[ticker], calc_model=self.calculator.calc_model)
                        if bheader:
                            header_data = header_data +  list(output.output_daily_params.keys())
                            bheader = False
                        # added to manage extra columns which comes while rebalancing
                        new_header_data = list(output.output_daily_params.keys())
                        outlist = list(output.output_daily_params.values())
                        if header_data != new_header_data:
                            distinctcolumn = list(set(new_header_data) - set(header_data))
                            if  distinctcolumn:
                                header_data = header_data + distinctcolumn
                            outlist = [ output.output_daily_params.get(val,None) for val in header_data[4:] ]


                        temp_data.extend([methodology,ticker,date])
                        temp_data.append("Success")
                        out = [CalcApi.date_to_string(x)  if util.validatedateInstance(x) else x for x in outlist]
                        temp_data = temp_data + out
                    except Exception as err:
                        temp_data.extend([methodology, ticker, date,traceback.format_exc()])
                else:
                    reason = 'Weekend' if (datetime.weekday(date) == 5 or datetime.weekday(date) == 6) else 'Index holiday'
                    holiday_message = reason + ', calculation skipped (ticker: ' + ticker + ', ' + CalcApi.date_to_string(
                        date) + ')'
                    temp_data.extend([methodology, ticker, date,reason])
                    logging.debug(holiday_message)
                date = date + timedelta(days=1)
                out_data.append(temp_data)
        if 'log_output_resut' in kwargs.keys():
            self.log_csv(methodology, "/backtesting/allreports/", out_data, header_data, kwargs['log_output_resut'],
                         kwargs['env'], kwargs['date_range'],kwargs['rundir_name'])
        return header_data,out_data

    def getbenchmarkfile(self,env,methodology,ticker,date_range,location,rundirname,s3resource):
        copybenchmarkdst = '{0}/backtesting/allreports/{1}/{2}/{3}/{4}/{5}'.format(os.path.dirname(__file__), env, methodology, rundirname,ticker,
                                                                               "benchmark_" + ticker + "_" + date_range + ".csv")
        if location == "AWS":
            s3 = s3resource
            s3filelocation = 'calc-build-files/test/benchmark/{0}/{1}/{2}.csv'.format( env, methodology.lower(), ticker + "_" + date_range )
            s3.Bucket("i6-files-eu-west-1").download_file(s3filelocation,copybenchmarkdst)
        else:
            benchmarkfile = '{0}/backtesting/benchmark/{1}/{2}/{3}.csv'.format(os.path.dirname(__file__), env, methodology.lower(),
                                                                           ticker + "_" + date_range)

            shutil.copyfile(benchmarkfile, copybenchmarkdst)
        return copybenchmarkdst

    def init_subindex_from_mapping(self,**kwargs):
        try:
            self.subindex_undertest=[val.split("_")[0] for val in self.config['headlineindex'][kwargs['ticker'][0]].split(';')[1].split(',')]
            self.subindex_undertest_old = [val for val in self.config['headlineindex'][kwargs['ticker'][0]].split(';')[1].split(',')]
        except:
            raise Exception("Subindex Does not Exist in equity_backtest_config.ini")

    def init_config_parser(self):
        self.config = configparser.ConfigParser()
        self.config.read('equity_backtest_config.ini')

    def init_equity_column(self):
        self.constituentcolumn = (self.config['Constituent']['Columns']).split(",")
        self.dailyparamcolumn = (self.config['DailyParameter']['Columns']).split(",")
        self.errorfilecolumn = (self.config['errorfile']['Columns']).split(",")
        self.holidayfilecolumn = (self.config['holidayfile']['Columns']).split(",")

    def get_equity_files(self,type,**kwargs):
        equity_dir = 'backtesting_equity/output/newframework/'
        if type=='equity_old':
            equity_dir = 'backtesting_equity/output/oldframework/'

        OUTPUT_DIRECTORY = os.path.join(os.path.dirname(__file__), equity_dir,
                                        kwargs['ticker'][0])
        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)
        const_csvfile = os.path.join(OUTPUT_DIRECTORY,
                                          "Constituent_" + kwargs['ticker'][0] + "_" + kwargs['date_range'] + ".csv")
        dailyparam_csvfile = os.path.join(OUTPUT_DIRECTORY, "Dailyparam_" + kwargs['ticker'][0] + "_" + kwargs[
            'date_range'] + ".csv")
        error_csvfile = os.path.join(OUTPUT_DIRECTORY, "Error_" + kwargs['ticker'][0] + "_" + kwargs[
            'date_range'] + ".csv")
        holiday_input_error_csvfile = os.path.join(OUTPUT_DIRECTORY, "holiday_inputerr_" + kwargs['ticker'][0] + "_" + kwargs[
            'date_range'] + ".csv")

        return const_csvfile,dailyparam_csvfile,error_csvfile,holiday_input_error_csvfile

    def init_equity_column_name(self):
        self.parentindex_column_name = self.constituentcolumn[0]
        self.subindex_column_name = self.constituentcolumn[1]
        self.fromDate_column_name = self.constituentcolumn[2]
        self.referenceDate_columnname = self.constituentcolumn[3]
        self.basketType_columnname = self.constituentcolumn[4]
        self.constituent_columnname = self.constituentcolumn[6]
        self.daily_param_subindex_column_name = self.dailyparamcolumn[1]
        self.daily_param_date_columnname = self.dailyparamcolumn[2]
        self.header_report_column = ['Ticker', 'Days Tested', 'Days Failed', 'Days Failed List']
        self.Summary_report_column = ['Summary', 'parameter']
        self.suffix_list = ["_benchmark", "_Actual", "_delta"]

    def init_equity_param(self,writeheader,**kwargs):
        #init all files and parameter required for Old as well as new calculator
        self.init_config_parser()
        self.init_equity_column()
        self.init_equity_column_name()
        self.init_subindex_from_mapping(**kwargs)
        self.init_equityfiles(writeheader,**kwargs)

    def init_equityfiles(self,writeheader,**kwargs):
        self.const_csvfile,self.dailyparam_csvfile,self.error_csvfile,self.holiday_input_errfile = self.get_equity_files('equity_new',**kwargs)
        self.const_csvfile_old, self.dailyparam_csvfile_old,self.error_csvfile_old,self.holiday_input_errfile_old = self.get_equity_files('equity_old', **kwargs)
        if writeheader:
            self.writeHeader([self.const_csvfile,self.const_csvfile_old],self.constituentcolumn)
            self.writeHeader([self.dailyparam_csvfile,self.dailyparam_csvfile_old],self.dailyparamcolumn)
            self.writeHeader([self.error_csvfile, self.error_csvfile_old], self.errorfilecolumn)
            self.writeHeader([self.holiday_input_errfile, self.holiday_input_errfile_old], self.holidayfilecolumn)


    def getreportfileName(self,reportname,**kwargs):
        report_dir = 'backtesting_equity/allreport/'

        OUTPUT_DIRECTORY = os.path.join(os.path.dirname(__file__), report_dir,
                                        kwargs['ticker'][0])
        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)
        report_csvfile = os.path.join(OUTPUT_DIRECTORY,
                                     reportname + kwargs['ticker'][0] + "_" + kwargs['date_range'] + ".csv")

        return report_csvfile

    def populateDelta(self,df,c1,c2):
        # if df[c1] != 0:
        #     delta = abs((df[c1] - df[c2])/df[c1])
        # else:
        #     delta = abs(df[c1] - df[c2])

        delta = abs(df[c1] - df[c2])
        if delta != 0:
            digitbeforeDecimal = len(str(int(df[c1])))
            digitafterDecimal =  str(df[c1])[::-1].find('.')
            totalDigit = digitbeforeDecimal + digitafterDecimal
            if totalDigit < 15 :
                deltaCompare = digitafterDecimal -1
            else:
                deltaCompare = 15 - digitbeforeDecimal

            # deltaCompare = digitafterDecimal -1

            benchmarkdelta = 0 if (1 / 10) ** deltaCompare == 1 else (1 / 10) ** deltaCompare


            if delta <= benchmarkdelta:
                delta =  0

        return delta



    def writedailyParamDelta(self,**kwargs):
        df1 = pd.read_csv(self.dailyparam_csvfile_old)
        df2 = pd.read_csv(self.dailyparam_csvfile)
        suffix_list = self.suffix_list
        new_column_list = list(df1.columns[:3])
        df3 = df1.merge(df2, on=new_column_list, how='outer', suffixes=suffix_list[:2])
        for c in df1.columns[3:]:
            bm_clm_name, ac_clm_name, dt_clm_name = c + suffix_list[0], c + suffix_list[1], c +suffix_list[2]
            df3[dt_clm_name] = df3.apply(self.populateDelta,args=(bm_clm_name, ac_clm_name),axis=1)
            # df3[dt_clm_name] = df3[bm_clm_name] - df3[ac_clm_name]
            new_column_list.extend([bm_clm_name, ac_clm_name, dt_clm_name])
        df3 = df3[new_column_list]
        reportFile = self.getreportfileName("delta_dailyparam_",**kwargs)
        df3.to_csv(reportFile, index=False)

    def writeConstdelta(self,**kwargs):
        df1 = pd.read_csv(self.const_csvfile_old)
        df2 = pd.read_csv(self.const_csvfile)
        suffix_list = self.suffix_list
        new_column_list = list(df1.columns[:5]) + [self.constituent_columnname]
        df3 = df1.merge(df2, on=new_column_list,
                        how='outer', suffixes=suffix_list[:2])

        delta_column = [item for item in df1.columns if item not in new_column_list]
        for c in delta_column:
            bm_clm_name, ac_clm_name, dt_clm_name = c + suffix_list[0], c + suffix_list[1], c + suffix_list[2]
            if ptypes.is_string_dtype(df3[bm_clm_name]):
                df3[dt_clm_name] = df3[bm_clm_name].str.lower() == df3[ac_clm_name].str.lower()
            else:
                df3[dt_clm_name] = df3.apply(self.populateDelta, args=(bm_clm_name, ac_clm_name), axis=1)
                # df3[c + suffix_list[2]] = df3[c + suffix_list[0]] - df3[c + suffix_list[1]]
            new_column_list.extend([bm_clm_name, ac_clm_name, dt_clm_name])
        df3 = df3[new_column_list]
        reportFile = self.getreportfileName( "delta_constituent_", **kwargs)
        df3.to_csv(reportFile, index=False)

    def createDeltaFile(self,**kwargs):
        self.writedailyParamDelta(**kwargs)
        self.writeConstdelta(**kwargs)

    def isListEmpty(self,inList):
        if isinstance(inList, list):  # Is a list
            return all(map(self.isListEmpty, inList))
        return False

    def Summary(self,df,c1,c2):
        return "NewFramework- " + str(df[c2]) + "!= OldFramework- " + str(df[c1])




    def createconstreport(self,precesion,**kwargs):
        const_reportFile = self.getreportfileName("delta_constituent_", **kwargs)
        df = pd.read_csv(const_reportFile)
        delta_columns = [val.split("_")[0] for val in list(df.columns[6:]) if "delta" in val]
        header_column = self.header_report_column + delta_columns
        summary_column = list(df.columns[1:6]) + self.Summary_report_column
        uniqueDays = df[self.referenceDate_columnname].unique().tolist()
        Summary_rows = [header_column]
        suffix_list = self.suffix_list
        mismatch_dfs_list = []
        passstatus = True
        delta = 0 if (1/10)**precesion == 1 else (1/10)**precesion

        for ticker in df[self.subindex_column_name].unique().tolist():
            ticker_mismatch_df = []
            ticker_list_dict = {}
            uniq_rows_ticker = df.loc[df[self.subindex_column_name] == ticker]
            totalDataPoint = len(uniq_rows_ticker.index)
            for c in delta_columns:
                columnName = c + suffix_list[2]
                delta_dataframe = pd.DataFrame(columns=summary_column)
                if uniq_rows_ticker[columnName].dtype == pd.np.dtype('bool'):
                    delta_filtered = uniq_rows_ticker[uniq_rows_ticker[columnName] != True]
                    attribute_failed_count = delta_filtered[columnName].count()

                else:
                    delta_filtered = uniq_rows_ticker[uniq_rows_ticker[columnName].abs() > delta]
                    attribute_failed_count = delta_filtered[columnName].count()
                ticker_list_dict[columnName] = str((attribute_failed_count / totalDataPoint) * 100) + "%"
                delta_dataframe[self.subindex_column_name] = delta_filtered[self.subindex_column_name]
                delta_dataframe[self.referenceDate_columnname] = delta_filtered[self.referenceDate_columnname]
                delta_dataframe[self.fromDate_column_name] = delta_filtered[self.fromDate_column_name]
                delta_dataframe[self.basketType_columnname] = delta_filtered[self.basketType_columnname]
                delta_dataframe[self.constituent_columnname] = delta_filtered[self.constituent_columnname]
                delta_dataframe['Summary'] = "Actual- " + delta_filtered[c + suffix_list[1]].map(str) + "!= Benchmark- " + delta_filtered[c + suffix_list[0]].map(str)
                delta_dataframe['parameter'] = c
                mismatch_dfs_list.append(delta_dataframe)
                ticker_mismatch_df.append(delta_dataframe)

            if not self.isListEmpty(ticker_mismatch_df):
                ticker_df = pd.concat(ticker_mismatch_df, ignore_index=True)
                Days_Failed = ticker_df[self.referenceDate_columnname].nunique()
                Days_Failed_list = ticker_df[self.referenceDate_columnname].unique().tolist()
            else:
                Days_Failed = 0
                Days_Failed_list= []
            row = [ticker, len(uniqueDays), Days_Failed,Days_Failed_list]
            for c in delta_columns:
                row.append(ticker_list_dict[c + suffix_list[2]])
            Summary_rows.append(row)

        if not self.isListEmpty(mismatch_dfs_list):
            df2 = pd.concat(mismatch_dfs_list, ignore_index=True)
            df_list = [list(df2.columns)] + df2.values.tolist()
            passstatus = False
        else:
            df_list = []
        self.writeReportRows(Summary_rows, df_list, "report_constituent_", **kwargs)
        return passstatus,df_list,Summary_rows




    def createdailyparamreport(self,precesion,**kwargs):
        dailyParam_reportFile = self.getreportfileName("delta_dailyparam_", **kwargs)
        df = pd.read_csv(dailyParam_reportFile)
        delta_columns = [val.replace("_delta","") for val in list(df.columns[2:]) if "delta" in val]
        header_column = self.header_report_column  + delta_columns
        summary_column = list(df.columns[1:3]) + self.Summary_report_column
        uniqueDays = df[self.daily_param_date_columnname].unique().tolist()
        Summary_rows = [header_column]
        suffix_list = self.suffix_list
        mismatch_dfs_list = []
        passstatus = True
        delta = 0 if (1 / 10) ** precesion == 1 else (1 / 10) ** precesion

        for ticker in df[self.daily_param_subindex_column_name].unique().tolist():
            ticker_mismatch_df = []
            ticker_list_dict = {}
            uniq_rows_ticker = df.loc[df[self.daily_param_subindex_column_name] == ticker]
            totalDataPoint = len(uniq_rows_ticker.index)
            for c in delta_columns:
                columnName = c + suffix_list[2]
                delta_dataframe = pd.DataFrame(columns=summary_column)
                if uniq_rows_ticker[columnName].dtype == pd.np.dtype('bool'):
                    delta_filtered = uniq_rows_ticker[uniq_rows_ticker[columnName] != True]
                    attribute_failed_count = delta_filtered[columnName].count()

                else:
                    delta_filtered = uniq_rows_ticker[uniq_rows_ticker[columnName].abs() > delta]
                    attribute_failed_count = delta_filtered[columnName].count()
                ticker_list_dict[columnName] = str((attribute_failed_count / totalDataPoint) * 100)
                delta_dataframe[self.daily_param_subindex_column_name] = delta_filtered[self.daily_param_subindex_column_name]
                delta_dataframe[self.daily_param_date_columnname] = delta_filtered[self.daily_param_date_columnname]
                delta_dataframe['Summary'] = "Actual- " + delta_filtered[c + suffix_list[1]].map(str) + "!= Benchmark- " + delta_filtered[c + suffix_list[0]].map(str)
                delta_dataframe['parameter'] = c
                mismatch_dfs_list.append(delta_dataframe)
                ticker_mismatch_df.append(delta_dataframe)
            if not self.isListEmpty(ticker_mismatch_df):
                ticker_df = pd.concat(ticker_mismatch_df, ignore_index=True)
                Days_Failed = ticker_df[self.daily_param_date_columnname].nunique()
                Days_Failed_list = ticker_df[self.daily_param_date_columnname].unique().tolist()
            else:
                Days_Failed = 0
                Days_Failed_list = []

            row = [ticker, len(uniqueDays), Days_Failed,Days_Failed_list]
            for c in delta_columns:
                row.append(ticker_list_dict[c + suffix_list[2]])
            Summary_rows.append(row)

        if not self.isListEmpty(mismatch_dfs_list):
            df2 = pd.concat(mismatch_dfs_list, ignore_index=True)
            df_list = [list(df2.columns)] + df2.values.tolist()
            passstatus =  False
        else:
            df_list = []
        self.writeReportRows(Summary_rows, df_list, "report_dailyparam_", **kwargs)
        return passstatus,df_list,Summary_rows

    def writeReportRows(self,Summary_rows,df_list,reportname,**kwargs):
        # To add blank rows in between
        Summary_rows.append([])
        Summary_rows.append([])

        reportFile = self.getreportfileName(reportname, **kwargs)
        with open(reportFile, 'w', newline='') as file_ptr:
            file_writer = csv.writer(file_ptr)
            for r in Summary_rows:
                file_writer.writerow(r)
            for r_df in df_list:
                file_writer.writerow(r_df)

    def retReducedlog(self,mismatchRows):
       return mismatchRows[:10] if  len(mismatchRows) > 10 else mismatchRows[:len(mismatchRows)-1]


    def createReportFile(self,precesion,**kwargs):
        constReportPassed,ConstmismatchRows,ConstSummary = self.createconstreport(precesion,**kwargs)
        dailyParamPassed, dailyParammismatchRows,dailyParamSummary = self.createdailyparamreport(precesion,**kwargs)
        table_error_Const = AsciiTable(ConstSummary)
        table_error_dailyparam = AsciiTable(dailyParamSummary)
        table_error_const_mismatch =  AsciiTable(self.retReducedlog(ConstmismatchRows))
        table_error_dlyparam_mismatch = AsciiTable(self.retReducedlog(dailyParammismatchRows))

        if (not constReportPassed) or (not dailyParamPassed):
            raise Exception("Const Report Summary is  %s  "
                            "\n daily param Report Summary is  %s "
                            "\n Detailed Error Logs (First 10 logs from Constituent) %s "
                            "\n Detailed Error Logs (First 10 logs from dailyparam) %s"% (
                            table_error_Const.table,
                            table_error_dailyparam.table,
                            table_error_const_mismatch.table,
                            table_error_dlyparam_mismatch.table
                            ))




    def get_equity_output_enhanced_new_base_calc(self, calc_api,ticket,methodology, data, **kwargs):
        out_data = []
        const_file_ptr = open(self.const_csvfile, 'a', newline='')
        dailyparam_file_ptr = open(self.dailyparam_csvfile, 'a', newline='')
        err_file_ptr = open(self.error_csvfile, 'a', newline='')
        err_file_writer = csv.writer(err_file_ptr)
        holiday_input_err_file_ptr = open(self.holiday_input_errfile, 'a', newline='')
        holiday_input_err_writer = csv.writer(holiday_input_err_file_ptr)

        dailyparam_file_writer = csv.writer(dailyparam_file_ptr)
        start_date = CalcApi.string_to_date(kwargs["start_date"])
        end_date = CalcApi.string_to_date(kwargs["end_date"])
        filter_data = [val  for val in data if val[2] >= start_date and  val[2] <= end_date]
        for day_data in filter_data:
            temp_data = []
            methodology,ticker,date,status,input_data = day_data[0],day_data[1],day_data[2],day_data[3],None if len(day_data)< 5 else day_data[4]
            if ((not (CalcApi.date_to_string(date) in kwargs["exclude_dates"])) and (status=='Success')):
                logging.debug('Processing ' + ticker + ', date ' + CalcApi.date_to_string(date) + '\n\n')
                self.calculator, error = MessageLoop._get_calculator_by_methodology(methodology)
                self.calculator.calc_model = self.calculator._get_calc_model(ticket, ticker, date, 12345)
                calc_requirements = self.calculator.get_calculation_requirements()
                self.calculator._calc_requirements = calc_requirements if calc_requirements else CalculationRequirements()
                try:
                    output = self.calculator._calc(inputs=input_data, calc_model=self.calculator.calc_model)
                    temp_data.extend([methodology,ticker,date])
                    temp_data.append("Success")
                    temp_data.append(output)
                    self.create_readable_outputFile(temp_data, dailyparam_file_writer, const_file_ptr)
                except Exception as err:
                    err_file_writer.writerow([methodology, ticker, date,traceback.format_exc()])
                    # temp_data.extend([methodology, ticker, date,"Error",traceback.format_exc()])

            else:
                holiday_input_err_writer.writerow([methodology, ticker, date, status])
                # temp_data.extend([methodology, ticker, date, status])
            #out_data.append(temp_data)
        self.closeFiles([const_file_ptr,dailyparam_file_ptr,err_file_ptr,holiday_input_err_file_ptr])
        return out_data

    def closeFiles(self,files):
        for file in files:
            file.close()


    def get_equity_output_enhanced_old(self, calc_api,ticket, data, **kwargs):
        out_data = []
        const_file_ptr = open(self.const_csvfile_old, 'a', newline='')

        dailyparam_file_ptr = open(self.dailyparam_csvfile_old, 'a', newline='')
        dailyparam_file_writer = csv.writer(dailyparam_file_ptr)

        err_file_ptr = open(self.error_csvfile, 'a', newline='')
        err_file_writer = csv.writer(err_file_ptr)

        holiday_input_err_file_ptr = open(self.holiday_input_errfile_old, 'a', newline='')
        holiday_input_err_writer = csv.writer(holiday_input_err_file_ptr)

        start_date = CalcApi.string_to_date(kwargs["start_date"])
        end_date = CalcApi.string_to_date(kwargs["end_date"])
        filter_data = [val for val in data if val[2] >= start_date and val[2] <= end_date]

        for day_data in filter_data:
            temp_data = []
            methodology,ticker,date,status,input_data = day_data[0],day_data[1],day_data[2],day_data[3],None if len(day_data)< 5 else day_data[4]
            if ((not (CalcApi.date_to_string(date) in kwargs["exclude_dates"])) and (status=='Success')):
                logging.debug('Processing ' + ticker + ', date ' + CalcApi.date_to_string(date) + '\n\n')
                calculator, error = MessageLoop._get_calculator_by_methodology(methodology)
                try:
                    output = calculator._calc(input_data)
                    temp_data.extend([methodology,ticker,date])
                    temp_data.append("Success")
                    temp_data.append(output)
                    self.create_readable_outputFile_oldEquity(temp_data, dailyparam_file_writer, const_file_ptr,kwargs['ticker'][0])
                except Exception as err:
                    err_file_writer.writerow([methodology, ticker, date,traceback.format_exc()])
                    # temp_data.extend([methodology, ticker, date,"Error",traceback.format_exc()])

            else:
                holiday_input_err_writer.writerow([methodology, ticker, date, status])
                # temp_data.extend([methodology, ticker, date, status, ""])
            #out_data.append(temp_data)

        self.closeFiles([const_file_ptr,dailyparam_file_ptr,err_file_ptr,holiday_input_err_file_ptr])
        return out_data

    def create_pickle_file(self,output_data,type,**kwargs):
        rep_dir = "backtesting_equity/output/newframework/"
        if type == "equity_old":
            rep_dir = "backtesting_equity/output/oldframework/"
        OUTPUT_DIRECTORY = os.path.join(os.path.dirname(__file__) , rep_dir , kwargs['ticker'][0])
        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)
        filePath = os.path.join(OUTPUT_DIRECTORY,kwargs['ticker'][0] + "_" +  kwargs['date_range'] + ".pickle")
        with open(filePath, 'wb') as handle:
            dump(output_data, handle, protocol=HIGHEST_PROTOCOL)
        return filePath

    def read_output_pickelfile(self,**kwargs):
        OUTPUT_DIRECTORY = os.path.join(os.path.dirname(__file__), 'backtesting_equity/newFrameworkOutput/',
                                        kwargs['ticker'][0])
        filePath = os.path.join(OUTPUT_DIRECTORY, kwargs['ticker'][0] + "_" + kwargs['date_range'] + ".pickle")
        with open(filePath, 'rb') as handle:
            d = load(handle)
        return d

    def createWritableConstDf(self,const):
        constDf = const.constituents
        constDf[self.parentindex_column_name] = const.calculation_ticker
        constDf[self.subindex_column_name] = const.ticker
        constDf[self.fromDate_column_name] = const.from_date.strftime("%Y%m%d")
        constDf[self.referenceDate_columnname] = const.reference_date.strftime("%Y%m%d")
        constDf[self.basketType_columnname] = const.basket_type
        writableconst = constDf[self.constituentcolumn]
        return writableconst

    def createWritableConstDf_oldequity(self,const,ticker):
        constDf = pd.DataFrame(const['constituents'])
        constDf[self.parentindex_column_name] = ticker
        constDf[self.subindex_column_name] = const['ticker'].split("_")[0]
        constDf[self.fromDate_column_name] = const['fromDate']
        constDf[self.referenceDate_columnname] = const['referenceDate']
        constDf[self.basketType_columnname] = const['openOrClose']
        writableconst = constDf[self.constituentcolumn]
        return writableconst

    def writeDailyParamRow(self,data, dailyparam_file_writer):
        atrribute_valuedict = {}
        for value in data.output_daily_params:
            atrribute_valuedict[value.attribute.lower()] = value.value

        dailyparamList = [data.constituents[0].calculation_ticker,
                          data.index, data.Index_Level['date'],
                          data.Index_Level['value']
                          ]

        for column in self.dailyparamcolumn[4:]:
            dailyparamList.append(atrribute_valuedict.get(column, ""))

        dailyparam_file_writer.writerow(dailyparamList)

    def writeDailyParamRow_oldequity(self,data, dailyparam_file_writer,ticker):
        dailyparamList = [ticker,
                          data.index.split("_")[0], data.date_t.strftime("%Y%m%d"),
                          data.daily_params_t['Index_Level']
                          ]

        for column in self.dailyparamcolumn[4:]:
            dailyparamList.append(data.daily_params_t.get(column.upper(), ""))
        dailyparam_file_writer.writerow(dailyparamList)

    def writeHeader(self,files,header_row):
        for file in files:
            with open(file, 'w', newline='') as file_ptr:
                file_writer = csv.writer(file_ptr)
                file_writer.writerow(header_row)



    def create_readable_outputFile(self,output_data,dailyparam_file_writer,const_file_ptr):
        # pickleFile = r"C:\Users\anuj.garg2\PycharmProjects\IndexServices-I6-CalcEngine-python_Latest3\calc_engine\tests\backtesting_equity\newFrameworkOutput\EEUP_ALL_SUBS\EEUP_ALL_SUBS_20181011.pickle"
        # with open(pickleFile, 'rb') as handle:
        #     data = load(handle)

        if output_data and len(output_data) > 4:
                for v in output_data[4]:
                    if v.index in self.subindex_undertest:
                        self.writeDailyParamRow(v, dailyparam_file_writer)
                        for const in v.constituents:
                            if not const.constituents.empty:
                                WritableConstDf = self.createWritableConstDf(const)
                                WritableConstDf.to_csv(const_file_ptr, header=False, index=False)


    def create_readable_outputFile_oldEquity(self,output_data,dailyparam_file_writer,const_file_ptr,ticker):
        if output_data and len(output_data) > 4:
            currentIndex = output_data[4].index
            if currentIndex in self.subindex_undertest_old:
                self.writeDailyParamRow_oldequity(output_data[4], dailyparam_file_writer,ticker)
                for const in output_data[4].all_constituent_sets:
                    WritableConstDf = self.createWritableConstDf_oldequity(const,ticker)
                    WritableConstDf.to_csv(const_file_ptr, header=False, index=False)

    def output_from_pickled_data(self,pickle_output_data):
        const_file_ptr = open(self.const_csvfile, 'a', newline='')
        dailyparam_file_ptr = open(self.dailyparam_csvfile, 'a', newline='')
        dailyparam_file_writer = csv.writer(dailyparam_file_ptr)
        for output_data  in pickle_output_data:
            if output_data and len(output_data) > 4:
                    for v in output_data[4]:
                        if v.index in self.subindex_undertest:
                            self.writeDailyParamRow(v, dailyparam_file_writer)
                            for const in v.constituents:
                                if not const.constituents.empty:
                                    WritableConstDf = self.createWritableConstDf(const)
                                    WritableConstDf.to_csv(const_file_ptr, header=False, index=False)
        dailyparam_file_ptr.close()
        const_file_ptr.close()

    def delete_pickle_file(self,picklefile):
        if os.path.isfile(picklefile):
            os.remove(picklefile)

    def create_output_picklefile(self,output_data,type,**kwargs):
        pickleFile  = self.create_pickle_file(output_data,type,**kwargs)
        #pickeld_output_data =  self.read_pickled_file(pickleFile)
        #self.create_readable_outputFile(pickeld_output_data)
        #self.delete_pickle_file(pickleFile)

    def create_output_from_picklefile(self,**kwargs):
        d = self.read_output_pickelfile(**kwargs)
        self.output_from_pickled_data(d)




    def get_benchmarks_values(self, methodology, **kwargs):
        benchmark_values = []
        header = []
        env=kwargs["env"]
        date_range=kwargs["date_range"]
        tickers=kwargs["ticker"]

        try:
            for ticker in tickers:
                #name = os.path.basename(file).split('_Input')[0]
                benchmarkfile = self.getbenchmarkfile(env,methodology,ticker,date_range,kwargs["storage_location"].upper(),kwargs["rundir_name"],kwargs["s3resource"])
                with open(benchmarkfile) as f:
                    for i, line in enumerate(f):
                        if i > 0:
                            line = line.strip()
                            values = line.split(',')
                            #values_formatted = util.identify_formats(values[:])
                            # values_formatted = values[:]
                            benchmark_values.append(values)
                        else:
                            header = line.strip().split(',')
        except:
            print("Error,", tickers[0], ",", date_range, ",: Benchmark file missing for the methodology: ", methodology)
            logging.debug("Benchmark file missing for the methodology %s", methodology)

        return benchmark_values, header

    @staticmethod
    def convert_data_dict_outout_enhanced(data):
        cust_header = ["methodology","ticker", "date","Error"]
        mod_data=data[0:4]
        for item in data[4]:
            cust_header.append(item)
            mod_data.append(data[4][item])
        return cust_header,mod_data

    def append_error_log(self,isallcompare,attribute,errorLogs,errorLogs_index,error):
        if isallcompare and attribute.startswith("index_"):
            errorLogs_index.append(error)
        else:
            errorLogs.append(error)

    def checkHoliday(self,benchmarks_header,benchmark):
        isholiday= False
        if "index_holiday" in benchmarks_header:
            Holiday_index = benchmarks_header.index("index_holiday")
            if benchmark[0][Holiday_index] == '1':
                isholiday = True
        elif "holiday" in benchmarks_header:
            Holiday = benchmarks_header.index("holiday")
            if benchmark[0][Holiday] == '1':
                isholiday = True

        return isholiday

    def retDateinFloatFmt(self,inpDate):
        if isinstance(inpDate, str):
            return float(inpDate)
        else:
            return float(datetime.strftime(inpDate,"%Y%m%d"))



    def validate_data_enhanced(self, methodology, back_test_parameters, column_dict, output_data, output_header,
                      benchmark_values, benchmarks_header,isallcompare):

        errorLogs = []
        custlogs=[]
        errorLogs_index = []

        for data in output_data:

            benchmark = [ben for ben in benchmark_values if
                         (ben[0] == data[1] and (util.string_to_date(str(ben[1]), '%Y%m%d') == data[2])
                          or (str(util.string_to_date(str(ben[1]), '%Y%m%d')) == data[2]))]
            if (data[3]=="Success" and benchmark.__len__()!=0):
                custReport = [data[1], data[2]]

                try:
                    if output_header.index('final_results') > 0:
                        mod_output_header, data = util.convert_data_dict_outout_enhanced(data)
                except ValueError as e:
                    mod_output_header = output_header
                mod_output_header = [value.lower() for value in mod_output_header]
                benchmarks_header = [value.lower() for value in benchmarks_header]

                for back_test_parameter in back_test_parameters:
                    o_para_lower  = back_test_parameter.attribute.lower()
                    b_para_lower = column_dict[back_test_parameter.attribute].lower()
                    o_index = mod_output_header.index(o_para_lower) if o_para_lower in mod_output_header else None
                    b_index = benchmarks_header.index(b_para_lower) if b_para_lower in benchmarks_header else None
                    if o_index is None:
                        custCol = [column_dict[back_test_parameter.attribute], benchmark[0][b_index], "", ""]
                        custReport.append(custCol)
                        continue
                    elif b_index is None:
                        custCol = [column_dict[back_test_parameter.attribute], "", data[o_index], ""]
                        custReport.append(custCol)
                        continue
                    elif (o_index >= len(data)) and (not benchmark[0][b_index]) :
                        custCol = [column_dict[back_test_parameter.attribute], "", "", ""]
                        custReport.append(custCol)
                        continue

                    act_value, value = "", ""

                    if (o_index is not None and b_index is not None):
                        try:
                            value = self.retDateinFloatFmt(data[o_index]) if util.validate_date(
                                str(data[o_index])) else data[o_index]

                            act_value = benchmark[0][b_index]

                            delta = back_test_parameter.precision
                            decimalpart = act_value.split(".")
                            if len(decimalpart) > 1:
                                numberfterdecimal = len(decimalpart[1])
                                if numberfterdecimal < back_test_parameter.precision:
                                    delta = numberfterdecimal -1
                            delta = 1 / 10 ** delta
                            act_value = util.interpret(act_value)
                            if act_value is not None  and (not isinstance(act_value, str)):
                                custCol=[column_dict[back_test_parameter.attribute],act_value,value,(act_value-value)]
                                custReport.append(custCol)
                                tc = unittest.TestCase('__init__')
                                tc.assertAlmostEqual(value, act_value, None, None, delta)
                            else:
                                custCol = [column_dict[back_test_parameter.attribute], act_value, value,""]
                                custReport.append(custCol)

                        except AssertionError as e:
                            error = [data[1], data[2], e.args, back_test_parameter.attribute,
                                     column_dict[back_test_parameter.attribute]]
                            self.append_error_log(isallcompare, back_test_parameter.attribute, errorLogs, errorLogs_index, error)
                            # e.args += (column_dict[back_test_parameter.attribute], data[1],data[2],back_test_parameter.attribute)
                            # raise
                        except Exception as ex:
                            error = [data[1], data[2],
                                     ex.args, back_test_parameter.attribute,
                                     column_dict[back_test_parameter.attribute]]
                            self.append_error_log(isallcompare, back_test_parameter.attribute, errorLogs, errorLogs_index, error)
                            custCol = [column_dict[back_test_parameter.attribute], act_value, value, ex.args]
                            custReport.append(custCol)
                custlogs.append(custReport)
            elif (data[3] == "Success" and benchmark.__len__() == 0):
                error = [data[1], data[2],
                         "Holiday Calender: Data published on non business day. Not available in benchmarks",
                         "Holiday_Error",
                         data[3]]
                errorLogs.append(error)
                errorLogs_index.append(error)
            elif((data[3]=="Index holiday") and (benchmark.__len__()!=0)):
                if not self.checkHoliday(benchmarks_header,benchmark):
                        error = [data[1], data[2],
                             "Holiday Calender: Data not published!!! however available in benchmarks...",
                             "Holiday_Error",
                             data[3]]
                        errorLogs.append(error)
                        errorLogs_index.append(error)

            elif (data[3] != "Success" and data[3] != "Index holiday" and benchmark.__len__()!=0):
                error = [data[1], data[2],
                         "Insufficient Data: No Data Available in environment , available in benchmarks",
                         "Insufficient_Data",
                         data[3]]
                errorLogs.append(error)
                errorLogs_index.append(error)

        return errorLogs,custlogs,errorLogs_index


    def get_report_enhanced(self, methodology, output_data, output_header, errorlogs, back_test_parameters,
                            column_dict, custLogs, errorLogs_index,isallcompare,**kwargs):
        if 'log_custom_result' in kwargs.keys():
            for ticker in kwargs["ticker"]:
                self.get_custom_report( methodology, custLogs, kwargs["log_custom_result"], column_dict, kwargs['env'],
                                   kwargs['date_range'],kwargs['rundir_name'])

        if  errorlogs or errorLogs_index:
            out_df = pd.DataFrame(output_data, columns=output_header)
            table_data_error = self.generate_report_enhanced(out_df, errorlogs, back_test_parameters, column_dict)
            table_data_error_index = self.generate_report_enhanced(out_df, errorLogs_index, back_test_parameters, column_dict)
            table_error = AsciiTable(table_data_error)
            table_error_index = AsciiTable(table_data_error_index)
            if 'log_result_csv' in kwargs.keys():
                for ticker in kwargs["ticker"]:
                    self.log_result_csv( methodology, table_data_error, errorlogs, kwargs["log_result_csv"]+ "_All_Param_"+ticker, kwargs['env'],
                                     kwargs['date_range'],kwargs['ticker'],kwargs['rundir_name'])
                    self.log_result_csv(methodology, table_data_error_index, errorLogs_index,
                                        kwargs["log_result_csv"] + "_Index_level_" + ticker, kwargs['env'],
                                        kwargs['date_range'],kwargs['ticker'],kwargs['rundir_name'])

            if isallcompare:
                table,errorlogs_print = table_error_index,errorLogs_index
            else:
                table,errorlogs_print = table_error,errorlogs

            if errorlogs_print:
                if 'display_error_logs' in kwargs.keys():
                    raise Exception("Report Summay is  %s /n/n/n Detailed Error Logs (First configurable logs) %s " % (
                        table.table, errorlogs_print[:kwargs["display_error_logs"]]))
                else:
                    raise Exception("Report Summay is  %s /n/n/n Detailed Error Logs (First 100 Logs) %s " % (
                            table.table, errorlogs_print[:]))




    def get_custom_report(self,methodology,custLogs,name,column_dict,env,date_range,rundirname):
        if custLogs:
            header=util.get_custom_header(self,column_dict)
            data=[]
            for row in custLogs:
                rowData=[]
                rowData.append(row[0])
                rowData.append(row[1])
                for col in header[2:]:
                    for element in row[2:len(row)]:
                        if col.split("#")[0] == element[0]:
                            if col.split("#")[1] == "Benchmark":
                                rowData.append(element[1])
                            elif col.split("#")[1] == "Actual":
                                rowData.append(element[2])
                            else:
                                rowData.append(element[3])
                            break
                data.append(rowData)
            self.log_csv(methodology, "/backtesting/allreports/", data, header, name,env,date_range,rundirname)

    def get_custom_header(self,column_dict):
        cust_header=["ticker","Date"]
        for key, value in column_dict.items():
            cust_header.append(value + "#Benchmark")
            cust_header.append(value + "#Actual")
            cust_header.append(value + "#Delta")
        return cust_header

    def log_result_csv(self,methodology,table_data,errorlogs,name,env,date_range,tickers,rundirname):
        if table_data:
            OUTPUT_DIRECTORY = os.path.dirname(__file__) + '/backtesting/allreports/' + env + "/" +  methodology + "/" + rundirname +"/" + tickers[0]
            if not os.path.exists(OUTPUT_DIRECTORY):
                os.makedirs(OUTPUT_DIRECTORY)
            test_file_name = OUTPUT_DIRECTORY + "/" + name + "_" + date_range + ".csv"
            test_file = open(test_file_name, 'w', newline='')
            a = csv.writer(test_file, delimiter=',')
            a.writerows(table_data)
            a.writerows('\n\n\n')
            a.writerows([['Detailed logs as follows -->']])
            a.writerows([['ticker','date','error_message','out_param','out_name']])
            a.writerows(errorlogs)
            test_file.close()



    def log_csv(self,methodology,folder,output_data,output_header,name,env,date_range,rundirname):
        out_df = pd.DataFrame(output_data, columns=output_header)
        cols = out_df.columns.tolist()
        index_columnpos = 2
        if "methodology" in cols and cols[0] == "methodology":
            index_columnpos = 4
        #bringing index columns to front
        index_column_index = [idx for idx,value in enumerate(cols) if value.lower().startswith("index_") ]
        for idx,value in enumerate(index_column_index):
            columnvalue = cols[value]
            cols.pop(value)
            cols.insert(idx+index_columnpos,columnvalue)
        out_df = out_df[cols]
        tickers = list(set(out_df.ticker))
        OUTPUT_DIRECTORY=os.path.dirname(__file__) + folder + "/" + env + "/" +  methodology + "/" + rundirname + "/" + tickers[0]
        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)
        for ticker in tickers:
            data=out_df[(out_df.ticker == ticker)]
            test_file=OUTPUT_DIRECTORY + '/' + name + '_' + ticker + "_" + date_range + '.csv'
            data.to_csv(test_file,index=False)



    def generate_report_enhanced(self,out_df,error,back_test_parameters,column_dict):
        headers=util.get_headers_enhanced(self,back_test_parameters,column_dict)
        tickers= set(out_df.ticker)
        table_data = []
        dt=None


        if error:
            table_data.append(headers)
            err_df = pd.DataFrame(error, columns=['ticker','date','error_message','out_param','out_name'])
            for ticker in tickers:
                temp=[]
                temp.append(ticker)

                total = len(out_df[(out_df.ticker == ticker)].values)
                temp.append(total)

                r_fail=len(set(err_df[(err_df.ticker == ticker)].date))
                temp.append(r_fail)

                if r_fail != 0:
                    temp.append(str(round(((r_fail * 100) / total),2))+'%')
                    dt=set(err_df[(err_df.ticker == ticker)].date)
                    dt= [str(k) for k in dt]
                else:
                    dt = None
                    temp.append('0%')

                h_fail = len(err_df[(err_df.ticker == ticker) & (err_df.out_name == "Holiday_Error")].values)
                if h_fail != 0:
                    temp.append(str(round(((h_fail * 100) / total),2))+'%')
                else:
                    temp.append('0%')

                i_fail = len(err_df[(err_df.ticker == ticker) & (err_df.out_name == "Insufficient_Data")].values)
                if i_fail != 0:
                    temp.append(str(round(((i_fail * 100) / total), 2)) + '%')
                else:
                    temp.append('0%')

                for header in headers[6:-1]:
                    fail = len(err_df[(err_df.ticker == ticker) & (err_df.out_name == header)].values)
                    if fail !=0:
                        temp.append(str(round(((fail*100)/total),2))+'%')
                    else:
                        temp.append('0%')

                temp.append(dt)
                table_data.append(temp)

        return table_data

    @staticmethod
    def get_headers_enhanced(self, back_test_parameters, column_dict):
        h_t1 = [x.attribute for x in back_test_parameters]
        h_t2 = []
        for x in h_t1:
            h_t2.append(column_dict[x])
        h_t3 = ["Ticker", "Data Tested", "Days Failed", "%Days Failed", "Holiday_Error", "Insufficient_Data"]
        headers = h_t3 + h_t2 + ["Error Dates"]
        return headers



    @staticmethod
    def get_input_header_enhanced(object):
        if isinstance(object, (dict)):
            header = list(object.__dict__.keys())

        elif isinstance(object, (tuple)):
            header = list(object._fields)

        else:
            header = list(object.__dict__.keys())
        header = ['methodology', 'ticker','date','Error'] + header
        return header



    @staticmethod
    def save_get_data(o):
        if isinstance(o, (dict)):
            line = list(o.__dict__.values())[:]
        elif isinstance(o, (tuple)):
            line = list(o[:])
        else:
            line = list(o.__dict__.values())[:]
        return line


    @staticmethod
    def validate_date(d):
        try:
            x = datetime.strptime(d, '%Y-%m-%d')
            return True
        except ValueError:
            try:
                datetime.strptime(d, '%Y%m%d')
                return True
            except:
                return False

    @staticmethod
    def interpret(val):
        try:
            return int(val)
        except :
            try:
                return float(val)
            except :
                return val

    @staticmethod
    def string_to_date(string,pattern):
        t = time.strptime(string, pattern)
        return date(t.tm_year, t.tm_mon, t.tm_mday)

    @staticmethod
    def validatedateInstance(obj):
        isDate = False
        if isinstance(obj, date) or isinstance(obj, datetime):
            isDate = True
        return isDate





