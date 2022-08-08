import csv
import unittest
from time import gmtime, strftime
from tests.pat_util import util_input
from tests.backtest_function import BackTestParameter, TestUtils,BACKTESTING_ENV
from datetime import datetime,timedelta
import logging
import os
import warnings
import boto3



class BackTest:
    #Parameterized Constructor
    def __init__(self, methodology, ticker,start_date,end_date,date_range,type,backtest,ticket,calc_api,reportfilename,excludedates,islongrun=True,s3resource=None):
        self.methodology= methodology
        self.ticker     = ticker
        self.start_date = start_date
        self.end_date   = end_date
        self.date_range = date_range
        self.type       = type
        self.backtest   = backtest
        self.ticket     = ticket
        self.calc_api    = calc_api
        self.reportfilename = reportfilename
        self.excludedates = excludedates
        self.islongrun = islongrun
        self.s3resource = s3resource

    #Main logic is inside this function
    def worker(self):
        print("for Ticker : " + self.ticker + " ,Methodology : " + self.methodology)
        start_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        ticker = (self.ticker,)
        precision_level = None
        if self.backtest.split(":")[0].lower() == "all":
            back_test_parameters = None
            Column_dict          = None
            precision_level = int(self.backtest.split(":")[1])
        else:
            back_test_parameters = [BackTestParameter(value.split(":")[0],int(value.split(":")[2])) for value in
                                    self.backtest.split(";")]
            Column_dict = {value.split(":")[0]: value.split(":")[1] for value in self.backtest.split(";")}
        rundirname = "short_run"
        if self.islongrun:
            rundirname = "long_run"
        dict = {
            "env": BACKTESTING_ENV.QA.value,
            "ticker": ticker,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "date_range": self.date_range,
            "display_error_logs": 100,
            "log_output_resut": "Output",
            "log_result_csv": "Summary",
            "log_custom_result": "DailyParamComparison",
            "precision_level": precision_level,
            "exclude_dates": self.excludedates,
            "storage_location":"AWS",
            "s3resource":self.s3resource,
            "rundir_name":rundirname}


        status = TestUtils().get_input_enhanced(self.type, self.methodology, back_test_parameters, Column_dict, self.ticket, self.calc_api,
                                                **dict)
        end_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        self.reporter(start_time, end_time, status)
        return status
    #run class
    def run(self):
        print ("Starting " + self.ticker + " for methodology " +self.methodology)
        status = self.worker()
        print ("Exiting " + self.ticker+ " for methodology " +self.methodology)
        return status

    #Synchronised method to report status of every test in output csv file
    def reporter(self,start_time,end_time,status):
        print("Writing in file")
        with open(self.reportfilename, 'a') as csvfile:
            fieldnames = ['methodology', 'ticker', 'start_time','end_time','status','errmsg']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(
                {'methodology': self.methodology, 'ticker': self.ticker , 'start_time':start_time,'end_time': end_time, 'status':status[0] + "_" + self.date_range, 'errmsg':status[1]})


def retexcludedates(excludedatesstring):
    if excludedatesstring:
        splitdates = excludedatesstring.split(";")
        excludedaterange = []
        for datevalue in  splitdates:
            if len(datevalue.split(":")) > 1:
                startdate,enddate =  datetime.strptime(datevalue.split(":")[0], '%Y%m%d').date(),datetime.strptime(datevalue.split(":")[1], '%Y%m%d').date()
                excludedaterange  += [ startdate + timedelta(days=x) for x in range(0, (enddate - startdate).days + 1)]
            else:
                excludedaterange.append(datetime.strptime(datevalue, '%Y%m%d').date())
        return excludedaterange
    else:
        return []

#creation and execution of test takes place here
def runbacktest(row,ticket,calc_api,reportfilename,islongrun= False,s3resource =None):
    excludedates = []
    try:
        excludedates = retexcludedates(row[8])
    except:
        pass
    backtestObj = BackTest(row[1],row[2],row[3],row[4],row[5],row[6],row[7],ticket,calc_api,reportfilename,excludedates,islongrun,s3resource)
    return backtestObj.run()


def initlogging(logfilename):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    print(logfilename)
    logging.basicConfig(filename=logfilename, format='%(levelname)s,%(asctime)s,%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.ERROR)
def initdir(dirpath):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

def init_report_file(reportfile,productType):
    reportfiledirectory = os.path.dirname(__file__) + "/backtesting/log/"
    if productType == 'equity':
        reportfiledirectory = os.path.dirname(__file__) + "/backtesting_equity/log/"
    initdir(reportfiledirectory)
    reportfile = os.path.join(reportfiledirectory,reportfile)
    with open(reportfile, "w") as my_empty_csv:
        pass
    return reportfile

def closelogfiles():
    for handler in logging.root.handlers[:]:
        handler.close()
        logging.root.removeHandler(handler)

class ExecuteBackTest(unittest.TestCase):
    longMessage = True
    @classmethod
    def setUpClass(cls):
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")
        logdirpath = os.path.join(os.path.dirname(__file__) + "/backtesting/log/")
        initdir(logdirpath)
        cls.logfilename = os.path.join(logdirpath, "backtest.log")
        initlogging(cls.logfilename)
    @classmethod
    def tearDownClass(cls):
        closelogfiles()

def make_test_function(row,ticket,calc_api,reportfilename,islongrun=False,s3resource=None):
    def test(self):
        if islongrun:
            #for long run test end date is initialised to max date
            row[4]=row[5]
        status = runbacktest(row,ticket,calc_api,reportfilename,islongrun,s3resource)
        errmsg = "for ticker {0} and methodology {1} test is failed please check file {2} for further detail".format(row[2],row[1],reportfilename)
        print("Test success detail can be found in file: ", reportfilename)
        self.assertEqual(status[0],"pass",errmsg)
    return test

def initreportfile(islongrun,productType):
    reportfile = "test_backtest_report_short" + datetime.now().strftime('%m_%d_%Y_%I_%M_%S') + ".csv"
    if islongrun:
        reportfile = "test_backtest_report_long_" + datetime.now().strftime('%m_%d_%Y_%I_%M_%S') + ".csv"
    reportfile = init_report_file(reportfile,productType)
    return reportfile


def ret_testarg():
    calc_api, ticket = util_input().initialize_environment()
    s3resource = None
    try:
        s3resource = boto3.resource('s3')
    except:
        pass
    return calc_api,ticket,s3resource

def createtestlist(inputfile):
    testclassList = []
    if os.path.isfile(inputfile):
        calc_api, ticket, s3resource = ret_testarg()
        productType = 'nonEquity'
        if 'equity' in  inputfile:
            productType = 'equity'
        longrunreportfile = initreportfile(True,productType)
        shortrunreportfile = initreportfile(False,productType)
        print("Starting Reading {0}".format(inputfile))
        with open(inputfile, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if (row[0] == 'yes'):
                    #end date is taken as end date for short range test case
                    test_func = make_test_function(row, ticket, calc_api,shortrunreportfile,s3resource=s3resource)
                    #date range is taken as end date for long run test case
                    test_long_func = make_test_function(row, ticket, calc_api, longrunreportfile,islongrun=True,s3resource=s3resource)
                    klassname = 'Test_{0}_{1}'.format(row[2],row[5])
                    testclass = type(klassname,
                                                (ExecuteBackTest,),
                                                {'test_gen_{0}_{1}'.format(row[2],row[5]): test_func, 'test_long_run_gen_{0}_{1}'.format(row[2],row[5]): test_long_func})
                    testclassList.append(testclass)
    return testclassList

def createCustumisedTestList(inputfile,islongrun=True):
    calc_api, ticket, s3resource = ret_testarg()
    productType = 'nonEquity'
    if 'equity' in  inputfile:
        productType = 'equity'
    reportfile = initreportfile(islongrun,productType)
    testclassList = []
    print("Starting Reading {0}".format(inputfile))
    with open(inputfile, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if (row[0] == 'yes'):
                #date range is taken as end date for long run test case
                test_func = make_test_function(row, ticket, calc_api, reportfile,islongrun,s3resource=s3resource)
                testclassList.append((test_func,row[2],row[5]))
    return testclassList

def ret_testClass():
    currentDir = os.path.dirname(__file__)
    equity_inputfile =  os.path.join(currentDir,"equity_backtest_config.csv")
    nonequity_inputfile = os.path.join(currentDir , "backtest_config.csv")
    equityTestList = createtestlist(equity_inputfile)
    nonequityTestList = createtestlist(nonequity_inputfile)
    combinedtestclassnameList = equityTestList + nonequityTestList
    return combinedtestclassnameList


if __name__ == '__main__':
    currentDir = os.path.dirname(__file__)
    equity_inputfile = os.path.join(currentDir , "equity_backtest_config.csv")
    nonequity_inputfile = os.path.join(currentDir , "backtest_config.csv")
    #initialise so wile testing we can comment out any test
    equityTestList,nonequityTestList = [],[]
    #To Run Equity Test case
    equityTestList = createCustumisedTestList(equity_inputfile,False)
    #To Run NonEquity Test Case
    nonequityTestList = createCustumisedTestList(nonequity_inputfile,True)
    combinedtestclassnameList = equityTestList + nonequityTestList
    for row in combinedtestclassnameList:
        klassname = 'Test_{0}_{1}'.format(row[1],row[2])
        globals()[klassname] = type(klassname,
                                    (ExecuteBackTest,),
                                    {'test_gen_{0}_{1}'.format(row[1],row[2]): row[0]})

    unittest.main()