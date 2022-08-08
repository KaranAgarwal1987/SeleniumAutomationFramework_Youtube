import configparser
import csv
import sys
from pathlib import Path


from migration_logging import logger
from migration_exceptions import ConfigException
from .csv_run_config import CSV_Run_Config

class Config:
    def __init__(self):
        self.config_file_path = Path(__file__).parent.parent / f'config_files/migration.ini'
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file_path)
        self.config_orderddict = {innerkey.strip():innervalue.strip() for key,value in self.config._sections.items() for innerkey,innervalue in value.items() }




    def genrate_dynamic_config_obj(self):
        config_obj = []
        try:
            self.config_file_path = self.config_orderddict.get('dynamic_config_filename') + sys.argv[1]
            print(self.config_file_path)
            with open(self.config_file_path) as csv_config_fileptr:
                reader = csv.DictReader(csv_config_fileptr)
                for csv_dict in reader:
                    csv_dict_keys_in_lower = {key.lower():value for key,value in csv_dict.items()}
                    csv_dict_keys_in_lower.update(self.config_orderddict)
                    config_obj.append(CSV_Run_Config(csv_dict_keys_in_lower))
        except Exception as exp:
            logger.error(str(ConfigException(exp)))
            raise ConfigException(exp)
        return config_obj