from os import getenv
from datetime import datetime


from migration_config import config
from migration_logging import logger
from migration_runner.migration_runner import MigrationRunner



def run_migration_rpt():
    """
    Method runs migration report.

    @return:
    """
    logger.info("Starting basket comparsion utilty...")
    MigrationRunner(config_objects=config.Config().genrate_dynamic_config_obj()).run_migration()

if __name__ == '__main__':
    run_migration_rpt()