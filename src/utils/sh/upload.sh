#!/usr/bin/env bash

cd ${1}/calc_engine

export ENGINE_VERSION=1.5.${2}

echo "##teamcity[progressStart 'Upload artifacts']"

echo "__version__ = '${ENGINE_VERSION}'" > _version.py
export CALC_FILE_NAME=calc-engine-python-${ENGINE_VERSION}.zip
zip -r ${CALC_FILE_NAME} ./ -i '*.py' '*.cfg' '*.json' 'virtual_environment.txt' 'venv.txt' 'requirements.txt' '.python-version' './calc_meth/equity_new/data/carried_over_wm_rates.csv' './calc_meth/equity_new/data/euromoney_rates.csv' './calc_meth/equity_new/data/emix_mics.csv' -x './teamcity/**' './tests/**'

aws s3 cp ./${CALC_FILE_NAME} s3://i6-files-eu-west-1/engines/${CALC_FILE_NAME} --acl bucket-owner-full-control


