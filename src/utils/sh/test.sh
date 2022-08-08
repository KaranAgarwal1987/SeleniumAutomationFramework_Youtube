#!/usr/bin/env bash

cd ${1}/calc_engine

export PYTHONPATH=${1}/calc_engine
echo ${HOME}
source /home/n6i6/venv/$(cat ${1}/calc_engine/venv.txt)/bin/activate
echo "##teamcity[progressStart 'Running unit tests']"
python3 -V
python3 -m pip freeze
python3 -m coverage run --branch ./tests/test.py --test-type=${2}