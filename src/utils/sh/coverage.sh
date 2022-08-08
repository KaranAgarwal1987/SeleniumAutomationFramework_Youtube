#!/usr/bin/env bash

cd ${1}/calc_engine
source /home/n6i6/venv/$(cat ${1}/calc_engine/venv.txt)/bin/activate

export FAIL_UNDER=10

echo "##teamcity[progressStart 'Running code coverage']"
python -V
coverage --version
coverage html --fail-under=$FAIL_UNDER --omit=${1}/calc_engine/teamcity/*,${1}/calc_engine/tests/*,${HOME}/.pyenv/*,/usr/*

if [ "$?" != "0" ]; then
    echo "##teamcity[message text='Fatal error during code coverage. Check Test Coverage tab for more information.' status='ERROR']"
    exit 1
fi
