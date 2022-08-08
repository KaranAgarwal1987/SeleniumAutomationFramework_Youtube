#!/usr/bin/env bash

cd ${1}/calc_engine
source /home/n6i6/venv/$(cat ${1}/calc_engine/venv.txt)/bin/activate

export PYTHONPATH=${1}/calc_engine
export PYLINT_JSON_OUTPUT=${PYTHONPATH}/pylint-json.output

mkdir ${PYTHONPATH}/pylint

echo "##teamcity[progressStart 'Running pylint code analysis']"
pylint --version
pylint -j 16 -E --output-format=json --disable=E1101,E1130 --rcfile=$PYTHONPATH/.pylintrc ${PYTHONPATH} >> ${PYLINT_JSON_OUTPUT}
pylintout=$?

if [ -s pylint-json.output ]; then
    echo "##teamcity[progressStart 'Converting pylint output to HTML']"
    pylint-json2html -o ${PYTHONPATH}/pylint/main.html ${PYLINT_JSON_OUTPUT}
fi

echo "pylint process exited with code $pylintout"

# 0 if everything went fine
# 1 if some fatal message issued
# 2 if some error message issued
# 4 if some warning message issued
# 8 if some refactor message issued
# 16 if some convention message issued
# 32 on usage error

if [ "$pylintout" == "1" ] || [ "$pylintout" == "2" ] ||  [ "$pylintout" == "32" ]; then
    echo "##teamcity[message text='pylint rules violation' errorDetails='Fatal error during code analysis. Check pylint tab for more information.' status='ERROR']"
fi
