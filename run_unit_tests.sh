#!/bin/bash

# Create a virtualenv and install required packages:
virtualenv dynamo_venv -p /usr/bin/python27 &> output_virtual_env.txt
. dynamo_venv/bin/activate &> output_activate.txt
pip install -r test_requirements.txt &> output_pip_install.txt

# We always want this to pass, because Jenkins reads the results.xml
# to determine a pass or fail (and if results.xml does not exist,
# Jenkins will fail the build):
if [ -z "${WORKSPACE}" ]; then
  WORKSPACE=`pwd`
  echo "WORKSPACE env var not specified. Defaulting to: ${WORKSPACE}"
fi

# Run the tests:
py.test --runxfail tests/ --junitxml=${WORKSPACE}/results.xml || true
