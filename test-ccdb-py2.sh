#!/bin/bash

sudo add-apt-repository universe
sudo apt update
sudo apt-get install -y python2
curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py
sudo python2 get-pip.py

ls /usr/bin/python*
ls /usr/bin/pip*

echo "WORKING DIR $(pwd)"

pip2 install pytest
pip2 install -r py2_requirements.txt
pip2 install -r test_requirements.txt

git fetch

which pytest

export PYTHONPATH=`pwd`
pytest --version
pytest

