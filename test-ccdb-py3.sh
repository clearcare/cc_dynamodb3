#!/bin/bash

sudo add-apt-repository universe
sudo apt update
sudo apt-get install -y python3.11
sudo apt-get install -y python3.11-distutils

curl https://bootstrap.pypa.io/get-pip.py --output get-pip.py
sudo python3.11 get-pip.py

ls /usr/bin/python*
ls /usr/bin/pip*

python3.11 --version
pip3.11 --version

pip3.11 install pytest
pip3.11 install -r requirements.txt
pip3.11 install -r test_requirements.txt

echo "WORKING DIR $(pwd)"

git fetch

which pytest

export PYTHONPATH=`pwd`
pytest --version
pytest
