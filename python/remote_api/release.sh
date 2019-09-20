#!/bin/bash

cd $(dirname $0)

rm -r dist/*
python3 setup.py sdist bdist_wheel
