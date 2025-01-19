#!/bin/bash

set -e

if [ -d ./build ]; then
    echo "Clearing build directory..."
    rm -r ./build/*
fi

if [ -d ./dist ]; then
    echo "Clearing dist directory..."
    rm -r ./dist/*
fi

echo "Running build stage..."
python setup.py sdist bdist_wheel