#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Version number must be input"
    echo "Number of arguments: $#"
    exit 1
fi

source ~/miniconda3/etc/profile.d/conda.sh

ver=$1
whl=$2  # Optional

new_env="testenv_$ver"

conda create --name $new_env python=3.12 -y  > /dev/null 2>&1
conda activate $new_env

if [ "$CONDA_DEFAULT_ENV" != "$new_env" ]; then
    echo "ERROR: Unable to create environment $new_env"
    exit 1
fi

if [ "$#" -eq 1 ]; then
    echo "Installing from PyPI"
    pip install openpolicedata==$ver > /dev/null 2>&1
else
    echo "Installing from $whl"
    pip install $whl > /dev/null 2>&1
fi

python -m opd_test_run

if [ $? -ne 0 ]; then
    # Prior to to v0.5.4, packaging library was not included in requirements when it should have been.
    echo 'Installing packaging'
    pip install packaging > /dev/null 2>&1
    python -m opd_test_run
fi

conda deactivate
conda remove --name $new_env --all -y > /dev/null 2>&1

rem_envs="$(conda env list)"
if [[ $rem_envs == *$new_env* ]]; then
    echo "ERROR: Unable to delete environment $new_env"
    exit 1
fi