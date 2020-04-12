#!/bin/bash

file_path=$1

egrep -o '(duration=[0-9]+)' $file_path | sed -e 's/.*=//g'
