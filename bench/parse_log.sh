#!/bin/bash

file_path=$1

fgrep "Elapsed time" $file_path | egrep -o '[0-9]+'
