#!/bin/bash

egrep -o '(duration=[0-9]+)' | sed -e 's/.*=//g'
