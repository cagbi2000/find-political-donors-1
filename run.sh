#!/bin/bash
#
# Use this shell script to compile (if necessary) your code and then execute it. Below is an example of what might be found in this file if your program was written in Python
#

# Compile
chmod a+x ./src/find_political_donors.py

# Run
/usr/bin/python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt

# Run Test
#/usr/bin/python ./src/find_political_donors.py ./insight_testsuite/tests/test_1/input/#itcont.txt ./insight_testsuite/tests/test_1/output/medianvals_by_zip.txt ./#insight_testsuite/tests/test_1/output/medianvals_by_date.txt