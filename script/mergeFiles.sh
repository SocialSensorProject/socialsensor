#!/usr/bin/env bash
mkdir res
FILES=CSVOut*.csv
for f in $FILES
do
  echo "Processing $f file..."
  sort -k 1,1 $f > res/$f
done
cat res/CSVOut_result_A*.csv > merged_A.csv
cat res/CSVOut_result_B*.csv > merged_B.csv
cat res/CSVOut_result_C*.csv > merged_C.csv
cat res/CSVOut_result_D*.csv > merged_D.csv
