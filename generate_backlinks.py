############################################
# Title:  generate_backlinks.py            #
# Author: Jakub Štětina                    #
# Date:   8 Feb 2023                       #
############################################

import argparse
import csv
import sys
import os
import re
import time
import json

SEARCH_PATTERN = r"\[\[(?!:?\w+:)(?!#)(?!.*\(disambiguation\))(.+?)(?:(?:\||#).*?)?\]\]"
reg = re.compile(SEARCH_PATTERN)

io_parser = argparse.ArgumentParser()

io_parser.add_argument(
    "-i","--input", 
    type = str, 
    required=True,
    action="store",
    dest="input_file",
    help = "Input file",
)

io_parser.add_argument(
    "-o","--output", 
    type = str, 
    required=True,
    action="store",
    dest="output_file",
    help = "Output file",
)

args = io_parser.parse_args()

input_file = args.input_file
output_file = args.output_file

if not os.path.exists(input_file):
    sys.stderr.write("ERROR: Input file not found\n")
    exit(1)

if os.path.exists(output_file):
    sys.stderr.write(f"ERROR: Output file '{output_file}' already exists\n")
    exit(1)



start_time = time.time()

bl_data = {}
with open(input_file) as dump_file:
    for line in dump_file:
        match = reg.match(line)
        if match:
            a_name = match.group(1).replace(" ", "_")
            if a_name not in bl_data:
                bl_data[a_name] = 1
            else:
                bl_data[a_name] += 1

with open(output_file, "w") as out_file:
    for key, value in bl_data.items():
        out_file.write(f"{key}\t{value}\n")



time_taken = time.time() - start_time
print(f"It took {time_taken} seconds")