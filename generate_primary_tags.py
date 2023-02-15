############################################
# Title:  generate_primary_tags.py         #
# Author: Jakub Štětina                    #
# Date:   7 Feb 2023                       #
############################################

import argparse
import sys
import os
import re
import time

SEARCH_PATTERN = r"<title>(.*?)(?<!\(disambiguation\))<\/title>"
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


if not input_file.endswith(".xml"):
    print("WARNING: Input file might not be in correct format (wanted: XML)\n")

if not os.path.exists(input_file):
    sys.stderr.write("ERROR: Input file not found\n")
    exit(1)

if os.path.exists(output_file):
    sys.stderr.write(f"ERROR: Output file '{output_file}' already exists\n")
    exit(1)


print("Starting")
start_time = time.time()

pt_data = {}
val_counter = 0
with open(input_file) as dump_file:
    for line in dump_file:
        match = reg.match(line.strip())
        if match:
            a_name = match.group(1).replace(" ", "_")
            
            if ",_" in a_name or "(" in a_name:
                pt_data[a_name] = 0
            else:
                pt_data[a_name] = 1

            val_counter += 1

with open(output_file, "w") as out_file:
    for key, value in pt_data.items():
        out_file.write(f"{key}\t{value}\n")



time_taken = int(time.time() - start_time)
print("Finished.")
print(f"Generated {val_counter} values, in {time_taken} seconds")
