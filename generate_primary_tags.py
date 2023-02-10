############################################
# Title:  generate_primary_tags.py         #
# Author: Jakub Štětina                    #
# Date:   7 Feb 2023                       #
############################################

import argparse
import time
import csv
import sys
import os

csv.field_size_limit(sys.maxsize)
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


if not input_file.endswith(".tsv"):
    print("WARNING: Input file might not be in correct format (wanted: .tsv)\n")


if not os.path.exists(input_file):
    sys.stderr.write("ERROR: Input file not found\n")
    exit(1)

if os.path.exists(output_file):
    sys.stderr.write(f"ERROR: Output file '{output_file}' already exists\n")
    exit(1)

start_time = time.time()

print("Starting")
with open(input_file) as file_in, open(output_file, "w") as file_out:
    kb = csv.reader(file_in, delimiter="\t", )
    val_counter = 0
    for row in kb:
        try:
            link = row[8]
        except Exception:
            continue
        if link:
            article_name = link.split("/")[-1]
            
            if "(" in article_name or ",_" in article_name:
                file_out.write(f"{article_name}\t0\n")
            else:
                file_out.write(f"{article_name}\t1\n")
            val_counter += 1

time_taken = int(time.time() - start_time)
print("Finished.")
print(f"Generated {val_counter} values, in {time_taken} seconds")
