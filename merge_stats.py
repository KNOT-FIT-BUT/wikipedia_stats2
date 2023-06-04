############################################
# Title:  merge_stats.py                   #
# Author: Jakub Štětina                    #
# Date:   12 Feb 2023                      #
############################################

from kb_head import KB_HEAD_TEMPLATE
import argparse
import time
import csv
import sys
import os

csv.field_size_limit(sys.maxsize)
io_parser = argparse.ArgumentParser()

io_parser.add_argument(
    "-bl","--backlinks", 
    type = str, 
    required=True,
    action="store",
    dest="bl_file",
    help = "Input file",
)


io_parser.add_argument(
    "-pw","--pageviews", 
    type = str, 
    required=True,
    action="store",
    dest="pw_file",
    help = "Input file",
)

io_parser.add_argument(
    "-pr","--primary-tags", 
    type = str, 
    required=True,
    action="store",
    dest="pr_file",
    help = "Input file",
)

io_parser.add_argument(
    "-o","--output", 
    type = str, 
    required=True,
    action="store",
    dest="out_file",
    help = "Merged output file",
)


args = io_parser.parse_args()

bl_file = args.bl_file
pw_file = args.pw_file
pr_file = args.pr_file
out_file = args.out_file


# OUTPUT FILE FORMAT
# ARTICLE_NAME \t BACKLINKS \t PAGEVIEWS \t PRIMARY
 
out_data = {}

print("Starting")
with open(bl_file) as bl_in, open(pw_file) as pw_in, open(pr_file) as pr_in:
    
    # Files in order of output format
    in_files_list = [bl_in, pw_in, pr_in]

    for idx, file in enumerate(in_files_list, start=0):
        in_data = csv.reader(file, delimiter="\t")
        for val in in_data:
            art_name = val[0]
            try:
                count = int(val[1])
            except ValueError:
                continue

            if art_name not in out_data:
                out_data[art_name] = ["NF", "NF", "NF"]
            out_data[art_name][idx] = count

print("Saving data..")
with open(out_file, "w") as file_out:
    # KB HEAD
    for type, columns in KB_HEAD_TEMPLATE.items():
        file_out.write(type)
        for column in columns:
            file_out.write(column + "\t")
        file_out.write("\n")
    file_out.write("\n")

    # KB DATA
    for key, values in out_data.items():
        file_out.write(key)
        for value in values:
            file_out.write(f"\t{value}")
        file_out.write("\n")

print("Finished.")