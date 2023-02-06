from pageviews import PageViews
import argparse
import csv
import sys
import time
import os

pw = PageViews(check_values=True)

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

# pw.parse_args()

if not os.path.exists(input_file):
    sys.stderr.write("ERROR: Input file not found\n")
    exit(1)

if os.path.exists(output_file):
    sys.stderr.write(f"ERROR: Output file '{output_file}' already exists\n")
    exit(1)

print("Starting")
with open(input_file) as file_in, open(output_file, "w") as file_out:
    kb = csv.reader(file_in, delimiter="\t", )
    val_counter = 0
    for row in kb:
        link = row[8]
        if link:
            article_name = link.split("/")[-1]
            pw_count = pw.get_page_views(article_name)
            if pw_count is not False:
                file_out.write(f"{article_name}\t{pw_count}\n")
            else:
                # Not found
                file_out.write(f"{article_name}\tNF\n")
            val_counter += 1 

print("Finished.")
print(f"Added {val_counter} values")
