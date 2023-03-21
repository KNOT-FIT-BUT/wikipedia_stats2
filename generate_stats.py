############################################
# Title:  generate_stats.py                #
# Author: Jakub Štětina                    #
# Date:   12 Mar 2023                      #
############################################

from datetime import datetime

import logging
import subprocess
import sys
import os
import re

from generate_backlinks import Backlinks
from generate_primary_tags import PrimaryTags
from generate_pageviews import PageViews


TMP_DIR = "unmerged"
OUT_DIR = "test_out"

if not os.path.exists(TMP_DIR):
    os.mkdir(TMP_DIR)

if not os.path.exists(OUT_DIR):
    os.mkdir(OUT_DIR)


DATA_FILE = "data/last_update"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
SEC_IN_DAY = 86400

DUMP_DIR = "/mnt/minerva1/nlp/corpora_datasets/monolingual/{}/wikipedia"
FILE_NAME_REG = re.compile(r"^(?:cs|en|sk)wiki-\d{8}-pages-articles.xml$")
PROJECTS = {
    "en": "english",
    "cs": "czech", 
    "sk": "slovak"
}


# Converts epoch to date in this format: YYYY-MM-DD
def timestamp_to_date(timestamp:str) -> str:
    return datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')


# Loads the timestamp representing the last run of the script
# This file must be present in order for the script to work correcty
def load_prev_date():
    with open(DATA_FILE) as file_in:
        timestamp = file_in.readline()
        try:
            timestamp = int(timestamp)
        except ValueError:
            sys.stderr.write("Date to epoch conversion error\n")
            exit(1)    
    return timestamp

# Saves the current timestamp to a datafile
def update_date(timestamp):
    timestamp = str(timestamp)
    with open(DATA_FILE, "w") as file_out:
        file_out.write(timestamp)

print("Cleaning temp dir")
subprocess.run(f"rm -r {TMP_DIR}/*", shell=True)

# Get the latest dump for each project
dumps_info = {}
for key, value in PROJECTS.items():
    check_dir = DUMP_DIR.format(value)

    if not os.path.exists(check_dir):
        sys.stderr.write(f"Error: dump dir does not exist ({check_dir})\n")
        exit(1)

    if not os.path.exists(f"{TMP_DIR}/{key}"):
        os.mkdir(f"{TMP_DIR}/{key}")

    
    files = os.listdir(check_dir)

    dumps = [file for file in files if FILE_NAME_REG.match(file)]
    latest_dump = sorted(dumps)[-1]

    date = latest_dump.split("-")[1]
    year = int(date[:4])
    month = int(date[4:6])
    day = int(date[6:])

    latest_dump_timestamp = int(datetime(year, month, day).strftime("%s"))
    last_timestamp = load_prev_date()
    
    # Compare if new dumps exist
    if latest_dump_timestamp > last_timestamp:
        print(f"Latest dump ({key}): {latest_dump}")
        dumps_info[key] = {
                    "path":f"{check_dir}/{latest_dump}", 
                    "latest_timestamp": latest_dump_timestamp
                     } 
if len(dumps_info) == 0:
    print("Everything up to date.")
    exit(0)

# Every project must have a new dump in order for the script to run
if len(dumps_info) != len(PROJECTS):
    sys.stderr.write("Error: not all dumps are yet available\n")
    exit(1)


# Date range from the last update until now                  
start_date_timestamp = load_prev_date()
end_date_timestamp = min([item["latest_timestamp"] for item in list(dumps_info.values())])-SEC_IN_DAY

start_date = timestamp_to_date(start_date_timestamp)
end_date = timestamp_to_date(end_date_timestamp)

print("Date range:", start_date, end_date)

print("Generating pageviews")                   

subprocess.run("rm -rf pwout/*", shell=True)

# Generate pageviews
pw = PageViews(start_date, end_date, tmp_dir="pwtemp", output_dir="pwout")
pw.get_pageviews()

# Move generated pageviews to a temp dir
for prj in PROJECTS.keys():
    prcs = subprocess.run(f"mv pwout/{prj}* {TMP_DIR}/{prj}", shell=True)
    if prcs.returncode != 0:
        sys.stderr.write("Error while moving pageviews data\n")
        exit(1)

# Generate primary tags and backlinks
for prj, dump_info in dumps_info.items():
    
    dump_path = dump_info["path"]
    print(f"Current dump ({prj}):", dump_path)

    bl = Backlinks(dump_path, f"{TMP_DIR}/{prj}/backlinks.tsv")
    bl.generate_backlinks()
    del bl

    pt = PrimaryTags(dump_path, f"{TMP_DIR}/{prj}/prtags.tsv")
    pt.generate_ptags()
    del pt
    print("--------------------")



print("Merging data")
# Merge dataa to one file
for prj in dumps_info.keys():
    data_out = {}
    data_dir = f"{TMP_DIR}/{prj}"
    
    print(f"Merging project: {prj}")

    files = os.listdir(data_dir)
    for file_name in files:
        print(f"Processing file: {file_name}")
        with open(f"{data_dir}/{file_name}") as file_in:
            for line in file_in:
                line_data = line.strip().split("\t")
                article_name = line_data[0]

                # Empty line
                if(len(line_data) == 0):
                    continue

                try:
                    pw_value = int(line_data[-1])
                except ValueError:
                    sys.stderr.write("Error: value not a number\n")
                    sys.stderr.write(line)
                    exit(1)
                except IndexError:
                    sys.stderr.write(f"Unable to load this line {line.strip()}")
                    continue

                if not article_name in data_out:
                    data_out[article_name] = 0
                data_out[article_name] += pw_value
    print(f"Saving {prj} merge")
    with open(f"{OUT_DIR}/{prj}_{end_date}.tsv", "w") as file_out:
        for key, value in data_out.items():
            file_out.write(f"{key}\t{value}\n")
                    
print("Finished. Updating date.")
update_date(end_date_timestamp)
print("Date updated.")

