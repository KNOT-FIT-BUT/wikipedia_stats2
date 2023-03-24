############################################
# Title:  generate_stats.py                #
# Author: Jakub Štětina                    #
# Date:   12 Mar 2023                      #
############################################

from datetime import datetime
import csv
import logging
import subprocess
import sys
import os
import re

from generate_backlinks import Backlinks
from generate_primary_tags import PrimaryTags
from generate_pageviews import PageViews

csv.field_size_limit(sys.maxsize)

TMP_DIR = "unmerged"
OUT_DIR = "test_out"

if not os.path.exists(TMP_DIR):
    os.mkdir(TMP_DIR)

if not os.path.exists(OUT_DIR):
    os.mkdir(OUT_DIR)


DATA_FILE = "data/last_update"
PROJECT_FILES = "data/project_files"

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
pw = PageViews(start_date, end_date, tmp_dir="pwtemp", output_dir="pwout", output_file="pageviews.tsv")
pw.get_pageviews()

# Move generated pageviews to a temp dir
for prj in PROJECTS.keys():
    prcs = subprocess.run(f"mv pwout/{prj}_pageviews.tsv {TMP_DIR}/{prj}", shell=True)
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


# TODO LOAD PREVIOUS FILES

# Load previous file
projects_files_data = {}
with open(PROJECT_FILES) as prj_data_in:
    projects_files_data = json.loads(prj_data_in.read())


print("Merging")

for prj in dumps_info.keys():
    
    out_data = {}
    
    # Load previous data for project
    prev_file_path = projects_files_data[prj]
    with open(prev_file_path) as prev_file_in:
        for line in prev_file_in:
            in_data = csv.reader(prev_file_in, delimeter="\t")
            for val in in_data:
                try:
                    art_name = val[0]
                    bl_count = val[1]
                    pw_count = val[2]
                    pr_count = val[3]
                except ValueError:
                    sys.stderr.write(f"Error while loading previous data (prj:{prj})\n")
                    exit(1)

                out_data[art_name] = [bl_count, pw_count, pr_count]

    bl_file = f"{TMP_DIR}/{prj}/backlinks.tsv"
    pw_file = f"{TMP_DIR}/{prj}/{prj}_pageviews.tsv"
    pr_file = f"{TMP_DIR}/{prj}/prtags.tsv"

    with open(bl_file) as bl_in, open(pw_file) as pw_in, open(pr_file) as pr_in:
        
        # Files in order of output format
        in_files_list = [bl_in, pw_in, pr_in]i
        pw_idx = 1

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
                
                # If pw_count --> add values
                # Else -> rewrite them
                if idx == pw_idx:
                    out_data[art_name][idx] += count
                    continue
                out_data[art_name][idx] = count

    print("Saving data..")
    with open(f"{OUT_DIR}/{prj}_{end_date}.tsv", "w") as file_out:
        for key, values in out_data.items():
            file_out.write(key)
            for value in values:
                file_out.write(f"\t{value}")
            file_out.write("\n")

print("Finished. Updating date.")
update_date(end_date_timestamp)
print("Date updated.")
