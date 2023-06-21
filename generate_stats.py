############################################
# Title:  generate_stats.py                #
# Author: Jakub Štětina                    #
# Date:   12 Mar 2023                      #
############################################

from datetime import datetime
import subprocess
import logging
import json
import csv
import sys
import os
import re

from generate_backlinks import Backlinks
from generate_primary_tags import PrimaryTags
from generate_pageviews import PageViews

csv.field_size_limit(sys.maxsize)

TMP_DIR = "unmerged"
STATS_DIR = "/mnt/minerva1/nlp-in/wikipedia-statistics/stats"

if not os.path.exists(TMP_DIR):
    os.mkdir(TMP_DIR)

if not os.path.exists(STATS_DIR):
    os.mkdir(STATS_DIR)


DATA_FILE = "/mnt/minerva1/nlp-in/wikipedia-statistics/data/last_update"
PROJECT_FILES = "/mnt/minerva1/nlp-in/wikipedia-statistics/data/project_files"

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

# Load previous file
def load_prev_files_data():
    # Check if files exist
    projects_files_data = {}
    with open(PROJECT_FILES) as prj_data_in:
        projects_files_data = json.loads(prj_data_in.read())
    for key, value in projects_files_data.items():
        if not os.path.exists(value):
            sys.stderr.write(f"Error: ({key}) project file not found\n")
            sys.stderr.write(f"({value})\n")
            exit(1)

    return projects_files_data

print("Cleaning temp dir..")
subprocess.run(f"rm -rf {TMP_DIR}/*", shell=True)
print("Checking previous project files..")
projects_files_data = load_prev_files_data()

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


print("Loading previous data")

for prj in dumps_info.keys():
    
    out_data = {}
    STATS_HEAD = ""

    # Load previous data for project
    prev_file_path = projects_files_data[prj]
    
    with open(prev_file_path) as prev_file_in:
        # Load head
        while (line := prev_file_in.readline()).strip() != "":
            STATS_HEAD += line
                
        # Load data
        print(f"Loading {prj}: {prev_file_path}")
        for line in prev_file_in:
            in_data = [val.strip() for val in line.split("\t")]      
            try:
                art_name = in_data[0]
                bl_count = in_data[1]
                pw_count = in_data[2]
                pr_count = in_data[3]
            except IndexError:
                continue
            out_data[art_name] = [bl_count, pw_count, pr_count]

    bl_file = f"{TMP_DIR}/{prj}/backlinks.tsv"
    pw_file = f"{TMP_DIR}/{prj}/{prj}_pageviews.tsv"
    pr_file = f"{TMP_DIR}/{prj}/prtags.tsv"
    
    print(f"Merging {prj}")
    # Merge data with previous file
    with open(bl_file) as bl_in, open(pw_file) as pw_in, open(pr_file) as pr_in:
        # Files in order of output format
        in_files_list = [bl_in, pw_in, pr_in]
        pw_idx = 1

        for idx, file in enumerate(in_files_list, start=0):
            for line in file:
                values = [val.strip() for val in line.split("\t")]
                art_name = values[0]

                # Get stat value
                try:
                    count = int(values[1])

                # Invalid stat --> ignore
                except ValueError:
                    continue

                if art_name not in out_data:
                    out_data[art_name] = ["NF", "NF", "NF"]
                
                # If pw_count --> add values
                # Else -> rewrite them
                if idx == pw_idx:
                    if out_data[art_name][idx] == "NF":
                        out_data[art_name][idx] = count
                    else:
                        prev_pw_count = int(out_data[art_name][idx])
                        out_data[art_name][idx] = prev_pw_count+count
                    continue
                    print("len of data_out before merge:", len(out_data))

                out_data[art_name][idx] = count

    print("Saving data..")
    print("len", len(out_data))
    with open(prev_file_path, "w") as file_out:
        # Write head
        file_out.write(STATS_HEAD)

        # Write data
        for key, values in out_data.items():
            file_out.write(key)
            for value in values:
                file_out.write(f"\t{value}")
            file_out.write("\n")

print("Finished. Updating date.")
update_date(end_date_timestamp)
print("Date updated.")

