#! /bin/python3

####################################################
# Title:  backlinks_primary_stats.py               #
# Author: Jakub Štětina <xsteti05@stud.fit.vut.cz> #
# Date:   15 Jul 2023                              #
####################################################

from datetime import datetime
import subprocess
import logging
import json
import csv
import sys
import os
import re

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from generate_backlinks import Backlinks
from generate_primary_tags import PrimaryTags
from generate_pageviews import PageViews

csv.field_size_limit(sys.maxsize)

TMP_DIR = "BPS_TEMP"
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
        print(f"Loading {prev_file_path}")
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
    pr_file = f"{TMP_DIR}/{prj}/prtags.tsv"
    
    print(f"Merging {prj}")
    # Merge data with previous file
    with open(bl_file) as bl_in, open(pr_file) as pr_in:
        # Files in order of output format
        in_files_list = [bl_in, None, pr_in]

        for idx, file in enumerate(in_files_list, start=0):
            
            if file is None:
                continue

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
                
                out_data[art_name][idx] = count

    print("Saving data..")
    with open(prev_file_path, "w") as file_out:
        # Write head
        file_out.write(STATS_HEAD)
        if not STATS_HEAD.endswith("\n\n"):
            file_out.write("\n")

        # Write data
        for key, values in out_data.items():
            file_out.write(key)
            for value in values:
                file_out.write(f"\t{value}")
            file_out.write("\n")

print("Done.")


