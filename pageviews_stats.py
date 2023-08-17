#! /bin/python3

####################################################
# Title:  pageviews_stats.py                       #
# Author: Jakub Štětina <xsteti05@stud.fit.vut.cz> #
# Date:   15 Jul 2023                              #
####################################################

from datetime import datetime, timedelta
import subprocess
import tempfile
import logging
import shutil
import json
import sys
import os
import re

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from generate_pageviews import PageViews
from config import *

TMP_DIR = tempfile.mkdtemp(prefix="ws_pw_")
os.mkdir(f"{TMP_DIR}/pwtemp")
os.mkdir(f"{TMP_DIR}/pwout")

if not os.path.exists(STATS_DIR):
    print("Error: Stats directory does not exits, exiting.")
    exit(1)


FILE_NAME_REG = re.compile(PAGES_ARTICLES_DUMP_REG)

def load_prev_date() -> datetime:
    with open(DATA_FILE) as file_in:
        line = file_in.readline()
        try:
            year, month, day = [int(val) for val in line.split("-")]
            last_update = datetime(year, month, day)

        except FileNotFoundError:
            sys.stderr.write("File not found\n")
            exit(1)    
        except:
            sys.stderr.write("Conversion error - ERROR in last_update file\n")
            exit(1)

    return last_update

def update_date(new_date:datetime) -> None:
    with open(DATA_FILE, "w") as file_out:
        file_out.write(new_date.strftime(DATE_FORMAT))


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

print("Checking previous project files..")
projects_files_data = load_prev_files_data()

# Get the latest dump for each project
dumps_info = {}
for key, value in PROJECTS.items():
    check_dir = DUMP_DIR.format(value)

    if not os.path.exists(check_dir):
        sys.stderr.write(f"Error: dump dir does not exist ({check_dir})\n")
        exit(1)

    os.mkdir(f"{TMP_DIR}/{key}")
    
    files = os.listdir(check_dir)

    dumps = [file for file in files if FILE_NAME_REG.match(file)]
    latest_dump = sorted(dumps)[-1]

    date = latest_dump.split("-")[1]
    year = int(date[:4])
    month = int(date[4:6])
    day = int(date[6:])

    latest_dump_timestamp = datetime(year, month, day)
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
start_date = load_prev_date()
end_date = min([item["latest_timestamp"] for item in list(dumps_info.values())])-timedelta(days=1)

start_date_str = start_date.strftime(DATE_FORMAT)
end_date_str = end_date.strftime(DATE_FORMAT)

print("Date range:", start_date_str, end_date_str)
print("Generating pageviews")                   
# Generate pageviews

pw = PageViews(start_date_str, end_date_str, tmp_dir=f"{TMP_DIR}/pwtemp", output_dir=f"{TMP_DIR}/pwout", output_file="pageviews.tsv")
pw.get_pageviews()

# Move generated pageviews to a temp dir
for prj in PROJECTS.keys():
    prcs = subprocess.run(f"mv {TMP_DIR}/pwout/{prj}_pageviews.tsv {TMP_DIR}/{prj}", shell=True)
    if prcs.returncode != 0:
        sys.stderr.write("Error while moving pageviews data\n")
        exit(1)

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
    with open(pw_file) as pw_in:
        pw_idx = 1
        for line in pw_in:
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
            if out_data[art_name][pw_idx] == "NF":
                out_data[art_name][pw_idx] = count
            else:
                prev_pw_count = int(out_data[art_name][pw_idx])
                out_data[art_name][pw_idx] = prev_pw_count+count

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

print("Finished. Updating date.")
update_date(end_date+timedelta(days=1))
print("Date updated.")

shutil.rmtree(TMP_DIR)
print("Done.")

