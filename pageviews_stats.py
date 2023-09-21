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

from kb_metrics.ws_file_locking.locking import FileLock
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
    try:
        with FileLock(file_path=DATA_FILE,mode="r+", timeout_sec=FILE_LOCK_TIMEOUT) as file_in: 
            
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
    except OSError:
        print(LOCKED_FILE_MESSAGE)
        exit(1)
    
    return last_update

def update_date(new_date:datetime) -> None:
    with open(DATA_FILE, "w") as file_out:
        file_out.write(new_date.strftime(DATE_FORMAT))

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
    prev_file_path = f"{STATS_DIR.rstrip('/')}/pageviews/{prj}_pageviews.tsv"

    try:
        with FileLock(prev_file_path, timeout_sec=FILE_LOCK_TIMEOUT) as prev_file_in:
            # Load head
            while (line := prev_file_in.readline()).strip() != "":
                STATS_HEAD += line
                    
            # Load data
            print(f"Loading {prj}: {prev_file_path}")
            for line in prev_file_in:
                in_data = [val.strip() for val in line.split("\t")]      
                try:
                    art_name = in_data[0]
                    pw_count = in_data[1]

                except IndexError:
                    continue
                
                out_data[art_name] = pw_count
    except TimeoutError:
        print(LOCKED_FILE_MESSAGE)
        exit(1)
    
    
    pw_file = f"{TMP_DIR}/{prj}/{prj}_pageviews.tsv"
    
    print(f"Merging {prj}")
    # Merge data with previous file
    try:
        with FileLock(pw_file, timeout_sec=FILE_LOCK_TIMEOUT) as pw_in:
            for line in pw_in:
                values = [val.strip() for val in line.split("\t")]
                art_name = values[0]

                # Get stat value
                try:
                    count = int(values[1])

                # Invalid stat --> ignore
                except ValueError:
                    continue
                
                # If pw_count --> add values
                # Else -> rewrite them
                if out_data[art_name] == "NF":
                    out_data[art_name] = count
                else:
                    prev_pw_count = int(out_data[art_name])
                    out_data[art_name] = prev_pw_count+count
    except TimeoutError:
        print(LOCKED_FILE_MESSAGE)
        exit(1)
        
    print("Saving data..")
    try:
        with FileLock(prev_file_path, mode="w", timeout_sec=FILE_LOCK_TIMEOUT) as file_out:
            # Write head
            file_out.write(STATS_HEAD)
            if not STATS_HEAD.endswith("\n\n"):
                file_out.write("\n")

            # Write data
            for article, value in out_data.items():
                file_out.write(f"{article}\t{value}\n")
    except TimeoutError:
        print(LOCKED_FILE_MESSAGE)
        exit(1)

print("Finished. Updating date.")
update_date(end_date+timedelta(days=1))
print("Date updated.")

shutil.rmtree(TMP_DIR)
subprocess.run(f"rm -rf {TMP_DIR}", shell=True)
print("Done.")

