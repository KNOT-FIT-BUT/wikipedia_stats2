#! /bin/python3

####################################################
# Title:  backlinks_primary_stats.py               #
# Author: Jakub Štětina <xsteti05@stud.fit.vut.cz> #
# Date:   15 Jul 2023                              #
####################################################

from kb_metrics.ws_file_locking.locking import FileLock
from datetime import datetime
import subprocess
import tempfile
import logging
import shutil
import json
import sys
import os
import re

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from generate_primary_tags import PrimaryTags
from generate_backlinks import Backlinks
from config import *

TMP_DIR = tempfile.mkdtemp(prefix="ws_bps_")

if not os.path.exists(STATS_DIR):
    print("Error: Stats directory does not exits, exiting.")
    exit(1)

FILE_NAME_REG = re.compile(PAGES_ARTICLES_DUMP_REG)

print("Checking previous project files..")

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

    latest_dump_timestamp = int(datetime(year, month, day).strftime("%s"))
    
    print(f"Latest dump ({key}): {latest_dump}")
    dumps_info[key] = {
        "path":f"{check_dir}/{latest_dump}", 
        "latest_timestamp": latest_dump_timestamp
        }

if len(dumps_info) == 0:
    print("Everything up to date.")
    exit(0)

# Generate primary tags and backlinks
for prj, dump_info in dumps_info.items():
    
    dump_path = dump_info["path"]
    print(f"Current dump ({prj}):", dump_path)

    bl = Backlinks(dump_path, f"{TMP_DIR}/{prj}/backlinks.tsv")
    bl.generate_backlinks()
    del bl

    print("--------------------")


print("Loading previous data")

for prj in dumps_info.keys():
    
    out_data = {}
    STATS_HEAD = ""

    # Load previous data for project
    prev_file_path = f"{STATS_DIR.rstrip('/')}/bps/{prj}_bps.tsv"
    
    try:
        with FileLock(
            file_path=prev_file_path,
            mode="r+",
            timeout_sec=FILE_LOCK_TIMEOUT) as prev_file_in: 
                   
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
                    pr_count = in_data[2]

                except IndexError:
                    continue
                out_data[art_name] = [bl_count, pr_count]
    except TimeoutError:
        print(LOCKED_FILE_MESSAGE)
        exit(1)

    bl_file = f"{TMP_DIR}/{prj}/backlinks.tsv"
    
    new_file_path = os.path.join(
        STATS_DIR, 
        f"bps/{datetime.now().strftime(FILE_DATE_FORMAT)}_{prj}_bps.tsv"
    )
    
    print(f"Merging {prj}..")
    # Merge data with previous file
    try:
        with FileLock(
            file_path=prev_file_path,
            mode="w",
            timeout_sec=FILE_LOCK_TIMEOUT) as file_out, open(bl_file) as bl_in: 

                # Write head
                file_out.write(STATS_HEAD)
                if not STATS_HEAD.endswith("\n\n"):
                    file_out.write("\n")

                for line in bl_in:
                    values = [val.strip() for val in line.split("\t")]
                    art_name = values[0]

                    # Default value if not found
                    ps_cout = bl_count = "NF"

                    try:
                        ps_count = int(PrimaryTags.is_primary(art_name))
                        bl_count = int(values[1])

                    # Invalid stat --> ignore
                    except ValueError:
                        pass
            
                    file_out.write(f"{art_name}\t{bl_count}\t{ps_count}\n") 
    except TimeoutError:
        print(LOCKED_FILE_MESSAGE)
        exit(1)

shutil.rmtree(TMP_DIR)
subprocess.run(f"rm -rf {TMP_DIR}", shell=True)
print("Done.")


