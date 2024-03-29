####################################################
# Title:  generate_pageviews.py                    #
# Author: Jakub Štětina <xsteti05@stud.fit.vut.cz> #
# Date:   18 Feb 2023                              #
####################################################

from datetime import datetime
import pandas as pd
import subprocess
import requests
import argparse
import logging
import time
import re
import sys
import os

# Log level -> only info
log_level = logging.INFO
# Log format -> display only message, no metadata
logging.basicConfig(level=log_level, format='%(message)s')

class PageViews():
    # Base url for wm dumps
    WM_DUMP_BASE_URL = "https://dumps.wikimedia.org/other/pageviews"

    REGEX = "(?!.*:)(.*?) (\d+) 0$"

    # Default tracked projects
    PROJECTS = ["en", "cs", "sk"]

    # Dirs for temp/out files
    TMP_DIR = "pwtmp"
    OUTPUT_DIR = "pageviews"
    OUTPUT_FILE = ""

    # Num of tries, if download fails
    DWNLD_TRIES = 3

    # Correct date format for input 
    CORRECT_DATE_FORMAT = r"^\d{4}-\d{2}-\d{2}$"

    # No dumps before this date
    START_DATE_MINIMUM = datetime(2015, 5, 1)

    # Date range, specifies user
    START_DATE = ""
    END_DATE = ""

    # Set class attributes and perform neccessary checks and cleanups
    def __init__(
        self,
        start_date:str,
        end_date:str, 
        tmp_dir:str=TMP_DIR,
        output_dir:str=OUTPUT_DIR,
        output_file:str=OUTPUT_FILE,
        projects:list=PROJECTS
    ):
        self.START_DATE = start_date
        self.END_DATE = end_date
        self.__check_date_range()


        self.TMP_DIR = tmp_dir
        self.OUTPUT_DIR = output_dir
        self.OUTPUT_FILE = output_file
        self.PROJECTS = projects
        self.__tmp_cleanup()
        self.__check_dirs()

        self.REGEX_DICT =  {prj:f"^{prj} {self.REGEX}" for prj in projects}
        
        self.__get_dwnld_data()
        self.__check_if_available()
    
    # Checks for possible errors in inputed dates, exits on incorrect date formats or future dates
    def __check_date_range(self):
        if  (not re.match(self.CORRECT_DATE_FORMAT, self.START_DATE) or
            not re.match(self.CORRECT_DATE_FORMAT, self.END_DATE)):

            logging.error("ERROR: Date format incorrect, expected YYYY-MM-DD")
            exit(1)

        try:
            start_year = int(self.START_DATE.split("-")[0])
            start_month = int(self.START_DATE.split("-")[1])
            start_day = int(self.START_DATE.split("-")[2])

            end_year = int(self.END_DATE.split("-")[0])
            end_month = int(self.END_DATE.split("-")[1])
            end_day = int(self.END_DATE.split("-")[2])
        except IndexError:
            logging.error("ERROR: Date format incorrect, expected YYYY-MM-DD")
            exit(1)

        except ValueError:
            logging.error("ERROR: Unwanted characters in date, expected, YYYY-MM-DD")
            exit(1)
        try:
            start_date = datetime(start_year, start_month, start_day)
            end_date = datetime(end_year, end_month, end_day)
        except ValueError:
            logging.error("ERROR: Date value out of range")
            exit(1)

        if start_date > end_date:
            logging.error("ERROR: Date range error")
            exit(1)

        if start_date < self.START_DATE_MINIMUM:
            logging.error("ERROR: Start date too low, minimum: 2015-05-01")
            exit(1)
        
        if end_date > datetime.now():
            logging.error("ERROR: End date is in the future")
            exit(1)

        try:
             self.DATE_RANGE = pd.date_range(self.START_DATE, self.END_DATE)
        except Exception as e:
            logging.error(f"ERROR: Date range error {e}")
            exit(1)
    
    # Checks if all files for the given date range are available
    # Exits with error if not
    def __check_if_available(self):
        dwnld_data_keys = list(self.DWNLD_DATA.keys())
        first_year_month = dwnld_data_keys[0]

        # Special case for start of pageviews dumps
        if "20150501-000000" in self.DWNLD_DATA[first_year_month][0]:
            self.DWNLD_DATA[first_year_month] = self.DWNLD_DATA[first_year_month][1:]
        
        first_file_name = self.DWNLD_DATA[first_year_month][0]

        last_year_month = dwnld_data_keys[-1]
        last_file_name = self.DWNLD_DATA[last_year_month][-1]

        req_url_first = f"{self.WM_DUMP_BASE_URL}/{first_year_month}/{first_file_name}"
        req_url_last = f"{self.WM_DUMP_BASE_URL}/{last_year_month}/{last_file_name}"

        resp_first = requests.head(req_url_first).status_code
        resp_last = requests.head(req_url_last).status_code

        if resp_first != 200 or resp_last != 200:
            sys.stderr.write("ERROR: The whole date range is not yet available on the server\n")
            exit(1)

    # Deletes files in a temp dir
    # Necessary before/after the script
    def __tmp_cleanup(self):
        subprocess.run(f"rm -rf {self.TMP_DIR}/*", shell=True)


    # Checks if out/tmp dirs exist
    # Creates them if not
    def __check_dirs(self):

        os.makedirs(f"{self.TMP_DIR}/pw", exist_ok=True)
        os.makedirs(f"{self.TMP_DIR}/prcs", exist_ok=True)
 
        for prj in self.PROJECTS:
            os.makedirs(f"{self.TMP_DIR}/pw/{prj}",exist_ok=True)
        
        if not os.path.exists(self.OUTPUT_DIR):
            logging.warning("Output dir does not exist, creating..")
            os.mkdir(self.OUTPUT_DIR)

    # Generates all download links for the specified date range
    def __get_dwnld_data(self):
        self.DWNLD_DATA = {}
        
        for value in self.DATE_RANGE:
            year = value.year
            month = value.month
            day = value.day
            year_month = f"{year}/{year}-{str(month).zfill(2)}"
            
            if not year_month in self.DWNLD_DATA:
                self.DWNLD_DATA[year_month] = []

            for hour in range(0, 23+1):
                month = str(month).zfill(2)
                day = str(day).zfill(2)
                hour = str(hour).zfill(2)

                file_name = f"pageviews-{year}{month}{day}-{hour}0000.gz"
                self.DWNLD_DATA[year_month].append(file_name)
    
    # Unzips downloaded pageviews data for one day
    # Merges them into one file
    # Exits with error on fail
    def __prcs_files(self, out_file_name:str):
        logging.info("Unzipping files")
        prcs = subprocess.run(f"gunzip {self.TMP_DIR}/prcs/*.gz --force", shell=True)
        if prcs.returncode != 0:
            sys.stderr.write("FAILED TO UNZIP FILES")
            exit(1)
        
        logging.info("Unzipped.")

        files =  sorted([file for file in os.listdir(f"{self.TMP_DIR}/prcs")])

        data = {}
        for file_name in files:
            logging.info(f"Extracting data: {file_name}")
            with open(f"{self.TMP_DIR}/prcs/{file_name}") as file_in:
                for line in file_in:
                    line = line.strip()
                    for prj, reg in self.REGEX_DICT.items():
                        if not prj in data:
                            data[prj] = {}
                        match = re.match(reg, line)
                        if match:
                            article_name = match.group(1)
                            page_count = int(match.group(2))
                            if not article_name in data[prj]:
                                data[prj][article_name] = 0
                            data[prj][article_name] += page_count
        logging.info("Saving..")
        for prj, values in data.items():
            with open(f"{self.TMP_DIR}/pw/{prj}/{prj}_{out_file_name}", "w") as file_out:
                for article_name, pw_count in values.items():
                    file_out.write(f"{article_name}\t{pw_count}\n")
        logging.debug("Removing tmp files")
        subprocess.run(f"rm -rf {self.TMP_DIR}/prcs/*", shell=True)
    
    # Merges all daily files into one file
    def __final_merge(self):

        for prj in self.PROJECTS:
            prj_dir = f"{self.TMP_DIR}/pw/{prj}"
            files = os.listdir(prj_dir)

            data_out = {}
            for file_name in files:
                logging.info(f"Processing file: {file_name}")

                with open(f"{prj_dir}/{file_name}") as file_in:
                    for line in file_in:
                        line_data = line.strip().split("\t")
                        article_name = line_data[0]

                        # Empty line
                        if(len(line_data) == 0): 
                            continue

                        try:
                            pw_value = int(line_data[-1])
                        except ValueError:
                            sys.stderr.write("Error: value not number")
                            exit(1)
                        except IndexError:
                            sys.stderr.write(f"Unable to load this line: {line.strip()}")
                            continue

                        if not article_name in data_out:
                            data_out[article_name] = 0 
                        data_out[article_name] += pw_value

            logging.info(f"Saving project {prj}...")
            out_file_name = f"{prj}_{self.START_DATE}_{self.END_DATE}.tsv"
            if self.OUTPUT_FILE:
                out_file_name = f"{prj}_{self.OUTPUT_FILE}"
            with open(f"{self.OUTPUT_DIR}/{out_file_name}", "w") as file_out:
                for key, value in data_out.items():
                    file_out.write(f"{key}\t{value}\n")
      
        self.__tmp_cleanup()
    
    # Download hourly data from the specified date range
    # Merge them into daily data
    # Finally merge into one file
    def __dwnld_files(self):
        skipped_files = []
        for year_month in self.DWNLD_DATA:
            for i, file_name in enumerate(self.DWNLD_DATA[year_month]):  
                
                dwnld_link =  f"{self.WM_DUMP_BASE_URL}/{year_month}/{file_name}"
                logging.info(f"Num: {i+1}, Downloading {file_name}")

                for try_n in range(self.DWNLD_TRIES):
                    prcs = subprocess.run(
                        f"wget \"{dwnld_link}\" -O {self.TMP_DIR}/prcs/{file_name} --quiet",
                        shell=True,
                        executable="/bin/bash")
                    
                    if prcs.returncode == 0:
                        break
                    else:
                        logging.info(f"Download of {file_name} unsuccessful, trying again..")
                        time.sleep(10)
                    if prcs.returncode != 0 and try_n == self.DWNLD_TRIES - 1:
                        skipped_files.append(dwnld_link)
                        logging.warning(f"Warning: Skipped file: {dwnld_link}")
                        os.remove(f"{self.TMP_DIR}/prcs/{file_name}")
                

                if (i+1) % 24 == 0:
                    logging.info("Processing files..")
                    out_file_name = "-".join(file_name.split("-")[:2]) + ".tsv"
                    self.__prcs_files(out_file_name) 
        logging.info("Download finished.")
        logging.warning(f"Skipped files: {skipped_files}")
        
        logging.info("Merging files")
        self.__final_merge()
        logging.info("Merging finished.")

    # Runs the dwnld_files method
    def get_pageviews(self):
        self.__dwnld_files()

# If script run from CLI
if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        "-s", "--start", 
        type=str,
        required=True,
        action="store", 
        dest="start_date",
        help="Start date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "-e", "--end", 
        type=str,
        required=True,
        action="store", 
        dest="end_date",
        help="End date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "-o", "--output", 
        type=str,
        required=False,
        action="store", 
        dest="out_dir",
        help="Output dir"
    )

    parser.add_argument(
        "--tmp", 
        type=str,
        required=False,
        action="store", 
        dest="tmp_dir",
        help="Temp dir for (for storing unprocessed data)"
    )

    parser.add_argument(
        "--projects", 
        required=False,
        nargs="+",
        action="store",
        dest="prj_list",
        help="List of projects",
    )

    parser.add_argument(
        "--quiet", 
        required=False,
        action="store",
        dest="quiet",
        help="Disable output",
    )
    args = parser.parse_args()


    start_date = args.start_date
    end_date = args.end_date
    
    # Deafults
    out_dir = "pageviews"
    tmp_dir = "pwtmp"
    prj_list = ["en", "cs", "sk"]

    if args.out_dir:
        out_dir = args.out_dir
    if args.tmp_dir:
        tmp_dir = args.tmp_dir
    if args.prj_list:
        prj_list = args.prj_list
    
    # Do not print anything
    if args.quiet:
        logging.basicConfig(level=logging.CRITICAL+1)

    # Generate pageviews
    pw = PageViews(
        start_date=start_date, 
        end_date=end_date,
        tmp_dir=tmp_dir,
        output_dir=out_dir,
        projects=prj_list
    )
    pw.get_pageviews()
   
