####################################################
# Title:  generate_primary_tags.py                 #
# Author: Jakub Štětina <xsteti05@stud.fit.vut.cz> #
# Date:   7 Feb 2023                               #
####################################################

import argparse
import logging
import sys
import os
import re
import time

log_level = logging.INFO
logging.basicConfig(level=log_level, format='%(message)s')


class PrimaryTags():
    SEARCH_PATTERN = r"<title>(?!:?\w+:)(.*?)(?<!\(disambiguation\))<\/title>"
    REGEX = re.compile(SEARCH_PATTERN)

    PT_DATA = dict()

    def __init__(
        self,
        input_file:str,
        output_file:str):
        self.INPUT_FILE = input_file
        self.OUTPUT_FILE = output_file
        self.__check_input_output()
    
    def __check_input_output(self):
        if not self.INPUT_FILE.endswith(".xml"):
            logging.warning("WARNING: Input file might not be in correct format (wanted: XML)")

        if not os.path.exists(self.INPUT_FILE):
            logging.error("ERROR: Input file not found")
            exit(1)

        if os.path.exists(self.OUTPUT_FILE):
            logging.error(f"ERROR: Output file '{self.OUTPUT_FILE}' already exists")
            exit(1)
    
    def __save_to_file(self):
        logging.info("Saving..")
        with open(self.OUTPUT_FILE, "w") as out_file:
            for key, value in self.PT_DATA.items():
                out_file.write(f"{key}\t{value}\n")
        logging.info("Saved.")

    def generate_ptags(self):
        val_counter = 0
        logging.info("Generating primary tags..")
        with open(self.INPUT_FILE) as dump_file:
            for line in dump_file:
                match = self.REGEX.match(line.strip())
                if match:
                    a_name = match.group(1).replace(" ", "_")   
                    if ",_" in a_name or "(" in a_name:
                        self.PT_DATA[a_name] = 0
                    else:
                        self.PT_DATA[a_name] = 1
                    val_counter += 1
        logging.info("Generation complete.")
        self.__save_to_file()
        logging.info(f"Generated {val_counter} values.")
        return val_counter
 
if __name__ == "__main__":
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

    io_parser.add_argument(
        "-q","--quiet", 
        required=False,
        dest="quiet",
        help="Disable output"
    )

    args = io_parser.parse_args()

    if args.quiet:
        logging.basicConfig(level=logging.CRITICAL+1)
    
    input_file = args.input_file
    output_file = args.output_file

    logging.info("Starting")
    pt = PrimaryTags(input_file=input_file, output_file=output_file)
    pt.generate_ptags()
    logging.info("Finished.")