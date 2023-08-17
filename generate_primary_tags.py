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

# Logging level
log_level = logging.INFO
# Logging format -> only display message
logging.basicConfig(level=log_level, format='%(message)s')


class PrimaryTags():
    # Search pattern for a valid primary link
    # (In title tag, must not contain ":" or disambiguation) 
    SEARCH_PATTERN = r"<title>(?!:?\w+:)(.*?)(?<!\(disambiguation\))<\/title>"
    REGEX = re.compile(SEARCH_PATTERN)

    # Temporary data storage
    PT_DATA = dict()

    # Set all class attributes
    def __init__(
        self,
        input_file:str,
        output_file:str):
        self.INPUT_FILE = input_file
        self.OUTPUT_FILE = output_file
        self.__check_input_output()
    
    # Check input, output files
    # Returns error if input/output file does not exist
    def __check_input_output(self):
        if not self.INPUT_FILE.endswith(".xml"):
            logging.warning("WARNING: Input file might not be in correct format (wanted: XML)")

        if not os.path.exists(self.INPUT_FILE):
            logging.error("ERROR: Input file not found")
            exit(1)

        if os.path.exists(self.OUTPUT_FILE):
            logging.error(f"ERROR: Output file '{self.OUTPUT_FILE}' already exists")
            exit(1)
    
    # Saves generated data to a file
    def __save_to_file(self):
        with open(self.OUTPUT_FILE, "w") as out_file:
            for key, value in self.PT_DATA.items():
                out_file.write(f"{key}\t{value}\n")
        self.PT_DATA.clear()
    
    @staticmethod
    def is_primary(a_name:str) -> bool:
        if ",_" in a_name or "(" in a_name:
            return False
        return True

    # Generates primary tags from a given input dump file
    # (primary tags must match the search pattern)
    # (also must not contain "(" or ",_" ... else not a primary link
    # Returns the number of generated values
    def generate_ptags(self):
        val_counter = 0
        logging.info("Generating primary tags..")
        with open(self.INPUT_FILE) as dump_file:
            for line in dump_file:
                match = self.REGEX.match(line.strip())
                if match:
                    a_name = match.group(1).replace(" ", "_")   
                    if self.is_primary(a_name):
                        self.PT_DATA[a_name] = 0
                    else:
                        self.PT_DATA[a_name] = 1
                    val_counter += 1
        self.__save_to_file()
        logging.info("Generation complete.")
        logging.info(f"Generated {val_counter} values.")
        return val_counter

# If run from CLI
if __name__ == "__main__":
    # Argument parsing
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

    # Do not pring anything
    if args.quiet:
        logging.basicConfig(level=logging.CRITICAL+1)
    
    input_file = args.input_file
    output_file = args.output_file

    # Generate primary tags
    logging.info("Starting")
    pt = PrimaryTags(input_file=input_file, output_file=output_file)
    pt.generate_ptags()
    logging.info("Finished.")
