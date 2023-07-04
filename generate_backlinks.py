####################################################
# Title:  generate_backlinks.py                    #
# Author: Jakub Štětina <xsteti05@stud.fit.vut.cz> #
# Date:   7 Feb 2023                               #
####################################################

import argparse
import logging
import os
import re

# Logging level
log_level = logging.INFO
# Logging format -> only display message
logging.basicConfig(level=log_level, format='%(message)s')

class Backlinks():
    # Pattern for a valid wiki backlink
    SEARCH_PATTERN = r"\[\[(?!:?\w+:)(?!#)(?!.*\(disambiguation\))(.+?)(?:(?:\||#).*?)?\]\]"
    REGEX = re.compile(SEARCH_PATTERN)

    # Temporary data storage
    BL_DATA = dict()
    
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
            for key, value in self.BL_DATA.items():
                out_file.write(f"{key}\t{value}\n")
        self.BL_DATA.clear()

    # Goes through file and removes redirect pages 
    # from BL_DATA and adds the backlinks to the actual page
    def remove_redirects(self):
        REDIRECT_TO_REG = r"<redirect(?:.*)title=\"(.*)\"(?:.*)/>"
        REDIRECT_FROM_REG = r"<title>(.*)</title>"

        with open(self.INPUT_FILE) as dump_file:
            in_page = False
            page_content = ""
            redirects_num = 0

            for line in dump_file:
                
                # Get page tag content
                if line.strip() == "<page>":
                    in_page = True
                    continue    
                
                if line.strip() == "</page>":
                    in_page = False
                    redirect_to = re.search(REDIRECT_TO_REG, page_content, re.MULTILINE)
                    redirect_from = re.search(REDIRECT_FROM_REG, page_content, re.MULTILINE)

                    if redirect_to and redirect_from:
                        redirect_to = redirect_to.group(1).replace(" ", "_")
                        redirect_from = redirect_from.group(1).replace(" ", "_")

                        if self.BL_DATA.get(redirect_from):
                            if not self.BL_DATA.get(redirect_to):
                                self.BL_DATA[redirect_to] = 0
                            self.BL_DATA[redirect_to] += self.BL_DATA.pop(redirect_from)
                            redirects_num += 1

                    page_content = ""
                    continue

                if in_page:
                    page_content += line
        return redirects_num
  
    # Generates backlinks from a given input dump file
    # (backlinks must match the search pattern)
    # Returns the number of generated values
    def generate_backlinks(self):
        val_counter = 0
        logging.info("Generating backlinks..")
        with open(self.INPUT_FILE) as dump_file:
            for line in dump_file:
                match = self.REGEX.match(line)
                if match:
                    a_name = match.group(1).replace(" ", "_")
                    if a_name not in self.BL_DATA:
                        self.BL_DATA[a_name] = 1
                    else:
                        self.BL_DATA[a_name] += 1
                    val_counter += 1
        
        logging.info("Removing redirects..")
        redirects_num = self.remove_redirects()
        logging.info(f"Removed {redirects_num} redirects.")
        self.__save_to_file()
        logging.info("Generation complete.")
        logging.info(f"Generated {val_counter} values.")
        return val_counter

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

    # Do not print anything
    if args.quiet:
        logging.basicConfig(level=logging.CRITICAL+1)

    args = io_parser.parse_args()

    input_file = args.input_file
    output_file = args.output_file

    # Generate backlinks
    logging.info("Starting")
    bl = Backlinks(input_file=input_file, output_file=output_file)
    bl.generate_backlinks()
    logging.info("Finished.")
