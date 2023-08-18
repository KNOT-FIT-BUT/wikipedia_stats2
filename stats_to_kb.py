#! /usr/bin/python3

import os
import sys
import argparse
from kb_metrics.metrics_knowledge_base import KnowledgeBase

if __name__ == "__main__":

    # Argument parsing
    io_parser = argparse.ArgumentParser()

    io_parser.add_argument(
        "-i","--input", 
        type = str, 
        required=True,
        action="store",
        dest="input_kb",
        help = "Input file",
    )

    io_parser.add_argument(
        "-pw","--pageviews", 
        type = str, 
        required=True,
        action="store",
        dest="pw_file",
        help = "Stats file with pageviews",
    )

    io_parser.add_argument(
        "-bps","--backlinks-primary-sense", 
        type = str, 
        required=True,
        action="store",
        dest="bps_file",
        help = "Stats file with backlinks and primary tags (bps)",
    )

    io_parser.add_argument(
        "-o","--output", 
        type = str, 
        required=False,
        action="store",
        dest="output_file",
        help = "Output file",
    )

    args = io_parser.parse_args()

    input_kb = args.input_kb
    output_file = args.output_file
    pw_file = args.pw_file
    bps_file = args.bps_file

    # Check if input_kb, stats_file exists
    if not os.path.exists(input_kb):
        sys.stderr.write("Error: Input KB does not exist\n")
        exit(1)

    if not os.path.exists(pw_file):
        sys.stderr.write("Error: Pageviews file does not exist\n")
        exit(1)
    
    if not os.path.exists(bps_file):
        sys.stderr.write("Error: Stats file does not exist\n")
        exit(1)


    # If no output file given 
    # Output file will be in the following format:
    # 'input_kb+stats.tsv'
    if output_file is None:
        output_file = ""

    # Knowledge base class 
    kb = KnowledgeBase(path_to_kb=input_kb)
    
    # Insert stats to KB
    if  kb.insert_stats(pw_path=pw_file, bps_path=bps_file, save_changes=False):
        kb.save_changes(output_file=output_file)


