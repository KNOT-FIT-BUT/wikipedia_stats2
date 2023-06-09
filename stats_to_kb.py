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
        dest="input_file",
        help = "Input file",
    )

    io_parser.add_argument(
        "-s","--stats", 
        type = str, 
        required=True,
        action="store",
        dest="stats_file",
        help = "File with statisticss",
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

    input_file = args.input_file
    output_file = args.output_file
    stats_file = args.stats_file

    # Check if input_file, stats_file exists
    if not os.path.exists(os.path.abspath(input_file)):
        sys.stderr.write("Error: Input KB does not exist\n")
        exit(1)

    if not os.path.exists(os.path.abspath(stats_file)):
        sys.stderr.write("Error: Stats file does not exist\n")
        exit(1)

    # If no output file given 
    # Output file will be in the following format:
    # 'input_file+stats.tsv'
    if output_file is None:
        output_file = ""

    # Knowledge base class 
    kb = KnowledgeBase(path_to_kb=input_file)
    
    # Insert stats to KB
    if not kb.insert_stats(stats_file=stats_file, save_changes=False):
        print("Warning: some or all stats already in present in KB")
        print("Stat inserting skipped...")

    kb.save_changes(output_file=output_file)


