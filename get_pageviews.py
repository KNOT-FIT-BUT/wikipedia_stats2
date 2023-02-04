############################################
# Title:  get_pageviews.py                 #
# Author: Jakub Štětina                    #
# Date:   5 Feb 2023                       #
############################################

import requests
import argparse
import time

# API VARs
API_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
headers = {
    ## Non-python User-Agent needed to prevent blocking from WIKIMEDIA API
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1) AppleWebKit/534.1.2 (KHTML, like Gecko) Chrome/38.0.845.0 Safari/534.1.2'
}

# ARGUMENTS PARSER

# VALID VALUES 
PROJECT_VALUES = ['en.wikipedia.org', 'cs.wikipedia.org', 'sk.wikipedia.org', 'www.mediawiki.org']
ACCESS_VALUES = ["all-access", "desktop", "mobile-app", "mobile-web"]
AGENT_VALUES = ["all-agents", "user", "spider", "automated"]
GRANULARITY_VALUES = ["daily", "monthly"]
TIMESTAMP_REGX = "^\d{8}(?:\d{2})?$"

parser = argparse.ArgumentParser()

# ARGUMENTS
## Article name
parser.add_argument(
    "-n","--name", 
    type = str, 
    required=True,
    help = "Valid article name",
)

## Project name 
parser.add_argument(
    "-p","--project", 
    type = str, 
    choices=PROJECT_VALUES,
    help = "Project name (en.wikipedia, cs.wikipedia, ...)",
    default = "en.wikipedia.org"
)

## Access 
parser.add_argument(
    "--access", 
    type = str, 
    choices=ACCESS_VALUES,
    help = "Access type (all-access, desktop, mobile-app, mobile-web)",
    default = "all-access"
)

## Agent
parser.add_argument(
    "--agent", 
    type = str, 
    choices=AGENT_VALUES,
    help = "Access agent (all-agents, user, spider, automated)",
    default = "all-agents"
)

## Granulatity
parser.add_argument(
    "--granularity", 
    type = str, 
    choices=GRANULARITY_VALUES,
    help = "Entries granularity (daily, monthly)",
    default = "daily"
)

## Start timestamp
parser.add_argument(
    "-s","--start", 
    type = str, 
    required=True,
    help = "Start timestamp: YYYYMMDD(HH)",
)

## End timestamp
parser.add_argument(
    "-e","--end", 
    type = str, 
    help = "End timestamp: YYYYMMDD(HH)",
    default = time.strftime('%Y%m%d%H', time.localtime(time.time()))
)

args = parser.parse_args()

