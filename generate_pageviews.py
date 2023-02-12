############################################
# Title:  generate_pageviews.py            #
# Author: Jakub Štětina                    #
# Date:   9 Feb 2023                       #
############################################

import argparse
import asyncio
import aiohttp
import socket
import time
import json
import sys
import re
import os

class PageViews():
    API_BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    API_REQ_LIMIT = 100

    headers = headers = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
    }
    results = []
    REQ_URLS = []

    DELAY = 0.5
    RETRY_COUNT = 3
    TIMEOUT = 50
    
    # VALID VALUES 
    PROJECT_VALUES = ['en.wikipedia.org', 'cs.wikipedia.org', 'sk.wikipedia.org']
    ACCESS_VALUES = ["all-access", "desktop", "mobile-app", "mobile-web"]
    AGENT_VALUES = ["all-agents", "user", "spider", "automated"]
    GRANULARITY_VALUES = ["daily", "monthly"]
    TIMESTAMP_REGX = "^\d{8}(?:\d{2})?$"
    LOWEST_TIMESTAMP = "20150701" # No data before this date

    def __init__(self, check_values:bool=True, rate_limit=API_REQ_LIMIT):
        self.CHECK_VALUES = check_values
        self.API_REQ_LIMIT = rate_limit

    def __check_input_file(self, input_file:str):
        if not os.path.exists(input_file):
            sys.stderr.write("Cannot find input file\n")
            exit(1)
        

    ## Checks api request values
    ## (Can be turned off by CHECK_VALUES = False)
    def __check_values(self,values):
        if values["project_name"] not in self.PROJECT_VALUES:
            sys.stderr.write("ERROR: Invalid project name\n")
            raise Exception("Invalid Project name")
        
        if values["access_type"] not in self.ACCESS_VALUES:
            sys.stderr.write("ERROR: Invalid access type\n")
            raise Exception("Invaid access type")
        
        if values["agent_type"] not in self.AGENT_VALUES:
            sys.stderr.write("ERROR: Invalid agent type\n")
            raise Exception("Invalid agent type")
        
        if values["granularity"] not in self.GRANULARITY_VALUES:
            sys.stderr.write("ERROR: Invalid granularity value\n")
            raise Exception("Invalid granularity value")
        
        if (not re.match(self.TIMESTAMP_REGX, values["start_time"]) or
            not re.match(self.TIMESTAMP_REGX, values["end_time"])):
            sys.stderr.write("ERROR: Incorrect time value\n")
            raise Exception("Incorrect time value")
        
        if int(values["start_time"]) > int(values["end_time"]):
            sys.stderr.write("ERROR: Time range error\n")
            raise Exception("Time range error") 

    async def __make_requests(self):
        async with aiohttp.ClientSession() as session:
            for url in self.REQ_URLS:
                async with session.get(url) as req:
                    self.results.append(await req.json())
    
    async def __run_requests(self, timeout=TIMEOUT):
        task = self.__make_requests()
        await asyncio.wait_for(task, timeout)          
        
    def __fetch_api(self):
        try:
            asyncio.run(self.__run_requests())
        except asyncio.exceptions.TimeoutError:
            print("TIMEOUT")
        except Exception as e:
            print(e)
            exit(1)

    # Counts views and writes them to output file
    def __process_data(self, output_file:str):  
        with open(output_file, "a") as out_file:
            for result in self.results:
                try:
                    result = result["items"]
                    article_name = result[0]["article"]
                    views_count = 0
                    for entry in result:
                        views_count += int(entry["views"])
                    out_file.write(f"{article_name}\t{views_count}\n")
                except KeyError:
                    out_file.write(f"{article_name}\tNF\n")
                    sys.stderr.write(f"{article_name} NOT FOUND\n")
                except Exception as e:
                    sys.stderr.write(f"ERROR: {e}\n")
        self.results.clear()


    ## Gets pageviews from api, writes results to output file
    def get_page_views(
        self, 
        input_file:str, 
        output_file:str,
        project_name:str="en.wikipedia.org",
        access_type:str="all-access", 
        agent_type:str="all-agents", 
        granularity:str="monthly", 
        start_time:str=LOWEST_TIMESTAMP, 
        end_time:str=time.strftime('%Y%m%d%H', time.localtime(time.time())) # Current time
        ):
        

        if self.CHECK_VALUES:
            self.__check_values(locals())

        self.__check_input_file(input_file)  

        print("Loading file, generating urls")
        with open(input_file) as in_file:
            self.REQ_URLS.clear()
            line_counter = 0
            chunk_counter = 0
            for line in in_file:
                article_name = line.strip()
                REQ_URL = self.API_BASE_URL + f"{project_name}/{access_type}/{agent_type}/{article_name}/{granularity}/{start_time}/{end_time}"
                self.REQ_URLS.append(REQ_URL)
                line_counter += 1

                if len(self.REQ_URLS) == self.API_REQ_LIMIT:
                    chunk_counter += 1
                    print(f"Fetching chunk: {chunk_counter}, count: {line_counter}",end="\r")
                    self.__fetch_api()
                    self.__process_data(output_file)
                                        
                    self.results.clear()
                    self.REQ_URLS.clear()
                    time.sleep(self.DELAY)
            
            # Process the rest
            self.__fetch_api()
            self.__process_data(output_file)

            # Cleanup
            self.REQ_URLS.clear()
            self.results.clear()

        print(f"Finished processing {line_counter} articles")
    
    def parse_args(self):
        parser = argparse.ArgumentParser()

        # ARGUMENTS
        ## Article name
        parser.add_argument(
            "-i","--input", 
            type = str, 
            required=True,
            action="store",
            dest="input_file",
            help = "Valid article name",
        )

        parser.add_argument(
            "-o","--output", 
            type = str, 
            required=True,
            action="store",
            dest="output_file",
            help = "Valid article name",
        )

        ## Project name 
        parser.add_argument(
            "-p","--project", 
            type = str, 
            choices=self.PROJECT_VALUES,
            action="store",
            dest="project_name",
            help = "Project name (en.wikipedia, cs.wikipedia, ...)",
            default = "en.wikipedia.org"
        )

        ## Access 
        parser.add_argument(
            "--access", 
            type = str, 
            choices=self.ACCESS_VALUES,
            action="store",
            dest="access_type",
            help = "Access type (all-access, desktop, mobile-app, mobile-web)",
            default = "all-access"
        )

        ## Agent
        parser.add_argument(
            "--agent", 
            choices=self.AGENT_VALUES,
            action="store",
            dest="agent_type",
            help = "Access agent (all-agents, user, spider, automated)",
            default = "all-agents"
        )

        ## Granulatity
        parser.add_argument(
            "--granularity", 
            type = str, 
            choices=self.GRANULARITY_VALUES,
            action="store",
            dest="granularity",
            help = "Entries granularity (daily, monthly)",
            default = "monthly"
        )

        ## Start timestamp
        parser.add_argument(
            "-s","--start", 
            type = str, 
            action="store",
            dest="start_time", 
            help = "Start timestamp: YYYYMMDD(HH)",
            default=self.LOWEST_TIMESTAMP 
        )

        ## End timestamp
        parser.add_argument(
            "-e","--end", 
            type = str, 
            action="store",
            dest="end_time",
            help = "End timestamp: YYYYMMDD(HH)",
            default = time.strftime('%Y%m%d%H', time.localtime(time.time())) # Current time
        )

        return parser.parse_args()
        
# Run as standalone script
if __name__ == "__main__":   
    pw = PageViews(check_values=False)
    
    args = pw.parse_args()

    pw_count = pw.get_page_views(
        input_file = args.input_file,
        output_file = args.output_file,
        project_name = args.project_name,
        access_type = args.access_type,
        agent_type = args.agent_type,
        granularity = args.granularity,
        start_time = args.start_time,
        end_time = args.end_time
    )



############################################
