############################################
# Title:  generate_pageviews.py            #
# Author: Jakub Štětina                    #
# Date:   9 Feb 2023                       #
############################################

import argparse
import asyncio
import aiohttp
import time
import json
import sys
import re
import os

class PageViews():
    API_BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    API_REQ_LIMIT = 200
    
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

    async def __get_async_requests(self, url:str, limit):
        async with limit:
            async with aiohttp.ClientSession() as session:
                req = await session.get(url)
                if limit.locked():
                    await asyncio.sleep(1)
                return req
    
    def __get_tasks(self, limit):
        tasks = []
        for url in self.REQ_URLS:
            tasks.append(self.__get_async_requests(url, limit))
        return tasks            
        

    async def __run_requests(self):
        limit = asyncio.Semaphore(self.API_REQ_LIMIT)
        self.results = []
        async with aiohttp.ClientSession() as session:
                print("Generating tasks")
                tasks = self.__get_tasks(limit)
                print("Running tasks")
                responses = await asyncio.gather(*tasks)
                print("Collecting responses")
                for response in responses:
                    self.results.append(await response.json())

    def __process_data(self, output_file:str):  
        with open(output_file, "w") as out_file:
            for result in self.results:
                result = result["items"]
                article_name = result[0]["article"]
                views_count = 0
                for entry in result:
                    views_count += int(entry["views"])
                out_file.write(f"{article_name}\t{views_count}\n")


    ## Returns the number of views for the input article in a given time period
    ## else False (if article DOES NOT EXIST) 
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

        # Load input file, generate URLs
        print("Loading file, generating urls")
        with open(input_file) as in_file:
            self.REQ_URLS = []
            for line in in_file:
                article_name = line.strip()
                REQ_URL = self.API_BASE_URL + f"{project_name}/{access_type}/{agent_type}/{article_name}/{granularity}/{start_time}/{end_time}"
                self.REQ_URLS.append(REQ_URL)
        

        asyncio.run(self.__run_requests())

        self.__process_data(output_file)
    
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
            type = str, 
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



