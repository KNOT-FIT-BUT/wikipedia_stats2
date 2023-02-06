############################################
# Title:  get_pageviews.py                 #
# Author: Jakub Štětina                    #
# Date:   4 Feb 2023                       #
############################################

import requests
import argparse
import time
import json
import sys
import re

class PageViews():
    API_BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    ## Non-python User-Agent needed to prevent blocking from WIKIMEDIA API
    user_agent = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1) AppleWebKit/534.1.2 (KHTML, like Gecko) Chrome/38.0.845.0 Safari/534.1.2'}

    # VALID VALUES 
    PROJECT_VALUES = ['en.wikipedia.org', 'cs.wikipedia.org', 'sk.wikipedia.org']
    ACCESS_VALUES = ["all-access", "desktop", "mobile-app", "mobile-web"]
    AGENT_VALUES = ["all-agents", "user", "spider", "automated"]
    GRANULARITY_VALUES = ["daily", "monthly"]
    TIMESTAMP_REGX = "^\d{8}(?:\d{2})?$"
    LOWEST_TIMESTAMP = "20150701" # No data before this date

    def __init__(self, check_values:bool=True):
        self.CHECK_VALUES = check_values

    def __make_request(self, req_url:str) -> requests.Response:
        return requests.get(req_url, headers=self.user_agent)

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

    # Returns true on good request
    # False on NOT FOUND
    # Raises exception if anything else
    def __check_response(self, response:requests.Response):
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        elif 400 <= response.status_code < 500:
            raise Exception("Bad request")
        raise Exception(f"Request error, response message: {response.text}")

    def __count_pageviews(self, resp_data:list):
        page_views_count = 0
        for entry in resp_data:
            page_views_count += int(entry["views"])
        return page_views_count    

    ## Returns the number of views for the input article in a given time period
    ## else False (if article DOES NOT EXIST) 
    def get_page_views(
        self, 
        article_name:str,
        project_name:str="en.wikipedia.org",
        access_type:str="all-access", 
        agent_type:str="all-agents", 
        granularity:str="monthly", 
        start_time:str=LOWEST_TIMESTAMP, 
        end_time:str=time.strftime('%Y%m%d%H', time.localtime(time.time())) # Current time
        ) -> int:

        if self.CHECK_VALUES:
            self.__check_values(locals())

        REQ_URL = self.API_BASE_URL + f"{project_name}/{access_type}/{agent_type}/{article_name}/{granularity}/{start_time}/{end_time}"
        response = self.__make_request(REQ_URL)     

        if not self.__check_response(response):
            return False

        resp_data = json.loads(response.text)["items"]
        return self.__count_pageviews(resp_data)
    
    def parse_args(self):
        parser = argparse.ArgumentParser()

        # ARGUMENTS
        ## Article name
        parser.add_argument(
            "-n","--name", 
            type = str, 
            required=True,
            action="store",
            dest="article_name",
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
        article_name = args.article_name,
        project_name = args.project_name,
        access_type = args.access_type,
        agent_type = args.agent_type,
        granularity = args.granularity,
        start_time = args.start_time,
        end_time = args.end_time
    )

    print("Name","Pageviews",sep="\t")
    
    if pw_count is False:
        print(args.article_name, "NF", sep="\t")
    
    else:
        print(args.article_name, pw_count, sep="\t")
    
    
    


