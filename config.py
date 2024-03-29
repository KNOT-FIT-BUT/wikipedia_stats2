# Wikipedia stats project config

WS_BASE_DIR = "/mnt/minerva1/nlp-in/wikipedia-statistics/"
STATS_DIR = WS_BASE_DIR + "stats"
DATA_FILE = WS_BASE_DIR + "data/last_update"
DATA_DIR = WS_BASE_DIR + "data/"
PROJECT_FILES = DATA_DIR + "project_files"

DUMP_DIR = "/mnt/minerva1/nlp/corpora_datasets/monolingual/{}/wikipedia"

DATE_FORMAT = "%Y-%m-%d"
FILE_DATE_FORMAT = "%Y_%m_%d_%H_%M_%S"

SEC_IN_DAY = 86400

PROJECTS = {
    "en": "english",
    "cs": "czech", 
    "sk": "slovak"
}

PAGES_ARTICLES_DUMP_REG = r"^(?:cs|en|sk)wiki-\d{8}-pages-articles.xml$"

FILE_LOCK_TIMEOUT = 600 # 10 minutes
LOCKED_FILE_MESSAGE = "File Acquisition Timeout: Process Exiting with Failure"
