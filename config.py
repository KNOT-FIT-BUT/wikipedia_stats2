# Wikipedia stats project config

WS_BASE_DIR = "/mnt/minerva1/nlp-in/wikipedia-statistics/"
STATS_DIR = WS_BASE_DIR + "stats"
DATA_FILE = WS_BASE_DIR + "data/last_update"

DUMP_DIR = "/mnt/minerva1/nlp/corpora_datasets/monolingual/{}/wikipedia"

DATE_FORMAT = "%Y-%m-%d"
SEC_IN_DAY = 86400

PROJECTS = {
    "en": "english",
    "cs": "czech", 
    "sk": "slovak"
}

PAGES_ARTICLES_DUMP_REG = r"^(?:cs|en|sk)wiki-\d{8}-pages-articles.xml$"

