KB_HEAD_TEMPLATE = \
{
    "<__generic__>": [
        "ID",
        "TYPE",
        "NAME",
        "DISAMBIGUATION NAME",
        "{m}ALIASES",
        "DESCRIPTION",
        "{m}ROLES",
        "FICTIONAL",
        "{u}WIKIPEDIA URL",
        "{u}WIKIDATA URL",
        "{u}DBPEDIA URL",
        "{gm[http://athena3.fit.vutbr.cz/kb/images/]}IMAGES"
    ],
    "<person>": [
        "GENDER",
        "{e}DATE OF BIRTH",
        "PLACE OF BIRTH",
        "{e}DATE OF DEATH",
        "PLACE OF DEATH",
        "{m}NATIONALITIES"
    ],
    "<group>": [
        "{m}INDIVIDUAL NAMES",
        "{m}GENDERS",
        "{em}DATES OF BIRTH",
        "{m}PLACES OF BIRTH",
        "{em}DATES OF DEATH",
        "{m}PLACES OF DEATH",
        "{m}NATIONALITIES"
    ],
    "<artist>": [
        "{m}ART FORMS",
        "{m}INFLUENCERS",
        "{m}INFLUENCEES",
        "ULAN ID",
        "{mu}OTHER URLS"
    ],
    "<geographical>": [
        "LATITUDE",
        "LONGITUDE",
        "{m}SETTLEMENT TYPES",
        "COUNTRY",
        "POPULATION",
        "ELEVATION",
        "AREA",
        "{m}TIMEZONES",
        "FEATURE CODE",
        "{m}GEONAMES IDS"
    ],
    "<event>": [
        "{e}START DATE",
        "{e}END DATE",
        "{m}LOCATIONS",
        "EVENT TYPE"
    ],
    "<organization>": [
        "{e}FOUNDED",
        "{e}CANCELLED",
        "LOCATION",
        "ORGANIZATION TYPE"
    ],
    "<__stats__>": [
        "WIKI BACKLINKS",
        "WIKI HITS",
        "WIKI PRIMARY SENSE"
    ]
}