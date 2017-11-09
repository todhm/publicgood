import requests
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import json

class DataCrawler(object):
    def __init__(self,col):
        self.metaCol = col



    def getMetaData(self,metaType):
        apiUrl= 'http://api-data.iadb.org/metadata/{}'\
                '?searchtype=name&searchvalue=all&languagecode=en'\
                '&responsetype=json'.format(metaType)

        data = requests.get(apiUrl)

        if data.status_code != 200:
            print("Fail to query " + metaType)
            return None
        else:
            metaJson = json.loads(data.text)
            return metaJson



    # Modifies:MongoDB
    # Effect: Crawling MetaData By metaType not Considering pre-inserted Data
    def BulkSaveMetaData(self,jsonData):
        self.metaCol.insert_many(jsonData)



    # Modifies: MongoDB
    #Effect: Crawling MetaData by metaType only data which is not exists.
    def SaveNoDupMetaData(self,metaType,jsonData):



if __name__ == "__main__":

    client = MongoClient("localhost")
    idbDB = client.idbDB
    metacol = idbDB.metadata
    dc = DataCrawler(metacol)
    
    metaType = []
    metaType.append("country")
    metaType.append("topic")
    metaType.append("subtopic")
    metaType.append("indicator")
    for meta in metaType:
        jsonData = dc.getMetaData(meta)
        dc.BulkSaveMetaData(jsonData)
