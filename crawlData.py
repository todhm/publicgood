import requests
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import json

class DataCrawler(object):
    def __init__(self,metaCol,dataCol):
        self.metaCol = metaCol
        self.dataCol = dataCol



    # Effect: get the List of CountryCode
    def getCountryCodeList(self):
        self.metaCol.find("")



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
    def saveNoDupMetaData(self,metaType,jsonData):
        if metaType not in ['Country','Indicator','Topic','SubTopic']:
            print("Not Matched MetaType")
            return False

        else:
            metaKey = metaType + "Code"
            for data in jsonData:
                if metaKey not in data:  continue
                self.metaCol.update({metaKey: data[metaKey]},data,upsert=True)




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
