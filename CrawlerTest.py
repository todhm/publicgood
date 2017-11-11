from crawlData import DataCrawler
from pymongo import MongoClient
import unittest


class MetaDataCrawlerTest(unittest.TestCase):

    def setUp(self):
        client = MongoClient("localhost")
        self.testDB = client.testDB
        self.testCol = self.testDB.testCol
        self.testDataCol = self.testDB.testDataCol
        testDict = {}
        testDict['Country'] = [{ 'ALPHA2Code': 'AR',
                                 'CountryCode': 'ARG',
                                 'CountryIncomeGroup': 'Upper middle income',
                                 'CountryLongName': 'Argentine Republic',
                                 'CountryRegion': 'Latin America & Caribbean',
                                 'CountryShortName': 'ARGENTINA',
                                 'CountrySpecialNotes': '',
                                 'CountryTableName': 'Argentina',
                                 'CurrencyUnit': 'Argentine peso',
                                 'OtherRegionalGroup': '',
                                 'WB2Code': 'AR'}]


        testDict['Indicator'] =   [{   'AggregationMethod': 'tm',
                                        'BasePeriod': '',
                                        'DataSetName': 'LMW',
                                        'DataSetcode': 'LMW',
                                        'DerivationMethod': '',
                                        'GeneralComment': '',
                                        'IndicatorCode': 'LMW_1',
                                        'IndicatorLongName': 'ttttt.',
                                        'IndicatorName': 'ttttt111',
                                        'IndicatorShortDef': 'tsfsdafas',
                                        'IndicatorSource': 'CBNA',
                                        'IndicatorSynonym': '',
                                        'LimitationException': '',
                                        'OtherNote': '',
                                        'Periodicity': 'Not Available',
                                        'PowerCode': '',
                                        'PredefinedCountry': '0',
                                        'PredefinedRegion': '1',
                                        'PredefinedTopic': '1',
                                        'ReferencePeriod': '',
                                        'SourceComment': '',
                                        'SubTopicName': 'National Accounts',
                                        'TopicID': None,
                                        'TopicName': 'Economy',
                                        'UOM': 'millions of US$'}]


        testDict['Topic'] = [  {   'SubTopic': [   { 'SubTopicCode': 'AGRPOL',
                                                'SubTopicName': 'Agricultural Policy'},
                                                { 'SubTopicCode': 'Balance of Payments',
                                                'SubTopicName': 'Balance of Payments'},
                                                {'SubTopicCode': 'Wages', 'SubTopicName': 'Wages'},
                                                {'SubTopicCode': 'WATER', 'SubTopicName': 'Water'}],
                                    'TopicCode': 'Economy',
                                    'TopicDesc': 'testData',
                                    'TopicName': 'Economy'}]


        testDict['SubTopic'] = [{   'SubTopicCode': 'National Accounts',
                                 'SubTopicName': 'National Accounts',
                                 'SubtopicDesc': ' ',
                                 'TopicName': 'Economy'}]


        self.testData = testDict
        self.testCrawler = DataCrawler(self.testCol,self.testDataCol)



    def tearDown(self):
        self.testDB.drop_collection('testCol')
        self.testDB.drop_collection('testDataCol')




    def test_Getter(self):

        self.copyDataHelper("Country")
        countryCodeList = self.testCrawler.getCountryCodeList()
        self.assertEquals(countryCodeList,11)









    def test_insertExistedMetaData(self):
        for key in self.testData:
            self.testCrawler.saveNoDupMetaData(key, self.testData[key])
            self.testCrawler.saveNoDupMetaData(key, self.testData[key])
            keyCode = key + "Code"
            getData = list ( self.testCol.find({keyCode:{ "$exists": True}},
                                               { "_id": False }))

            self.assertEqual( len(getData), 1)






    def copyDataHelper(self,key):

        originData = self.testData[key][0]
        emptyList = []
        for i in range(10):
            strInd = str(i)
            emptyDict = {}
            for nestedKey in originData:
                if nestedKey == "SubTopic":
                    emptyDict[nestedKey] =  [ { 'SubTopicCode': strInd,
                                                'SubTopicName':  strInd}]
                else:
                    emptyDict[nestedKey] = str(originData[nestedKey]) + strInd
            emptyList.append(emptyDict)
        self.testCrawler.saveNoDupMetaData(key,emptyList)



    def test_insertUnExistedMeataData(self):

        list( map( self.copyDataHelper,self.testData))

        for key in self.testData:
            keyCode = key + "Code"
            queryData = list ( self.testCol.find({keyCode:{ "$exists": True}},
                                                 { "_id": False }))
            self.assertEqual( len(queryData), 10)














if __name__ == '__main__':
    unittest.main()
