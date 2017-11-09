from crawlData import DataCrawler
from pymongo import MongoClient
import unittest


class MetaDataCrawlerTest(unittest.TestCase):

    def setUp(self):
        client = MongoClient("localhost")
        self.testDB = client.testDB
        self.testCol = testDB.testCol
        self.testCountry = [{ 'ALPHA2Code': 'AR',
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


        self.testIndicator =   [{    'AggregationMethod': 'tm',
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


        self.testTopic = [  {   'SubTopic': [   { 'SubTopicCode': 'AGRPOL',
                                                'SubTopicName': 'Agricultural Policy'},
                                                { 'SubTopicCode': 'Balance of Payments',
                                                'SubTopicName': 'Balance of Payments'},
                                                {'SubTopicCode': 'Wages', 'SubTopicName': 'Wages'},
                                                {'SubTopicCode': 'WATER', 'SubTopicName': 'Water'}],
                                                'TopicCode': 'Economy',
                                                'TopicDesc': 'testData',
                                                'TopicName': 'Economy'}]




    def insertExistedData(self):
        pass



    def insertUnExistedData(self):
        pass






if __name__ == '__main__':
    unittest.main()
