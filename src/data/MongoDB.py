import pymongo
import json
import bson
import datetime
from src.business.Common import Common
from pymongo import MongoClient


class MongoDB:
    """connection to the MongoDB database"""    

    def __init__(self, DB='DB_DATABASE_NAME'):
        self.user = Common.Configuration['Database']['DB_USER_NAME']
        self.password = Common.Configuration['Database']['DB_PASSWORD']
        self.cluster = Common.Configuration['Database']['DB_CLUSTER']
        self.port = Common.Configuration['Database']['DB_PORT']
        self.db = DB
        self.database = Common.Configuration['Database'][self.db]
        """
        if self.db == 'Stage':
          self.database = Common.Configuration['Database']['DB_DATABASE_STAGE_NAME']
        else:
          self.database = Common.Configuration['Database']['DB_DATABASE_NAME']
        """        

        self._stringConn = (f'mongodb://{self.user}:{self.password}@{self.cluster}:{self.port}/')

        try:
            cluster = MongoClient(self._stringConn)
            self._db = cluster[self.database]            
        except Exception as ex:
            return (f'Database connection error: {ex}')
    
    def Find(self, Filter, Collection, Limit):
        try:
            coll = self._db[Collection]
            listFound = list(coll.find(Filter).limit(Limit))
            return listFound
        except Exception as ex:
            return(f'Find error: {Collection}')

    def InsertItems(self, Items, Collection):
        """ Insert a set of documents"""
        try:
            coll = self._db[Collection]
         
            insList = coll.insert_many(Items)
            
            return insList
        except Exception as ex:
            return (f'Data insert error: {Collection}')
    
    def UpdateItems(self, Filter, Update, Collection, Upsert):
        """ Update a set of documents"""
        try:
            coll = self._db[Collection]

            updList = coll.update_many(Filter, Update, upsert=Upsert)
            return updList
        except Exception as ex:
            return(f'Data update error: {Collection}')


   