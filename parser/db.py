import codecs
import json
import csv
import os

class MongoStorage():

    def __init__(self, db_name = 'raw_data'):
        self.db_name = db_name
        self.collections = []
    
    def get_db(self):
        from pymongo import MongoClient
        client = MongoClient('localhost:27017')
        db = client[self.db_name] # return db
        self.collections = db.collection_names()
        return db

    def save(self, data = [], collection = None):
        if not data :
            return 0
        db = self.get_db()
        result = {}
        insert_result = None
        if collection in self.collections :
            insert_result = db[collection].insert_many(data, ordered=False)
        else :
            print('criando colecao: ',collection)
            insert_result = db[collection].insert_many(data, ordered=False)
        # return db.insert_many(data,ordered=False)
        if insert_result :
            # result['inserted_lenght'] = len(insert_result.inserted_ids) #TypeError: 'InsertManyResult' object does not support item assignment
            # result['inserted_ids'] = insert_result.inserted_ids
            return len(insert_result.inserted_ids)
        else :
            return None

class SingletonMongoStorage():
    def __init__(self):
        self.instance = None

    def get_instance(self):
        if not self.instance:
            self.instance = MongoStorage()
        return self.instance

class StorageFactory(object):
    def __init__(self):
        self.storages = {}
        self.storages['mongo'] = SingletonMongoStorage()

    def get_storage(self, storage_name = '') :
        if storage_name in self.storages :
            return self.storages[storage_name].get_instance()
        else:
            raise AttributeError('Storage name is not defined')