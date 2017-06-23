import codecs
import json
import csv
import os

class MongoStorage():

    def __init__(self, db_name = 'public_data'):
        self.db_name = db_name
        self.collections = []
    
    def get_db(self):
        from pymongo import MongoClient
        client = MongoClient('localhost:27017')
        db = client[self.db_name] # return db
        self.collections = db.collection_names()
        return db

    def save(self, data, collection = None):
        db = self.get_db()
        result = {}
        insert_result = None
        if collection in self.collections :
            result = db[collection].insert_many(data, ordered=False)
        else :
            print('criando colecao: ',collection)
            insert_result = db[collection].insert_many(data, ordered=False)
        # return db.insert_many(data,ordered=False)
        if insert_result :
            result['inserted_lenght'] = len(insert_result.inserted_ids) #TypeError: 'InsertManyResult' object does not support item assignment
            result['inserted_ids'] = insert_result.inserted_ids
            return result
        else :
            return None
    
class FileJSONStorage():
            
    def save(self,data,file_name):
        file_name += '.json'
        with codecs.open(file_name, encoding='utf-8', mode='a') as file:
            for d in data:
                file.write(self.dict_to_json(d))
            return True
    
    def dict_to_json(self,dict_data):
        return json.JSONEncoder(ensure_ascii=False,indent=2).encode(dict_data)

class FileCSVStorage():
    def save(self, data, file_name):
        file_name += '.csv'
        field_names = self.define_header(data)
        with open(file_name,'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(data)
    
    def define_header(self, data):
        conjunto = set()
        if type(data) is list:
            for reg in data:
                ls = list(reg.keys())
                for e in ls:
                    conjunto.add(e)
            return list(conjunto)
        elif type(data) is dict :
                return list(data.keys())