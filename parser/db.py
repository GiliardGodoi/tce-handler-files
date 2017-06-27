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
    
class FileJSONStorage():
            
    def save(self,data,file_name):
        file_name += '.json'
        qtd = len(data)
        with codecs.open(file_name, encoding='utf-8', mode='a') as file:
            for d in data:
                file.write(self.dict_to_json(d))
        return qtd
    
    def dict_to_json(self,dict_data):
        return json.JSONEncoder(ensure_ascii=False,indent=2).encode(dict_data)

class FileCSVStorage():
    def __init__(self):
        csv.register_dialect('mydialect', delimiter=',',lineterminator='\n' )
        self.sniffer = csv.Sniffer()

    def save(self, data, file_name):
        file_name += '.csv'
        field_names = self.define_header(data)
        nro = len(data)
        csvfile =  open(file_name,'a+')
        try:
            writer = csv.DictWriter(csvfile, fieldnames=field_names,dialect='mydialect')
            testHeader = self.has_header(csvfile)
            if testHeader : 
                print('printando header')
                writer.writeheader()
            writer.writerows(data)
        except Exception as error:
            raise error
        finally :
            csvfile.close()
        return nro
    
    def has_header(self, csvfile):
        csvfile.seek(0)
        texto = csvfile.read( (1024 * 2) )
        csvfile.seek(0)
        if csv.Sniffer().has_header(texto) :
            return True
        else:
            return False

    def define_header(self, data):
        conjunto = set()
        if type(data) is list:
            for reg in data:
                ls = list(reg.keys())
                for e in ls:
                    conjunto.add(e)
            ls = list(conjunto)
            ls.sort()
            return ls
        elif type(data) is dict :
                return list(data.keys())

class SingletonMongo():
    def __init__(self):
        self.instance = None

    def get_instance(self):
        if not self.instance:
            self.instance = MongoStorage()
        return self.instance

class SingletonCSVStorage():
    def __init__(self):
        self.instance = None
    
    def get_instance(self):
        if not self.instance :
            self.instance = FileCSVStorage()
        return self.instance

class SingletonJSONStorage():
    def __init__(self):
        self.instance = None
    
    def get_instance(self):
        if not self.instance:
            self.instance = FileJSONStorage()
        return self.instance

class StorageFactory(object):
    def __init__(self):
        self.storages = {}
        self.storages['mongo'] = SingletonMongo()
        self.storages['csv'] = SingletonCSVStorage()
        self.storages['json'] = SingletonJSONStorage()

    def get_storage(self, storage_name = '') :
        if storage_name in self.storages :
            return self.storages[storage_name].get_instance()
        else:
            raise AttributeError('Storage name is not defined')