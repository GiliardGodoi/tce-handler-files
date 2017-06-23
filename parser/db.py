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
            ls = list(conjunto)
            ls.sort()
            return ls
        elif type(data) is dict :
                return list(data.keys())

class StorageFactory(object):
    def __init__(self):
        self.storages = {'mongo' : MongoStorage, 'csv' : FileCSVStorage, 'json' : FileJSONStorage}
        self.instance = None

    def get_storage(self, storage_name = '') :
        '''
            Tentativa de implementar um singleton com factory method. Vc pode instanciar duas classes de um mesmo storage utilizando o seguinte artificio:
            instancia primeiro de um tipo e depois de outro. quando for instanciar o terceiro - que será do mesmo tipo do primeiro - uma nova instancia deste
            primeiro sera retornada. Mas isso se deve como o python gerencia as suas variaveis
        '''
        if storage_name in self.storages :
            if not self.instance :
                self.instance = self.storages[storage_name]()
                return self.instance
            elif not type(self.instance) is self.storages[storage_name]:
                self.instance = self.storages[storage_name]()
                return self.instance
            else:
                return self.instance           
        else:
            raise AttributeError('Storage name is not defined')
    
    def _print(self,name = 'printar'):
        print(name)