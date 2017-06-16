
class MyMongoDBInstance():

    def __init__(self, db_name = 'teste'):
        self.db_name = db_name
        self.collections = []
    
    def get_db(self):
        from pymongo import MongoClient
        client = MongoClient('localhost:27017')
        db = client[self.db_name] # return db
        self.collections = db.collection_names()
        return db

    def save(self, data, collection = ''):
        db = self.get_db()
        if collection in self.collections :
            db.collection.insert_many(data, ordered=False)
        else :
            print('criando colecao: ',collection)
            db.collection.insert_many(data, ordered=False)
        # return db.insert_many(data,ordered=False)