from pymongo import MongoClient

class ProcessData():
    def __init__(self):
        self._db = None

    def get_db(self):
        if not self._db:
            client = MongoClient("mongodb://localhost:27017")
            self._db = client['raw_data']
        return self._db
    
    def process(self):
        

if __name__ == "__main__":
    p = ProcessData()
    p.process()