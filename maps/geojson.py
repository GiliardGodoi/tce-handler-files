import json
import codecs
from pymongo import MongoClient

def getdb(dbname):
    client = MongoClient("mongodb://localhost:27017")
    db = client[dbname]
    return db

def insert(data):
    db = getdb("raw_data")
    db.geojson.insert_one(data)

def update(geojs):
    estado = {"properties":{"id":"41","name":"PARANÁ"}}
    estado.update(geojs)
    return estado

if __name__ == "__main__":
    FILE_NAME = "geodata/geojs-41-mun2.json"
    FILE = codecs.open(FILE_NAME,mode="r",encoding="utf8")
    
    geojson = json.loads(FILE.read())
    data = update(geojson)
    insert(data)


