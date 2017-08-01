import json
import codecs
from pymongo import MongoClient

def atualizaEstado(registro):
    estado = {"cdUF" : "41", "nmUF" : "PARANÁ"}
    registro.update(estado)
    return registro

def atualizaCodigoIBGE(registro):
    aux = registro["cdIBGE"]
    registro["cdIBGEdv"] = aux
    registro["cdIBGE"] = aux[0:(len(aux)-1)]
    return registro

FILE_NAME = "data/info.json"

FILE = codecs.open(FILE_NAME,mode="r",encoding="utf8")

listaMunicipios = json.loads(FILE.read())

objetoIteravel = map(atualizaEstado,listaMunicipios)
objetoIteravel = map(atualizaCodigoIBGE,objetoIteravel)

client = MongoClient("mongodb://localhost:27017")
db = client['raw_data']

db.municipio.insert_many(objetoIteravel)