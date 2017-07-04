from pymongo import MongoClient

if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017")
    db = client['raw_data']
    cd_para_remover = '411370'
    ls_colletcions = db.collection_names()

    for coll in ls_colletcions:
        result = db[coll].delete_many({'cdIBGE' : cd_para_remover })
        print(coll,result.deleted_count)