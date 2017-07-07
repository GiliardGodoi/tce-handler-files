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
        db = self.get_db()
        lsCodEntidade = db.rawLicitacao.distinct('cdEntidade')
        for cod in lsCodEntidade:
            cursor = self.agrupar_ano_modalidade_avaliacao(cod)
    
    def agrupar_ano_modalidade_avaliacao(self,cod):
        match = {"$match" : {"cdEntidade" : cod}}
        group = { "$group" :
            "_id" :{
                "nrAno" : "$nrAnoLicitacao",
                "dsModalidade" : "$dsModalidadeLicitacao"
                "dsAvaliacao" : "$dsAvaliacaoLicitacao"
            },
            "total" : {'$sum' : 1 },
            "vlLicitado" : { '$sum' : "$vlLicitado"},
            'vlAdquirido' : {'$sum' : "$vlTotalAdquiridoLicitacao"}
        }
        pipeline = [match, group]
        db = self.get_db()
        cursor = db.rawLicitacao.aggregate(pipeline)
        return cursor

if __name__ == "__main__":
    p = ProcessData()
    p.process() 