from pymongo import MongoClient
import bson
from bson.objectid import ObjectId

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
        lstCodEntidade = db['rawLicitacao'].distinct('cdEntidade')
        print('Executando para as entidades:\n',lstCodEntidade)
        for entidade in lstCodEntidade:
            cursor = self.agrupar_licitacao_anual_por_modalidade(entidade)
            aux = self.informacao_municipio(entidade)
            self.inserir_collecao_resumo_licitacao(aux,cursor)

            
    def inserir_collecao_resumo_licitacao(self,aux, cursor):
        if not (cursor and aux):
            raise AttributeError('cursor ...')
        obj = {}
        obj['nmMunicipio'] = aux['nmMunicipio']
        obj['cdIBGE'] = aux['cdIBGE']
        
        obj['sinopse'] = []
        for doc in cursor:
            sinopse = {}
            obj['nrAnoLicitacao'] = doc['_id']['nrAnoLicitacao']
            sinopse['dsModalidadeLicitacao'] = doc['_id']['dsModalidadeLicitacao']
            sinopse["nmEntidade"] = aux["nmEntidade"]
            sinopse['nrQuantidadeProcedimento'] = doc['nrQuantidadeProcedimento'],
            sinopse['vlAnualTotalAdquirido'] = doc['vlAnualTotalAdquirido'],
            sinopse['vlAnualTotalLicitado'] = doc['vlAnualTotalLicitado'],
            obj['sinopse'].append(sinopse)
        db = self.get_db()
        db.sinopseLicitacao.insert_one(obj)

        
    
    def informacao_municipio(self, cdEntidade):
        db = self.get_db()
        aux = db.rawLicitacao.find_one({"cdEntidade" : cdEntidade}, {"_id" : 0, "nmMunicipio" : 1, "nmEntidade" : 1, "cdIBGE" : 1})
        return aux

    def agrupar_licitacao_anual_por_modalidade(self, cdEntidade):
        match = { '$match' : {'cdEntidade' : cdEntidade}}
        group = { '$group' : { 
            '_id' : {
                'nrAnoLicitacao' : '$nrAnoLicitacao',
                'dsModalidadeLicitacao' : '$dsModalidadeLicitacao' 
            },
            'nrQuantidadeProcedimento' : {'$sum' : 1},
            'vlAnualTotalAdquirido' : { '$sum' : '$vlTotalAdquiridoLicitacao' },
            'vlAnualTotalLicitado' : { '$sum' : '$vlLicitacao' },
        }}
        db = self.get_db()
        pipeline = [match, group]
        cursor = db.rawLicitacao.aggregate(pipeline)
        return cursor

if __name__ == "__main__":
    p = ProcessData()
    p.process() 