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
        lsEntidade = db['rawLicitacao'].distinct('idPessoa')
        print('Executando para as entidades:\n',lsEntidade)
        for entidade in lsEntidade:
            cursor = self.agrupar_licitacao_anual_por_modalidade(entidade)
            docs_inserted = self.inserir_collecao_resumo_licitacao(cursor)

            
    def inserir_collecao_resumo_licitacao(self, cursor):
        if not cursor:
            raise AttributeError('parametro invalido')
        db = self.get_db()
        count = 0
        for doc in cursor:
            doc['cdIBGE'] = doc['_id']['cdIBGE']
            doc['cdEntidade'] = doc['_id']['cdEntidade']
            doc['nrAnoLicitacao'] = doc['_id']['nrAnoLicitacao']
            doc['dsModalidadeLicitacao'] = doc['_id']['dsModalidadeLicitacao']
            doc['_id'] = ObjectId()
            result = db['resumoLicitacaoAnual'].insert_one(doc)
            if result:
                count += 1

        return count

    def agrupar_licitacao_anual_por_modalidade(self, cdEntidade):
        match = { '$match' : {'idPessoa' : cdEntidade}}
        group = { '$group' : { 
            '_id' : {
                'cdIBGE' : '$cdIBGE', 
                'cdEntidade' : '$idPessoa',
                'nrAnoLicitacao' : '$nrAnoLicitacao',
                'dsModalidadeLicitacao' : '$dsModalidadeLicitacao' 
                },
            "nmMunicipio" : { "$first" : "$nmMunicipio"},
            'nmEntidade' : {"$first" : "$nmEntidade"},
            'nrQuantidadeProcedimento' : {'$sum' : 1},
            'vlAnualTotalAdquirido' : { '$sum' : '$vlTotalAdquiridoLicitacao'},
            'vlAnualTotalLicitado' : { '$sum' : '$vlLicitacao'},
            'procedimento' : { '$push' : {
                'idlicitacao' : '$idLicitacao',
                'dsModalidade' : '$dsModalidadeLicitacao',
                'nrAnoLicitacao' : '$nrAnoLicitacao',
                'nrLicitacao' : '$nrLicitacao',
                'dsObjeto' : '$dsObjeto',
                'vlLicitacao' : '$vlLicitacao',
                'vlAdquirido' : '$vlTotalAdquiridoLicitacao'
            }}
        }}
        
        pipeline = [match, group]
        db = self.get_db()
        print('executando pipeline... ',cdEntidade)
        cursor = db.rawLicitacao.aggregate(pipeline)
        return cursor

if __name__ == "__main__":
    p = ProcessData()
    p.process() 