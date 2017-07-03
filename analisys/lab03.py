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
        lsAno = db['rawLicitacao'].distinct('nrAnoLicitacao')
        lsEntidade = db['rawLicitacao'].distinct('idPessoa')
        for entidade in lsEntidade:
            cursor = self.process_resumo_licitacao_anual(entidade)
            
    
    def process_resumo_licitacao_anual(self, cdEntidade):
        match = { '$match' : {'idPessoa' : cdEntidade}}
        group = { '$group' : { 
            '_id' : {
                'cdIBGE' : '$cdIBGE', 
                'cdEntidade' : '$idPessoa',
                'nrAnoLicitacao' : '$nrAnoLicitacao',
                'dsModalidadeLicitacao' : '$dsModalidadeLicitacao' 
                },
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
        out = { '$out' : 'resumoAnualLicitacoes' }
        pipeline = [match, group, out]
        db = self.get_db()
        cursor = db.rawLicitacao.aggregate(pipeline)
        return cursor

if __name__ == "__main__":
    p = ProcessData()
    p.process() 