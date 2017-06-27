from pymongo import MongoClient
import pprint

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
        lsIdPessoa = db['rawLicitacao'].distinct('idPessoa')
        
        for idPessoa in lsIdPessoa:
            cursor = self.process_item_licitacao(idPessoa)
            docs_updated = self.update_field(cursor)
            print('idPessoa', idPessoa)
            print('Documentos atualizados: ',docs_updated)
        
    
    def process_item_licitacao(self, idPessoa):
        match = { '$match' : { 'idPessoa' : idPessoa } }
        project = { '$project' : {'cdIBGE' : 0, 'idPessoa' : 0, 'nmEntidade' : 0,'nrAnoLicitacao' : 0, 'nrLicitacao': 0, 'dsModalidadeLicitacao': 0,'idUnidadeMedida' : 0 ,'idTipoEntregaProduto': 0,'DataReferencia' : 0, 'ultimoEnvioSIMAMNesteExercicio' : 0 }}
        group = { '$group' : {
            '_id' : '$idlicitacao',
            'item' : { '$push' : {
                'fornecedor' : '$nmPessoa',
                'nrDocumento' : '$nrDocumento',
                'nrLote' :'$nrLote',
                'nrItem' : '$nrItem',
                'nrQuantidade' : '$nrQuantidade',
                'dsUnidadeMedida' : '$dsUnidadeMedida',
                'vlMinimoUnitarioItem' : '$vlMinimoUnitarioItem',
                'vlMinimoTotal' : '$vlMinimoTotal'
                'vlMaximoUnitarioItem' '$vlMaximoUnitarioitem',
                'vlMaximoTotal' : '$vlMaximoTotal',
                'dsItem' : '$dsItem',
                'dsFormaPagamento' : '$dsFormaPagamento',
                'nrPrazoLimiteEntrega' : '$nrPrazoLimiteEntrega',
                'dsTipoEntregaProduto': '$dsTipoEntregaProduto',
                'nrQuantidadePropostaLicitacao' : '$nrQuantidadePropostaLicitacao',
                'vlPropostaItem' : '$vlPropostaItem',
                'dtValidadeProposta' : '$dtValidadeProposta',
                'dtPrazoEntregaPropostaLicitacao' : '$dtPrazoEntregaPropostaLicitacao',
                'nrQuantidadeVencedorLicitacao' : '$nrQuantidadeVencedorLicitacao',
                'vlUnitarioVencedor' : '$vlLicitacaoVencedorLicitacao',
                'nrClassificacao' : '$nrClassificacao',
                'vlTotalVencedor' : '$vlTotalVencedorLicitacao'
            }},
            'vlTotalAdquiridoLicitacao' : {
                '$sum' : '$vlTotalVencedorLicitacao'
            }
        }}
        pipeline = [match, project, group]
        db = self.get_db()
        cursor =  db.rawLicitacaoVencedor.aggregate(pipeline)
        return cursor

    def update_field(self, cursor = None):
        if not cursor:
            raise AttributeError('parametro invalido em update_field')
        db = self.get_db()
        count = 0
        docs_updated = []
        for doc in cursor:
            result = db.rawLicitacao.update_one({'idLicitacao' : doc['_id']}, { '$set' : {'item' : doc['item'], 'vlTotalAdquiridoLicitacao': doc['vlTotalAdquiridoLicitacao']} })
            print(result.matched_count, result.modified_count)
            count += 1
            # docs_updated.append(result.raw_result)
        # return count, docs_updated
        return count

        
    

if __name__ == "__main__":
    p = ProcessData()
    p.process()
    