# -*- coding: utf-8 -*-
from pymongo import MongoClient
from functools import reduce
import pprint

class ProcessData():
    '''
        Rotina para preparação dos dados 
        rota: licitacao/:idLicitacao
        modelo de dados:
        {
            cdIBGE : <string>,
            nmMunicipio : <string>,
            nmEntidade : <string>,
            cdEntidade : <string>,
            idLicitacao :<string>,
            dsModalidade : <string>,
            nrLicitacao : <string>,
            nrAnoLicitacao : <string>,
            dtEdital : <string>,
            dtAbertura : <string>,
            vlLicitacao : <string>,
            dsNaturezaLicitacao : <string>,
            dsAvaliacaoLicitacao : <string>,
            dsClassificacaoObjeto : <string>,
            dsRegimeExecucao : <string>,
            dsObjeto : <string>,
            dsClausulaProrrogacao : <string>,
            item : <array>,
            vlTotalAdquiridoLicitacao : <double>
        }
    '''
    def __init__(self):
        self._db = None

    def get_db(self):
        if not self._db:
            client = MongoClient("mongodb://localhost:27017")
            self._db = client['raw_data']
        return self._db

    def process(self):
        db = self.get_db()
        lstCdEntidade = db['rawLicitacao'].distinct('cdEntidade')
        # lstCdEntidade = self.determinar_idPessoa_para_processamento()
        
        print("Processamento ocorrendo para:\n",lstCdEntidade)
        for cod in lstCdEntidade:
            cursor = self.process_item_licitacao(cod)
            docs_updated = self.update_field(cursor)
            print('cdEntidade: ', cod, 'Documentos atualizados: ',docs_updated)
        
    def determinar_idPessoa_para_processamento(self):
        db = self.get_db()
        match = { "$match" : {'item' : {'$exists' : True }} }
        project = {"$project" : {'_id' : 0, 'idPessoa' : 1} }
        group = { "$group" :{ "_id" : "$idPessoa" } }
        pipeline = [match, project, group]

        cursor = db.rawLicitacao.aggregate(pipeline)
        idPessoaJaProcessados = reduce(lambda a,x : a + [x['_id']] , list(cursor),[])

        idPessoaFromRawLicitacao = db.rawLicitacao.distinct('idPessoa')

        conjunto = set(idPessoaFromRawLicitacao) - set(idPessoaJaProcessados)

        return list(conjunto)
        
    def process_item_licitacao(self, cdEntidade):
        match = { '$match' : { 'cdEntidade' : cdEntidade } }
        project = { '$project' : {'cdIBGE' : 0, 'cdEntidade' : 0, 'nmEntidade' : 0,'nrAnoLicitacao' : 0, 'nrLicitacao': 0, 'dsModalidadeLicitacao': 0,'idUnidadeMedida' : 0 ,'idTipoEntregaProduto': 0,'DataReferencia' : 0, 'ultimoEnvioSIMAMNesteExercicio' : 0 }}
        group = { '$group' : {
            '_id' : '$idlicitacao',
            'item' : { '$push' : {
                'nmFornecedor' : '$nmFornecedor',
                'nrDocumentoFornecedor' : '$nrDocumento',
                'nrLote' :'$nrLote',
                'nrItem' : '$nrItem',
                'nrQuantidade' : '$nrQuantidade',
                'dsUnidadeMedida' : '$dsUnidadeMedida',
                'vlMinimoUnitario' : '$vlMinimoUnitarioItem',
                'vlMinimoTotal' : '$vlMinimoTotal',
                'vlMaximoUnitario': '$vlMaximoUnitarioitem',
                'vlMaximoTotal' : '$vlMaximoTotal',
                'dsItem' : '$dsItem',
                'dsFormaPagamento' : '$dsFormaPagamento',
                'dsTipoEntregaProduto': '$dsTipoEntregaProduto',
                'nrQuantidadePropostaLicitacao' : '$nrQuantidadePropostaLicitacao',
                'vlPropostaItem' : '$vlPropostaItem',
                'dtValidadeProposta' : '$dtValidadeProposta',
                'dtPrazoEntregaPropostaLicitacao' : '$dtPrazoEntregaPropostaLicitacao',
                'nrClassificacao' : '$nrClassificacao',
                'dtHomologacao' : '$dtHomologacao',
                'nrQuantidadeVencedor' : '$nrQuantidadeVencedorLicitacao',
                'vlUnitarioVencedor' : '$vlLicitacaoVencedorLicitacao',
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
            count += 1
            # docs_updated.append(result.raw_result)
        # return count, docs_updated
        return count

        
    

if __name__ == "__main__":
    p = ProcessData()
    p.process()
    