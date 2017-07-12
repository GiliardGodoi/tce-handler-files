# -*- coding: utf-8 -*-
import time
import pprint
from pymongo import MongoClient
from bson.objectid import ObjectId

class ProcessData():
    '''
        ROTINA PARA PREPARAÇÃO DOS DADOS

        CAMINHO ROTA
        /sinopseLicitacao

        MÉTODO: GET
        PARÂMETROS:
        cdIBGE=[cdIBGE]&nrAno=[nrAno]
        
        OBJETIVO:
        Fornecer a quantidade de procedimentos realizados durante um determinado ano. 
        Fornecer, ainda, dados sobre
        valor total licitado e o valor total efetivamente adquirido. Atenção para essa diferenciação.

        MODELO DE DADOS
        <array>
        [{
            cdIBGE : <string>,
            nmMunicipio : <string>,
            nmEntidade : <string>,
            nrAnoLicitacao : <string>,
            cdEntidade :<string>,
            sinopse : [
                {
                    dsModalidadeLicitacao : <string>,
                    vlAnualTotalLicitado : <string>,
                    vlAnualTotalAdquirido : <string>,
                }
            ]
        }
        , ... ]
    '''
    def __init__(self):
        self._db = None

    def get_db(self):
        if not self._db:
            client = MongoClient("mongodb://localhost:27017")
            self._db = client['raw_data']
        return self._db

    def execute(self):
        lstCodEntidade = self.determinar_entidades_para_execucao()
        anos = ['2013', '2014', '2015', '2016']
        print('Executando para as entidades:\n',lstCodEntidade)
        for entidade in lstCodEntidade:
            for ano in anos:
                pipe = self.determinar_pipeline(cdEntidade=entidade,nrAno=ano)
                cursor = self.executar_consulta(pipe)
                aux = self.complemento_informacao_entidade(cd_entidade=entidade,nr_ano=ano)
                nro = self.salvar_resultado(cursor=cursor,doc=aux)
                print('numero documentos aggregate: ',nro)
            

    def determinar_entidades_para_execucao(self):
        ''' determinar os codigos das entidades que serao processadas '''
        return self.get_db()['rawLicitacao'].distinct('cdEntidade')

    def determinar_pipeline(self, cdEntidade = None, nrAno = None):
        ''' Controi o pipeline de acordo com os dados recebeidos '''
        match = { '$match' : {'cdEntidade' : cdEntidade, 'nrAnoLicitacao' : nrAno}}
        group = { '$group' : { 
            '_id' : '$dsModalidadeLicitacao',
            'vlAnualTotalAdquirido' : { '$sum' : '$vlTotalAdquiridoLicitacao' },
            'vlAnualTotalLicitado' : { '$sum' : '$vlLicitacao' },
            'nrQuantidadeProcedimento' : {"$sum" : 1}
        }}
        pipeline = [match, group]
        return pipeline
    
    def complemento_informacao_entidade(self,cd_entidade,nr_ano):
        ''' complementa informacao'''
        query = {'cdEntidade' : cd_entidade, 'nrAnoLicitacao' : nr_ano}
        project = {"_id" : 0, 'cdIBGE' : 1, 'nmMunicipio' : 1, 'nmEntidade' : 1, 'cdEntidade' : 1, 'nrAnoLicitacao' : 1}
        aux = self.get_db().rawLicitacao.find_one(query,project)
        # pprint.pprint(aux)
        print('entidade',cd_entidade,'nrAno',nr_ano)
        return aux

    def executar_consulta(self,pipeline = None):
        ''' retornar um cursor '''
        if not pipeline:
            raise AttributeError('Deve ser forneceido um pipeline')
        cursor = self.get_db().rawLicitacao.aggregate(pipeline)
        return cursor

    def salvar_resultado(self, cursor,doc=None):
        if not doc:
            return 0
        DB = self.get_db()
        doc['_id'] = ObjectId()
        doc['sinopse'] = []
        count = 0
        for item in cursor:
            item['dsModalidadeLicitacao'] = item['_id']
            del item['_id']
            doc['sinopse'].append(item)
            count += 1
        DB.sinopseLicitacao.insert_one(doc)
        return count



if __name__ == "__main__":
    P = ProcessData()
    print('Iniciando execucao: ')
    INI = time.time()
    P.execute()
    FIM = time.time()
    print('tempo de execução: ', str(FIM-INI))