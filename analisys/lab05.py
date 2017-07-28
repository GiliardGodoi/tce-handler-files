# -*- coding: utf-8 -*-
import time
from pymongo import MongoClient
from bson.objectid import ObjectId

class ProcessData():
    '''
        Rotina para preparação dos dados
        CAMINHO ROTA:
        rankingFornecedor/
        
        MÉTODO: GET

        PARÂMETROS:
        ?cdIBGE=[cdIBGE]&nrAno=[nrAno]&typeRanking=[tipoRanking]&limit=[nrlimit]&sort=[asc || desc]
        tipoRanking = valor || presenca

        OBJETIVO
        Para determinado ano e Municipio, determinar lista  das empresas:
        com maiores valores contratados
        
        MODELO DE DADO

        cdIBGE : <string>,
        nmMunicipio : <string>,
        fornecedor : {
            nmFornecedor : <string>,
            nrDocumento : <string>,
            vlContratado : <double>
        }
    '''
    def __init__(self):
        self._db = None
        self._db_name = 'raw_data'
        self.output_collection = 'rankingFornecedor'


    def get_db(self):
        ''' me '''
        if not self._db:
            client = MongoClient("mongodb://localhost:27017")
            self._db = client[self._db_name]
        return self._db
    
    def execute(self):
        anos = ['2013', '2014', '2015', '2016']
        municipios = self.determinar_municipios_para_execucao()
        for cdIBGE in municipios:
            for ano in anos:
                pipe = self.determinar_pipeline(cdIBGE=cdIBGE,nrAno=ano)
                cursor = self.executar_consulta(pipe)
                self.salvar_resultado(cursor=cursor,cdIBGE=cdIBGE,nrAno=ano)

    def determinar_municipios_para_execucao(self):
        ''' retorna uma lista dos municipios para processamento
        da para melhorar
        '''
        db = self.get_db()
        return db.rawLicitacaoVencedor.distinct('cdIBGE')
    
    def determinar_pipeline(self,cdIBGE = None, nrAno = None, typeRanking = None, limit = None, sortType = None ):
        ''' Constroi o pipeline com base nos parametros passados '''
        if not cdIBGE:
            raise AttributeError('cdIBGE deve ser informado')
        if not nrAno:
            raise AttributeError('nrAno deve ser informado')
        # if not typeRanking in ['valor', 'presenca'] :
        #     raise AttributeError('typeRanking deve ser valor ou presenca')
        match = {"$match" : {"cdIBGE" : cdIBGE, "nrAnoLicitacao" : nrAno} }
        group = { "$group" : {
            "_id" : {"nrDocumento" : "$nrDocumento","nmFornecedor" : "$nmFornecedor"},
            "vlContratado" : { "$sum" : "$vlTotalVencedorLicitacao" }
        }}
        sort = { "$sort" : {"vlContratado" : -1 }}

        pipeline = [match, group, sort]
        return pipeline


    def executar_consulta(self, pipeline):
        db = self.get_db()
        cursor = db['rawLicitacaoVencedor'].aggregate(pipeline)
        return cursor
    
    def salvar_resultado(self, cursor, output_collection = None, cdIBGE = None, nrAno = None):
        if not output_collection:
            output_collection = self.output_collection
        if not (cdIBGE and nrAno):
            raise AttributeError('aux deve fornecer informacoes sobre a cdIBGE e ano')
        db = self.get_db()
        for doc in cursor:
            obj = {}
            obj['_id'] = ObjectId()
            obj['cdIBGE'] = cdIBGE
            obj['nrAno'] = nrAno
            
            obj['nmFornecedor'] = doc['_id']['nmFornecedor']
            obj['nrDocumento'] = doc['_id']['nrDocumento']
            obj['vlContratado'] = doc['vlContratado']
            db[output_collection].insert_one(obj)


if __name__ == "__main__":
    P = ProcessData()
    print('Iniciando execucao: ')
    INI = time.time()
    P.execute()
    FIM = time.time()
    print('tempo de execução: ', str(FIM-INI))