# -*- coding: utf-8 -*-
from pymongo import MongoClient

class ProcessData():
    '''
    ROTINA PARA PREPARAÇÃO DOS DADOS

    CAMINHO ROTA
    modalidadePorTipoAvaliacao/
    
    MÉTODO: GET
    
    PARÂMETROS:
    &cdIBGE
    &nrAno

    OBJETIVO:
    Relacionar as modalidades de licitação com as formas de avaliação, para cada município em cada ano

    MODELO DE DADOS:
    {
        nmMunicipio : <string>,
        cdIBGE : <string>,
        nrAno : <string>,
        sinopse : [
            dsModalidade : <string>,
            dsCriterioAvaliacao : <string>,
            nrQuantidadeProcedimento : <inteiro>
        ]
    }
    '''
    def __init__(self):
        self._db = None
        self._db_name = 'raw_data'


    def get_db(self):
        ''' me '''
        if not self._db:
            client = MongoClient("mongodb://localhost:27017")
            self._db = client[self._db_name]
        return self._db
    
    def execute(self):
        lst_cod_municipios = self.determinar_municipios_para_execucao()
        lst_nr_ano = self.determinar_nr_ano()
        print('processar: \n',lst_cod_municipios,'\n',lst_nr_ano)
        for cd_ibge in lst_cod_municipios:
            nm_municipio = self.determinar_nome_municipio(cd_ibge=cd_ibge)
            for ano in lst_nr_ano:
                print('executando consulta para: ', nm_municipio, 'ano : ',ano)
                pipe = self.determinar_pipeline(cd_ibge=cd_ibge,nr_ano=ano)
                cursor = self.executar_consulta(pipe)
                nro = self.salvar_consulta(cursor=cursor,cd_ibge=cd_ibge,nm_municipio=nm_municipio,nr_ano=ano)
    
    def determinar_municipios_para_execucao(self):
        ''' determina para quais municipios serão feita as pesquisas '''
        ls = self.get_db()['rawLicitacao'].distinct('cdIBGE')
        return ls

    def determinar_nr_ano(self):
        ls = self.get_db()['rawLicitacao'].distinct('nrAnoLicitacao')
        return ls

    def determinar_pipeline(self,cd_ibge = None, nr_ano = None):
        if not (cd_ibge and nr_ano):
            raise AttributeError('cd_ibge or nr_ano do not must be empty')
        match = { '$match' : { 'cdIBGE' : cd_ibge, 'nrAnoLicitacao' : nr_ano} }
        project = { '$project' : { '_id' : 0, 'dsModalidadeLicitacao' : 1, 'dsAvaliacaoLicitacao' : 1}}
        group = { '$group' : {
            '_id' : {'dsModalidade' : '$dsModalidadeLicitacao', 'dsAvaliacao' : '$dsAvaliacaoLicitacao'},
            'contagem' : { '$sum' : 1 }
        }}
        pipeline = [match, project, group]
        return pipeline

    def executar_consulta(self, pipeline = None):
        if not pipeline:
            raise AttributeError('atribute do not must be empty')
        cursor = self.get_db().rawLicitacao.aggregate(pipeline)
        return cursor

    def determinar_nome_municipio(self,cd_ibge = None):
        if not cd_ibge:
            raise AttributeError('cd_ibge empty')
        obj = self.get_db().rawLicitacao.find_one({'cdIBGE' : cd_ibge}, {'_id' : 0, 'nmMunicipio' : 1})
        if obj :
            return obj['nmMunicipio']
        else :
            return None

    def salvar_consulta(self, cursor = None,cd_ibge = None, nm_municipio = None, nr_ano = None):
        if not (cursor and cd_ibge and nm_municipio and nr_ano):
            raise AttributeError('attr empty')
        doc = dict()
        doc['nmMunicipio'] = nm_municipio
        doc['nrAno'] = nr_ano
        doc['cdIBGE'] = cd_ibge
        doc['sinopse'] = []
        for item in cursor:
            obj = dict()
            obj['dsModalidade'] = item['_id']['dsModalidade']
            obj['dsCriterioAvaliacao'] = item['_id']['dsAvaliacao']
            obj['nrQuantidadeProcedimento'] = item['contagem']
            doc['sinopse'].append(obj)
        db = self.get_db()
        db.sinopseCriterioAvaliacaoPorModalidade.insert_one(doc)
        return 1
        
if __name__ == "__main__":
    p = ProcessData()
    p.execute()