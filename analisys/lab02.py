'''
    
'''
from pymongo import MongoClient

class ProcessData():
    '''
        ROTINA PARA PREPARAÇÃO DOS DADOS

        CAMINHO ROTA
        licitacao/:idLicitacao
                
        OBJETIVO: Acrescenta o campo nrTotalParticipante e lsParticipante nos documentos da coleção raw Licitacao
        a partir dos dados da coleção rawLicitacaoParticipante.
        
        MODELO DE DADOS
        {
            "nrTotalParticipante" : <inteiro>,
            "lsParticipante" : {
                "nmFornecedor" : <string>,
                "sgDocumentoForncedor" : <string>,
                "nrDocumentoFornecedor" : <string>
            } 
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
        lstCodIBGE = db.rawLicitacao.distinct('cdIBGE')
        print('Executando para:\n',lstCodIBGE)
        for cod in lstCodIBGE:
            cursor = self.agrupar_fornecedores_por_idlicitacao(cod)
            count = self.adicionar_participante(cursor)
            print('cdIBGE: ', cod,' documentos atualizados: ', count)

    def agrupar_fornecedores_por_idlicitacao(self, cdIBGE):
        match = {"$match" : {"cdIBGE" : cdIBGE }}
        group = { "$group" : {
            "_id" : "$idLicitacao",
            "nrTotalParticipante" : {"$sum" : 1},
            "lsParticipante" : { "$push" : {
                "nmFornecedor" : "$nmParticipanteLicitacao",
                "sgDocumentoForncedor" : "$sgDocParticipanteLicitacao",
                "nrDocumentoFornecedor" : "$nrDocParticipanteLicitacao" 
            } } 
        }}
        db = self.get_db()
        cursor = db.rawLicitacaoParticipante.aggregate([match, group])
        return cursor

    def adicionar_participante(self, cursor = None):
        if not cursor:
            raise AttributeError('cursor not valid')
        db = self.get_db()
        count = 0
        for doc in cursor:
            atualizar = {"participante" : {"nrTotalParticipante" : doc["nrTotalParticipante"], "lsParticipante" : doc['lsParticipante']}}
            resultado = db.rawLicitacao.update_one({'idLicitacao' : doc['_id']}, {"$set" : atualizar })
            if resultado:
                count +=1 
        return count


if __name__ == "__main__":
    p = ProcessData()
    p.process()