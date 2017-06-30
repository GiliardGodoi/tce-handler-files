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
        cdMunicipios = self.get_db().rawLicitacao.distinct('cdIBGE')
        for cod in cdMunicipios :
            self.agrupar_fornecedor_por_cidade(cod) 

    def agrupar_fornecedor_por_cidade(self,cdIBGE):
        match = {"$match" : {'cdIBGE' : cdIBGE}}
        group = { "$group" : {
            "_id" : "$nrDocumento",
            "nmFornecedor" : {"$addToSet" : {
                "nome" : "$nmPessoa",
            }},
            "nrQuantidadeProcedimento" : { "$sum" : 1},
            "vlTotalAdquiridoParticipante" : {"$sum": "$vlTotalVencedorLicitacao" },
            "participacoes" : { "$addToSet" : {
                "idlicitacao" : "$idlicitacao",
                "nrLicitacao" : "$nrLicitacao",
                "nrAnoLicitacao" : "$nrAnoLicitacao",
                "dsModalidadeLicitacao" : "$dsModalidadeLicitacao",
                "vlAdquirido" : {"$sum" : "$vlTotalVencedorLicitacao" }
            }}
        }}
        out = {"$out" : "listaFornecedores"}
        pipeline = [match, group, out]
        db = self.get_db()
        db.rawLicitacaoVencedor.aggregate(pipeline)
        

if __name__ == "__main__":
    p = ProcessData()
    p.process()