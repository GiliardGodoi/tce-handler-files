from pymongo import MongoClient
import pprint

def get_db():
    client = MongoClient("mongodb://localhost:27017")
    db = client['raw_data']
    return db

def process():
    db = get_db()
    lsIdPessoa = db['rawLicitacao'].distinct('idPessoa')
    lsAno = db['rawLicitacao'].distinct('nrAnoLicitacao')
    for idPessoa in lsIdPessoa:
        print('idPessoa', idPessoa)
        process_item_licitacao(idPessoa)
    
def process_item_licitacao(idPessoa):
    match = { '$match' : { 'idPessoa' : idPessoa } }
    project = { '$project' : {'cdIBGE' : 0, 'idPessoa' : 0, 'nmEntidade' : 0,'nrAnoLicitacao' : 0, 'nrLicitacao': 0, 'dsModalidadeLicitacao': 0,'idUnidadeMedida' : 0 ,'idTipoEntregaProduto': 0,'DataReferencia' : 0, 'ultimoEnvioSIMAMNesteExercicio' : 0 }}
    group = { '$group' : {
        '_id' : '$idlicitacao',
        'item' : { '$push' : {
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
            'dsTipoEntregaProduto': '$dsTipoEntregaProduto'
        }},
        'itemVencedor' : { '$push' : {
            'fornecedor' : '$nmPessoa',
            'nrDocumento' : '$nrDocumento',
            'nrLote' :'$nrLote',
            'nrItem' : '$nrItem',
            'nrQuantidadePropostaLicitacao' : '$nrQuantidadePropostaLicitacao',
            'vlPropostaItem' : '$vlPropostaItem',
            'dtValidadeProposta' : '$dtValidadeProposta',
            'dtPrazoEntregaPropostaLicitacao' : '$dtPrazoEntregaPropostaLicitacao',
            'nrQuantidadeVencedorLicitacao' : '$nrQuantidadeVencedorLicitacao',
            'vlUnitarioVencedor' : '$vlLicitacaoVencedorLicitacao',
            'nrClassificacao' : '$nrClassificacao',
            'vlTotalVencedor' : '$vlTotalVencedorLicitacao'
        }}
    }}

    pipeline = [match, project, group]
    db = get_db()
    count = 0
    result =  db.rawLicitacaoVencedor.aggregate(pipeline)
    pprint.pprint(result.next())
        
    

if __name__ == "__main__":
    process()