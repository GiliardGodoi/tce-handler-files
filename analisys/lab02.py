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
        }}