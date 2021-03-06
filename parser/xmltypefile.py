from validators import Validator


class XMLTypeFactory():
    def criar(self, tipo):
        tiposArquivos = { 'Empenho': EmpenhoValidator, 'EmpenhoLiquidacao': EmpenhoLiquidacaoValidator,
         'EmpenhoLiquidacaoEstorno': EmpenhoLiquidacaoEstornoValidator, 
         'EmpenhoLiquidacaoDocumentoFisc' : EmpenhoLiquidacaoDocumentoFiscValidator,
         'EmpenhoPagamento': EmpenhoPagamentoValidator,'EmpenhoPagamentoEstorno': EmpenhoPagamentoEstornoValidator,
         'Contrato' : ContratoValidator, 'ContratoAditivo' : ContratoAditivoValidator,
         'ContratoAditivoCessao' : ContratoAditivoCessaoValidator, 'ContratoAditivoPrazo' : ContratoAditivoPrazoValidator,
         'ContratoAditivoRedimensionamen' : ContratoAditivoRedimensionamenValidator,
         'ContratoAditivoSubContratacao' : ContratoAditivoSubContratacaoValidator,
         'ContratoAditivoRescisao' : ContratoAditivoRescisaoValidator,
         'Licitacao' : LicitacaoValidator, 'LicitacaoParticipante' : LicitacaoParticipanteValidator,
         'LicitacaoVencedor' : LicitacaoVencedorValidator,
         'ContratoXConvenio' : ContratoXConvenioValidator,
         'EmpenhoXContrato' : EmpenhoXContratoValidator, 'LicitacaoXContrato':LicitacaoXContratoValidator,
         'LicitacaoXConvenio' : LicitacaoXConvenioValidator, 'LicitacaoXEmpenho' :LicitacaoXEmpenhoValidator
        }
        if tipo in tiposArquivos:
            return tiposArquivos[tipo]()
        else:
            raise TypeError("Nao pode ser definido 'validator' para o tipo especificado: "+tipo)
        

class EmpenhoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = [ "vlEmpenho", "vlSaldoAntDotacao", "vlLiquidacao", "vlPagamento" ]
        self.collection_name = 'rawEmpenho'

class EmpenhoLiquidacaoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlLiquidacaoBruto", "vlLiquidacaoEstornado", "vlLiquidacaoLiquido"]
        self.collection_name = 'rawEmpenhoLiquidacao'

class EmpenhoLiquidacaoEstornoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlEstorno"]
        self.collection_name = 'rawEmpenhoLiquidacaoEstorno'
    
class EmpenhoLiquidacaoDocumentoFiscValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlDocumento"]
        self.collection_name = 'rawEmpenhoLiquidacaoDocumentoFisc'

class EmpenhoPagamentoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlOperacao", "vlPagamentoBruto", "vlPagamentoEstornado", "vlPagamentoLiquido"]
        self.collection_name = 'rawEmpenhoPagamento'

class EmpenhoPagamentoEstornoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlEstorno"]
        self.collection_name = 'rawEmpenhoPagamentoEstorno'

class ContratoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlContrato", "vlDesteContratado"]
        self.collection_name = 'rawContrato'

class ContratoAditivoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlAditivo", "vlAtualizadoContrato"]
        self.collection_name = 'rawContratoAditivo'
        self.schema = [{"old" : "idcontrato", "new" : "idContrato"}]
    
    def valide(self, registro):
        novo = super().valide(registro)
        return super()._ensure_schema_names(novo)
        
class ContratoAditivoCessaoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawContratoAditivoCessao'
        self.schema = [{"old" : "idcontrato", "new" : "idContrato"}]
    
    def valide(self, registro):
        novo = super().valide(registro)
        return super()._ensure_schema_names(novo)

class ContratoAditivoPrazoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawContratoAditivoPrazo'
        self.schema = [{"old" : "idcontrato", "new" : "idContrato"}]

    def valide(self, registro):
        novo = super().valide(registro)
        return super()._ensure_schema_names(novo)

class ContratoAditivoRedimensionamenValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ['nrQuantidade','vlAditivoItem']
        self.collection_name = 'rawContratoAditivoRedimensionamen'
        self.schema = [{"old" : "idcontrato", "new" : "idContrato"}]
    
    def valide(self, registro):
        novo = super().valide(registro)
        return super()._ensure_schema_names(novo)

class ContratoAditivoRescisaoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawContratoAditivoRescisao'
    
    def valide(self, registro):
        novo = super().valide(registro)
        return super()._ensure_schema_names(novo)

class ContratoAditivoSubContratacaoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawContratoAditivoSubContratacao'
    
    def valide(self, registro):
        novo = super().valide(registro)
        return super()._ensure_schema_names(novo)
        
class LicitacaoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = [ "vlLicitacao", "nrQuantidade", 
                    "vlMinimoUnitarioItem",
                    "vlMinimoTotal",
                    "vlMaximoUnitarioitem",
                    "vlMaximoTotal",
                    "nrQuantidadePropostaLicitacao",
                    "vlPropostaItem",
                    "nrQuantidadeVencedorLicitacao",
                    "vlLicitacaoVencedorLicitacao" ]
        self.schema = [{ 'old' : 'nranoEditalOrigem' , 'new' : 'nrAnoEditalOrigem'},
                       {'old' : 'idPessoa', 'new' : 'cdEntidade' } ]
        self.collection_name = 'rawLicitacao'
    
    def valide(self, registro):
        novo = super().valide(registro)
        return super()._ensure_schema_names(novo)
        
            
class LicitacaoParticipanteValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawLicitacaoParticipante'

class LicitacaoVencedorValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["nrQuantidade" , "vlMinimoUnitarioItem", "vlMinimoTotal", "vlMaximoUnitarioitem",
        "vlMaximoTotal", "nrPrazoLimiteEntrega", "nrQuantidadePropostaLicitacao", "vlPropostaItem",
        "nrQuantidadeVencedorLicitacao", "vlLicitacaoVencedorLicitacao"]
        self.collection_name = 'rawLicitacaoVencedor'
        self.schema = [{'old' : 'idPessoa', 'new' : 'cdEntidade' }, {'old' : 'nmPessoa' , 'new' : 'nmFornecedor'}]

    def valide(self,registro):
        novo_registro = super().valide(registro)
        novo_registro = super()._ensure_schema_names(novo_registro)
        novo_registro['vlTotalVencedorLicitacao'] = self.multiplicar_valor_monetario(novo_registro["vlLicitacaoVencedorLicitacao"], novo_registro["nrQuantidadeVencedorLicitacao"])
        return novo_registro
    
    def multiplicar_valor_monetario(self, value1, value2):
        return round((value1 * value2),2)

class ConvenioValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlRecursoProprio", "vlConvenio"]
        self.collection_name = 'rawConvenio'

class ContratoXConvenioValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawContratoXConvenio'

class EmpenhoXContratoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawEmpenhoXContrato'

class LicitacaoXContratoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawLicitacaoXContrato'

class LicitacaoXConvenioValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawLicitacaoXConvenio'

class LicitacaoXEmpenhoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.collection_name = 'rawLicitacaoXEmpenho'
