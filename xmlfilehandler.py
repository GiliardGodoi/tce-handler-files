from validators import Validator
# from db import MyMongoDBInstance as DB
from db import MyFileSaveOption as DB

class Construtor():
    def criar(self, tipo):
        tiposArquivos = { 'Empenho': EmpenhoValidator, 'EmpenhoLiquidacao': EmpenhoLiquidacaoValidator,
         'EmpenhoLiquidacaoEstorno': EmpenhoLiquidacaoEstornoValidator, 
         'EmpenhoLiquidacaoDocumentoFisc' : EmpenhoLiquidacaoDocumentoFiscValidator,
         'EmpenhoPagamento': EmpenhoPagamentoValidator,'EmpenhoPagamentoEstorno': EmpenhoPagamentoEstornoValidator,
         'Contrato' : ContratoValidator, 'ContratoAditivo' : ContratoAditivoValidator,
         'ContratoAditivoCessao' : ContratoAditivoCessaoValidator, 'ContratoAditivoPrazo' : ContratoAditivoPrazoValidator,
         'ContratoAditivoRedimensionamen' : ContratoAditivoRedimensionamenValidator,
         'ContratoAditivoRescisao' : ContratoAditivoRescisaoValidator,
         'Licitacao' : LicitacaoValidator, 'LicitacaoParticipante' : LicitacaoParticipanteValidator,
         'LicitacaoVencedor' : LicitacaoVencedorValidator
        }
        if tipo in tiposArquivos:
            return tiposArquivos[tipo]()
        else:
            raise TypeError("Nao pode ser definido 'validator' para o tipo especificado: "+tipo)
        

class EmpenhoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = [ "vlEmpenho", "vlSaldoAntDotacao", "vlLiquidacao", "vlPagamento" ]
        self.intFields = []
        self.db = DB()
        
    def valide(self, data):
        print('validando empenho')
    
    def save(self,data):
        pass

class EmpenhoLiquidacaoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlLiquidacaoBruto", "vlLiquidacaoEstornado", "vlLiquidacaoLiquido"]

class EmpenhoLiquidacaoEstornoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlEstorno"]
    
class EmpenhoLiquidacaoDocumentoFiscValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlDocumento"]

class EmpenhoPagamentoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlOperacao", "vlPagamentoBruto", "vlPagamentoEstornado", "vlPagamentoLiquido"]

class EmpenhoPagamentoEstornoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlEstorno"]

class ContratoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlContrato", "vlDesteContratado"]

class ContratoAditivoValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["vlAditivo", "vlAtualizadoContrato"]


class ContratoAditivoCessaoValidator(Validator):
    def __init__(self):
        super().__init__()

class ContratoAditivoPrazoValidator(Validator):
    def __init__(self):
        super().__init__()

class ContratoAditivoRedimensionamenValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ['nrQuantidade','vlAditivoItem']

class ContratoAditivoRescisaoValidator(Validator):
    def __init__(self):
        super().__init__()
        


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
        self.schema = {'nranoEditalOrigem' : 'nrAnoEditalOrigem'}

    def valide(self, data):
        print('validando licitacao')
        
class LicitacaoParticipanteValidator(Validator):
    def __init__(self):
        super().__init__()

class LicitacaoVencedorValidator(Validator):
    def __init__(self):
        super().__init__()
        self.floatFields = ["nrQuantidade" , "vlMinimoUnitarioItem", "vlMinimoTotal", "vlMaximoUnitarioitem",
        "vlMaximoTotal", "nrPrazoLimiteEntrega", "nrQuantidadePropostaLicitacao", "vlPropostaItem",
        "nrQuantidadeVencedorLicitacao", "vlLicitacaoVencedorLicitacao"]

class ConvenioValidator(Validator):
    def __init__(self):
        super().__init__()
    
    def valide(self, data):
        print('validando convenio')

class RelacionamentoValidator(Validator):
    def __init__(self):
        super().__init__()