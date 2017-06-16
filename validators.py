import re
# from db import MyMongoDBInstance as DB
from db import MyFileSaveOption as DB

class Validator():
    def __init__(self):
        self.db = DB()
        self.collection_name = 'teste'
        self.intFields = []
        self.floatFields = []
        self.schema = []
    
    def valide(self, registro):
        novo = {}
        for chave in list(registro.keys()):
            if type(registro[chave]) is str :
                novo[chave] = registro[chave].strip()
            if chave in self.floatFields :
                novo[chave] = self._convert_to_decimal(registro[chave])
        return novo

    def save(self,data):
        self.db.save(data,self.collection_name)

    def is_decimal(self,string): # testa por expressao regular se a string possui o foramto 0000.0000
        match_object = re.fullmatch(r'\d+\.?\d*',string)
        if match_object :
            result = True if match_object.group() else False
            return result
        else :
            return False

    def _to_decimal(self,value):
        try:
            number = float(value)
            return number
        except ValueError:
            raise ValueError('Nao foi possivel converter a string '+value)
    
    def _to_int(self, value):
        try:
            number = int(value)
            return number
        except ValueError:
            raise ValueError("Nao foi possivel converter o valor para int: "+ value)

    def _strip(self,novo_obj, registro,chave):
        if type(registro[chave]) is str :
            novo_obj[chave] = registro[chave].strip()
        return novo_obj
    
    def _convert_to_decimal(self, value):
        # if self.is_decimal(value) :
        #     return self._to_decimal(value)
        # else:
        #     return value
        return self._to_decimal(value)

