import re

class Validator():
    def __init__(self):
        self.intFields = []
        self.floatFields = []
        self.schema = []
    
    def valide(self, registro):
        return registro
    
    def is_decimal(self,string): # testa por expressao regular se a string possui o foramto 0000.0000
        match_object = re.fullmatch(r'\d+\.?\d*',string)
        if match_object :
            result = True if match_object.group() == string else False
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


    def _strip_field(self,registro,chave):
        if type(registro[chave]) is str :
            registro[chave] = registro[chave].strip()
        return registro
    
    def _convert_to_decimal(self, registro, chave):
        if self.is_decimal(registro[chave]) :
            registro[chave] = self._to_decimal(registro[chave])

