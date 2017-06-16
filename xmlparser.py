import xml.etree.ElementTree as ET
from xmlfilehandler import Construtor

class XMLParser():
    def __init__(self):
        self.construtor = Construtor()
        self.xml_type = None
    
    def process_xml_file(self, xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()
        self._define_xml_type(xml_file)
        lenght = len(root)
        limit = 500 if 500 < lenght else lenght
        skip = 0; padding = limit
        print('file: ',xml_file,'nodes: ',lenght,'type: ',self.xml_type)
        while(skip < lenght):
            self._process_root_xml(root[skip:limit])
            skip = limit
            limit = (limit + padding) if (limit + padding) < lenght else lenght

    def _process_root_xml(self, nodes):
        data =  []
        for n in nodes:
            data.append(self.xml_type.valide(n.attrib))
        self.xml_type.save(data)
    
    def _define_xml_type(self,xml_file):
        start = xml_file.rfind('_')
        end = xml_file.rfind('.xml')
        start += 1
        xml_type = xml_file[start:end]
        self.xml_type = self.construtor.criar(xml_type)
        return True