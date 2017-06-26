import xml.etree.ElementTree as ET
from xmltypefile import XMLTypeFactory

class XMLParser():
    def __init__(self):
        self.construtor = XMLTypeFactory()
        self.xml_type = None

    def process_xml_file(self, xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()
        self._define_xml_type(xml_file)
        length = len(root)
        nr_saved = self._process_root_xml(root)
        print(xml_file,length, nr_saved)
        assert length == nr_saved
    
    def process_xml_file_interval(self, xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()
        self._define_xml_type(xml_file)
        length = len(root)
        limit = 500 if 500 < length else length
        skip = 0; padding = limit
        print('file: ',xml_file,'nodes: ',length,'type: ',self.xml_type)
        while(skip < length):
            self._process_root_xml(root[skip:limit])
            skip = limit
            limit = (limit + padding) if (limit + padding) < length else length

    def _process_root_xml(self, nodes):
        data =  []
        for n in nodes:
            data.append(self.xml_type.valide(n.attrib))
        nro = self.xml_type.save(data)
        return nro
    
    def _define_xml_type(self,xml_file):
        start = xml_file.rfind('_')
        end = xml_file.rfind('.xml')
        start += 1
        xml_type = xml_file[start:end]
        self.xml_type = self.construtor.criar(xml_type)
        return True