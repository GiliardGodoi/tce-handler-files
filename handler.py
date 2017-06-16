import os
import stdio
from xmlparser import XMLParser


class Handler():

    def __init__(self, start = os.getcwd() ):
        self.startFolder = start
        self.parser = XMLParser()
    
    def start(self):
        self.file_handler(self.startFolder)

    def write(file, nivel):
        stdio.writeln('{}{}'.format(('  '*nivel),file))

    def file_handler(self, folder):
        os.chdir(folder)
        files = os.listdir(os.getcwd())
        for f in files:
            if(os.path.isfile(f) and f.endswith('.xml') ):
                self.parser.process_xml_file(f)
            elif(os.path.isdir(f)):
                new_folder = os.path.join(os.getcwd(), f)
                self.file_handler(new_folder)
                os.chdir(new_folder)

if __name__ == "__main__":
    h = Handler()
    h.start()