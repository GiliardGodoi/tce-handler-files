import os

tipos = {}

conjunto = []

def write(file, num):
    print(('  ' * num) +file)

def determinar_info_arquivo(file):
    i = file.rfind('_')
    ixml = file.find('.xml')
    i += 1
    t = file[i:ixml]
    return t

def _add(tipo,file):
    if tipo in tipos:
        tipos[tipo]['files'] += [file]
        tipos[tipo]['count'] += 1
    else:
        tipos[tipo] = { 'files' : [file], 'count' : 1 }

def descrever(folder, nivel = 0):
    os.chdir(folder)
    write(folder,nivel)
    files = os.listdir(os.getcwd())
    for f in files :
        if(os.path.isfile(f) and f.endswith('.xml')):
            t = determinar_info_arquivo(f)
            _add(t,f)
            write(f,nivel+10)
        elif(os.path.isdir(f)):
            new_folder = os.path.join(os.getcwd(), f)
            n = nivel + 1
            descrever(new_folder, n)
            os.chdir(folder)


if __name__ == '__main__':
    start = os.getcwd()
    descrever(start)
    for t in tipos.keys():
        print(t,'\t',tipos[t])