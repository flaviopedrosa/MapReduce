import mincemeat  #Juliana e Flavio
import os
import codecs
import csv

folder = 'C:\\Temp\\Exerc23\\Metadados\\'
arquivos = os.listdir(folder)
source = []

for i, arquivo in enumerate(arquivos):
    filename = os.path.join(folder, arquivo)
    arq = open(filename, encoding='utf-8')
    source.append(arq.read())
    arq.close()
    
datasource = dict(enumerate(source))
#import pdb; pdb.set_trace()    
def mapfn(k, v):
    print("processando map:{}".format(k))
    from stopwords import allStopWords
    
    for line in v.split('\n'):
        if line:            
            fields = line.split(':::')
            authors = fields[1].split('::')
            words = fields[2].split(' ')
            for author in authors:
                for word in words:
                    if word not in allStopWords.keys():                        
                        yield (author, word)

def reducefn(k, vs):
    print("processando reduce:{}".format(k))
    from collections import Counter
    result = Counter(vs)
    return result            

s = mincemeat.Server()

s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

search = ["Grzegorz Rozenberg", "Philip S. Yu", "Alexander Tuzhilin"]
autores = [autores for autores in results.keys()]
new_ark = open("C:\\Temp\\Exerc23\\resultado.csv", "w")

for author in autores:
    if author in results.keys():
        for k, v in results[author].items():
            new_ark.write("\"{}\";\"{}\";\"{}\"\n".format(author, k, v))
            
        
        amount = [value for value in results[author].values()]
        amount.sort()
        max_values = amount[-2:]
       
        

new_ark.close()        
        




#w = csv.writer(open("C:\\Temp\\Exerc23\\resultado.csv", "w"))

