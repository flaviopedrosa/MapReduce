import mincemeat  #Juliana e Flávio
import glob
import csv

text_files = glob.glob('C:\\Temp\\Exerc23\\Metadados\\*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name, file_contents(file_name))for file_name in text_files)

def mapfn(k, v):
    for w in v.split():
        yield w, 1

def reducefn(k, vs):
    result = sum(vs)
    return result

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

search = ["Grzegorz Rozenberg", "Philip S. Yu"]

w = csv.writer(open("C:\\Temp\\Exerc23\\resultado.csv", "w"))

for author in sorted(results.iterkeys()):
    if(filter.count > 0 and author in filter):
        w.write(str(author) + " -> ")
        #apenas as duas palavras que mais aparecem
        for word in results[author][:2]:
            word_count = word[0] + ":" + str(word[1]) + "  "
            w.write(word_count)
        w.write("\n")