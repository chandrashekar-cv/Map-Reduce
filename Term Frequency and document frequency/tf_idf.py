import MapReduce
import sys,re

"""
Term Frequency and document frequency
"""

mr = MapReduce.MapReduce()
regex = re.compile("[a-zA-Z0-9_]+")
# =============================
# Do not modify above this line

def mapper(record):
    key = record[0]
    words = record[1].strip().split()
    for w in words:
        w = w.lower()
        if not(regex.match(w) is None):
            value= [1, key]
            mr.emit_intermediate(w, value)

def reducer(key, list_of_values):
    docDict = {}
    docFreq=0
    for record in list_of_values:
        if(docDict.get(record[1]) is None):
            docDict.setdefault(record[1],1)
            docFreq+=1
        else:
            docDict[record[1]] += record[0]

    tfList = list()
    for docKey in docDict.keys():
        tfList.append([docKey,docDict.get(docKey)])

    output=[None] * 3
    output[0]=key
    output[1] = docFreq
    output[2] = tfList
    mr.emit(output)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
