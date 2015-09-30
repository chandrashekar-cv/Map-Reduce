import MapReduce
import sys,re

"""
Frequent item sets.
"""

mr = MapReduce.MapReduce()
# =============================
# Do not modify above this line

def mapper(record):
    items = record
    for i in range(0,len(items)-1,1):
        for j in range(i+1,len(items)-1,1):
            set = tuple([record[i],record[j]])
            mr.emit_intermediate(set,1)

def reducer(key, list_of_values):
    total = 0
    for v in list_of_values:
        total+=v

    if(total>=100):
        mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
