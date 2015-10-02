import MapReduce
import sys

"""
Two phase matrix sqaure - Phase 2
"""

mr = MapReduce.MapReduce()
# =============================
# Do not modify above this line

def mapper(record):
    i_index = record[0]
    k_index = record[1]
    value = record[2]
    mr.emit_intermediate(tuple([i_index,k_index]),value)


def reducer(key, list_of_values):
    key = list(key)
    i=key[0]
    j=key[1]
    sum = 0
    for v in list_of_values:
        sum += v

    mr.emit([i,j,sum])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
