import MapReduce
import sys

"""
Two phase matrix sqaure - Phase 1
"""

mr = MapReduce.MapReduce()
# =============================
# Do not modify above this line

def mapper(record):
    i_index=record[0]
    j_index=record[1]
    mr.emit_intermediate(j_index,{tuple(['A',i_index]):record[2]})

    k_index=record[1]
    j_index=record[0]
    mr.emit_intermediate(j_index,{tuple(['B',k_index]):record[2]})


def reducer(key, list_of_values):
    A_values={}
    B_values={}
    for v in list_of_values:
        temp = list(v.keys()[0])
        if temp[0]=='A':
            A_values.update(v)
        else:
            B_values.update(v)

    for A_Key in A_values:
        i = list(A_Key)[1]
        for B_Key in B_values:
            k= list(B_Key)[1]
            value = A_values.get(A_Key,0) * B_values.get(B_Key,0)
            mr.emit([i,k,value])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
