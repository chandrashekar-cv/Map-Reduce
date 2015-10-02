import MapReduce
import sys

"""
Single phase matrix sqaure
"""

mr = MapReduce.MapReduce()
# =============================
# Do not modify above this line

def mapper(record):

    for k in range(0,5,1):
        #[i, j, A(i,j)] is the input
        #to emit {(i,k) , (A,i,j, A(i,j))}
        #k is the number of columns in the result matrix
        i_index=record[0]
        j_index=record[1]
        mr.emit_intermediate(tuple([i_index,k]),{tuple(['A',i_index,j_index]):record[2]})

    for i in range(0,5,1):
        #[j, k, B(j,k)] is the input
        #to emit {(i,k) , (B,j,k, B(j,k))}
        #i is the number of rows in the result matrix
        k_index=record[1]
        j_index=record[0]
        mr.emit_intermediate(tuple([i,k_index]),{tuple(['B',j_index,k_index]):record[2]})

        #Since this is a soln to find square of a matrix, iterators of A and B are combined into one.

def reducer(key, list_of_values):
    index = list(key)
    i = index[0]
    k = index[1]
    values ={}
    for v in list_of_values:
        values.update(v)
    sum = 0
    for j in range(0,5,1):
        a = values.get(tuple(['A',i,j]),0)
        b = values.get(tuple(['B',j,k]),0)
        sum += (a * b)

    mr.emit([i,k,sum])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
