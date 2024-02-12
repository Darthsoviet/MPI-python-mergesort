import numpy as np
import sys
import MPIParallelDistributedMergeSort 


def main(arg):

   array_size = int(arg[len(arg)-1])
   data = np.random.randint(100,100000,size=array_size)
   sorter = MPIParallelDistributedMergeSort.MPIParallelDistriutedMergeSort()
   res = sorter.sort(data)
   if(res!= None):
      print(res)
main(sys.argv)
