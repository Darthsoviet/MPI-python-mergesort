from typing import Any
from mpi4py import MPI

class MPIParallelDistriutedMergeSort:


   def partitionate_array(self ,array:list, parts:int) -> list:
      res = []
      chunk_size = len(array)//parts
      mod = len(array)%parts

      
      begin=0
      for _ in range(parts):
         if mod!=0:
            end=begin+chunk_size+1
            res.append(array[begin : end ])
            mod=mod-1
         else:
            end=begin+chunk_size
            res.append(array[begin : end])
         begin = end

      return res

   def split(self,data) -> list:
      length = len(data) 
      mid = length // 2
      if length > 1:
         left = self.split(data[:mid])
         rigth = self.split(data[mid:])
         return self.merge(left,rigth)
      else: 
         return data




   def merge(self, left, rigth) -> list:
      res = []
      l= len(left)
      r = len(rigth)
      i = j = 0
      while i < l and j < r:    
         if left[i] < rigth[j] :
            res.append(left[i])
            i+=1
         else:
            res.append(rigth[j])
            j+=1
      while i < l:
         
         res.append(left[i])
         i+=1
      while j < r:

         res.append(rigth[j])
         j+=1

         
      return res


   def sort(self, array) -> (list | Any | None):

      comm = MPI.COMM_WORLD
      size = comm.Get_size()
      rank = comm.Get_rank()
      ROOT = 0
      if rank == ROOT:
         partitioned_data = self.partitionate_array(array,size)
      else:
         partitioned_data = []

      local_scattered_data = comm.scatter(partitioned_data)

      local_sorted_data = self.split(local_scattered_data)

      global_merged_data =  comm.reduce(local_sorted_data,op=self.merge) 

      return global_merged_data
