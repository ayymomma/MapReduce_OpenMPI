import os
import sys
import time
import re
from os import walk
from mpi4py import MPI

from mapper import mapper_function
from master import master_function
from reducer import reducer_function

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
master = size - 1
workers = size - 1

if __name__ == "__main__":
    if rank == master:
        master_function(master, workers, comm)
    else:
        mapper_function(rank, comm, master)
        reducer_function(rank, comm, master)
