import os
import sys
from os import walk
from mpi4py import MPI

def master_function(master, workers, comm):
    if len(sys.argv) == 3:
        test_files_location = sys.argv[1]
        result_files_location = sys.argv[2]
    else:
        test_files_location = "test-files"
        result_files_location = "result"
    inter_files_location = "inter-files"
    if not os.path.exists(os.path.join(os.getcwd(), result_files_location)):
        os.makedirs(os.path.join(os.getcwd(), result_files_location))

    if not os.path.exists(os.path.join(os.getcwd(), inter_files_location)):
        os.makedirs(os.path.join(os.getcwd(), inter_files_location))


    # mapping

    input_file_names = next(walk(test_files_location), (None, None, []))[2]
    stop_index = 0
    for i in range(0, master):
        files_per_process = int(len(input_file_names) / workers)
        if len(input_file_names) % workers > 0:
            if i < len(input_file_names) % workers:
                start_index = stop_index
                files_per_process += 1
                stop_index = start_index + files_per_process
            else:
                start_index = stop_index
                stop_index = start_index + files_per_process
        else:
            start_index = stop_index
            stop_index = start_index + files_per_process
        print("Process - " + str(i) + "\tStart: " + str(start_index)
              + "\tStop: " + str(stop_index) + "\tTotal: " + str(files_per_process))

        comm.send([test_files_location, input_file_names[start_index:stop_index]], dest=i, tag=0)
    for i in range(0, master):
        command = comm.recv(source=MPI.ANY_SOURCE, tag=0)


    # reducing

    inter_files_names = next(walk(inter_files_location), (None, None, []))[2]
    stop_index = 0
    for i in range(0, master):
        files_per_process = int(len(inter_files_names) / workers)
        if len(inter_files_names) % workers > 0:
            if i < len(inter_files_names) % workers:
                start_index = stop_index
                files_per_process += 1
                stop_index = start_index + files_per_process
            else:
                start_index = stop_index
                stop_index = start_index + files_per_process
        else:
            start_index = stop_index
            stop_index = start_index + files_per_process
        print("Process - " + str(i) + "\tStart: " + str(start_index)
              + "\tStop: " + str(stop_index) + "\tTotal: " + str(files_per_process))

        comm.send([result_files_location, inter_files_names[start_index:stop_index]], dest=i, tag=0)

    for i in range(0, master):
        command = comm.recv(source=MPI.ANY_SOURCE, tag=0)

