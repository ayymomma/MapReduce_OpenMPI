import os
import sys
import time
import re
from os import walk
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
master = size - 1
workers = size - 1


def mapper_function():
    print(f" <{rank}> started mapping")
    data = comm.recv(source=master, tag=0)
    inter_files_location = "inter-files"
    test_files_location = data[0]
    files = data[1]
    for file in files:
        with open(os.path.join(test_files_location, file), errors="ignore") as f:
            try:
                text = f.read()
                words = re.findall(r"[\w']+", text)
                for word in words:
                    while True:
                        try:
                            with open(os.path.join(inter_files_location, word.lower()[0] + ".txt"), 'a') as word_file:
                                word_file.write(f"< {word.lower()} , {file} >\n")
                            break
                        except:
                            time.sleep(0.01)
            except:
                print(file)
    print(f" <{rank}> finished mapping")
    comm.send("FINISHED", dest=master, tag=0)


def reducer_function():
    print(f" <{rank}> started reducing")
    data = comm.recv(source=master, tag=0)
    inter_files_location = "inter-files"
    result_files_location = data[0]
    result_file_name = "result.txt"
    files = data[1]

    words_count = {}
    for file in files:
        with open(os.path.join(inter_files_location, file), errors="ignore") as f:
            lines = f.readlines()
            for line in lines:
                try:
                    word = line.split()[1]
                    source_file = line.split()[3]

                    if word in words_count:
                        if source_file in words_count[word]:
                            words_count[word][source_file] += 1
                        else:
                            words_count[word][source_file] = 1
                    else:
                        words_count[word] = {source_file: 1}
                except:
                    print(file)
    while True:
        try:
            with open(os.path.join(result_files_location, result_file_name), 'a') as output_file:

                for word in words_count:
                    output = f"<{word}, " + "{ "
                    for source_file in words_count[word]:
                        output += f"{source_file}: {words_count[word][source_file]}, "
                    output = output[:-2] + "} >\n"
                    output_file.write(output)
            break
        except:
            time.sleep(0.01)

    print(f" <{rank}> finished reducing")
    comm.send("FINISH", dest=master, tag=0)


def master_function():
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


if __name__ == "__main__":
    if rank == master:
        master_function()
    else:
        mapper_function()
        reducer_function()
