import os
import time


def reducer_function(rank, comm, master):
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