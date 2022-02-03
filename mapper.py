import os
import re
import time


def mapper_function(rank, comm, master):
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
            except Exception as err:
                print(file + "\t" + err)
    print(f" <{rank}> finished mapping")
    comm.send("FINISHED", dest=master, tag=0)