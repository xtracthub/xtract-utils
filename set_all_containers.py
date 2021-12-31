
import os
import shutil 

base_path = "/home/tskluzac/ext_repos"
dest_path = "/home/tskluzac/.xtract/.containers"

all_dirs = os.listdir(base_path)
all_sing_conts = []
all_sing_dests = []

for dir_name in all_dirs:
    if dir_name.startswith("xtract-"):
        inner_path = os.path.join(base_path, dir_name)
        all_files = os.listdir(inner_path)
        for file_name in all_files:
            if file_name.endswith(".sif") or file_name.endswith(".img"):
                full_file_path = os.path.join(inner_path, file_name)
                all_sing_conts.append(full_file_path)
                new_dest = full_file_path.replace(inner_path, dest_path)
                all_sing_dests.append(new_dest)

for i in range(0, len(all_sing_dests)):
    source = all_sing_conts[i]
    dest = all_sing_dests[i]

    print(f"Moving container from {source} to {dest}")
    shutil.move(source, dest)
    
