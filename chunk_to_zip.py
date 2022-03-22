
import os
from shutil import move
from math import ceil
from argparse import ArgumentParser


def pack_into_folders(datadir, num, write_base_path):
    all_items = os.listdir(datadir)

    all_full_folders = ceil(len(all_items) / num)
    partial_folders = 0
    if len(all_items) % num != 0:
        partial_folders = 1

    total_folders = all_full_folders + partial_folders

    dir_storage = []
    for i in range(0, total_folders):
        new_dir = os.path.join(write_base_path, str(i))
        os.makedirs(new_dir)
        dir_storage.append(new_dir)

    file_count = 0
    dir_index = 0
    for item in all_items:
        file_count += 1
        move(item,  dir_storage[dir_index])

        if file_count % num == 0:
            dir_index += 1

        if file_count % 25000 == 0:
            print(file_count)


if __name__=="__main__":
    parser = ArgumentParser()
    parser.add_argument('--num', '-n')
    parser.add_argument('--source_dir', '-s')
    parser.add_argument('--dest_dir_base', '-d')
    args = parser.parse_args()

    pack_into_folders(args.soure_dir, args.num, args.dest_dir_base)