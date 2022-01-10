
import os
import json
from argparse import ArgumentParser


def find_missing_families(top_num, dir):
    all_dir_items = os.listdir(dir)
    missing_ls = []

    all_dir_dict = dict()
    for item in all_dir_items:
        all_dir_dict[str(item)] = "BALDERDASH"

    for i in range(0, top_num+1):
        cur_fid = str(i)
        if cur_fid not in all_dir_dict:
            missing_ls.append(cur_fid)

    print("Writing missing files to JSON...")
    with open('missing_files.json', 'w') as f:
        json.dump({'missing_ls': missing_ls, 'num_missing': len(missing_ls)}, f)
    print("Completed!")


if __name__=="__main__":
    parser = ArgumentParser()
    parser.add_argument('--top_num', '-n')
    parser.add_argument('--dir', '-d')
    args = parser.parse_args()

    find_missing_families(args.top_num, args.dir)
