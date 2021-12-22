
import argparse
import csv
from random import randint


def create_new_csv(filename, old_path, new_path):

    # To make everything the same, make sure everything has a trailing slash
    if not new_path.endswith("/"):
        new_path = new_path + "/"
    if not old_path.endswith("/"):
        old_path = old_path + "/"

    with open(filename, 'r') as f:
        reader = csv.reader(f)
        new_filename = f"{filename}-UPDATE-{randint(100,999)}.csv"

        num_lines = 0
        for line in reader:
            num_lines += 1
            the_path = line[0]
            # print(the_path)
            replaced_path = the_path.replace(old_path, new_path)
            line[0] = replaced_path

            with open(new_filename, 'a') as g:
                writer = csv.writer(g)
                writer.writerow(line)
            if num_lines % 10000 == 0:
                print(f"Lines completed: {num_lines}")

    return "Done"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filename", required=True)
    parser.add_argument("-o", "--old_path", required=True)
    parser.add_argument("-n", "--new_path", required=True)
    args = parser.parse_args()

    crawl_obj = create_new_csv(filename=args.filename, old_path=args.old_path, new_path=args.new_path)
    print(crawl_obj)