
import csv
import hashlib
from queue import Queue

from xtract_sdk


class ExtractorRunner:
    def __init__(csv_path, mode=single, extractor=None, mdata_dir='/home/tskluzac/'):
        
        self.proc_queue = Queue()

        reader = None
        with open(csv_path, 'r') as f:
            reader = csv.reader(f)

            next(reader)
            for line in reader:
                path_to_extract = line[0]
                path_hash = hashlib.md5(path_to_extract.encode())
                
                proc_tuple = (path_to_extract, path_hash)
                self.proc_queue.put(proc_tuple)
        print(f"Queue loaded with {self.proc_queue.qsize()} groups!") 


    def run_extractor_on_everything():
