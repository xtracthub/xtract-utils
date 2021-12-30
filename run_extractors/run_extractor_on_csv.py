
import os
import csv
import json
import hashlib
from queue import Queue
from copy import deepcopy
from argparse import ArgumentParser
from threading import Thread

from xtract_sdk.agent.xtract import XtractAgent


def create_fam_batch(files, fam_id, parser=None):
        # mock_event = dict()

        fam_batch = FamilyBatch()

        test_fam_1 = Family()
        group_file_objs = []

        for file in files:
            base_path = file
            group_file_objs.append({'path': base_path, 'metadata': dict()})
            test_fam_1.download_type = "LOCAL"
        test_fam_1.family_id = fam_id
        test_fam_1.add_group(files=group_file_objs, parser=parser)
        fam_batch.add_family(test_fam_1)
        # mock_event['family_batch'] = fam_batch
        return fam_batch

def base_extractor(event):
    # import json
    # from xtract_sdk.agent.xtract import XtractAgent

    # Load endpoint configuration. Init the XtractAgent.
    xtra = XtractAgent(ep_name=event['ep_name'],
                       xtract_dir=event['xtract_dir'],
                       sys_path_add=event['sys_path_add'],
                       module_path=event['module_path'],
                       recursion_depth=event['recursion_limit'],
                       metadata_write_path=event['metadata_write_path'])

    # Execute the extractor on our family_batch.

    try: 
    	xtra.execute_extractions(family_batch=event['family_batch'], input_type=event['type'])

    	# All metadata are held in XtractAgent's memory. Flush to disk!
    	paths = xtra.flush_metadata_to_files(writer=event['writer'])
    	stats = xtra.get_completion_stats()
    	stats['mdata_paths'] = paths
    except: 
        pass

    return "COMPLETE"


def get_event_template(files, mode, dir_name):

    event = {'ep_name': 'foobar', 
             'xtract_dir': '~/.xtract/.containers', 
             'sys_path_add': '/',
             'module_path': f'xtract_{extractor}_main', 
             'recursion_limit': 5000, 
             'metadata_write_path': dir_name, 
             'writer': 'json'}

    if mode == 'single':
        # Should grab the hash for a family_id
        event['family_batch'] = create_fam_batch(files, files[1])

    else:
        raise NotImplementedError("Need to support multi-file groups")



class ExtractorRunner:
    def __init__(self, csv_path, mode, extractor, mdata_dir):
        
        self.num_threads = 1
        self.proc_queue = Queue()
        self.mdata_dir = mdata_dir

        reader = None
        with open(csv_path, 'r') as f:
            reader = csv.reader(f)

            next(reader)
            for line in reader:
                path_to_extract = line[0]
                path_hash = hashlib.md5(path_to_extract.encode())
                path_size = line[1]

                proc_tuple = (path_to_extract, path_hash, path_size)
                self.proc_queue.put(proc_tuple)
        print(f"Queue loaded with {self.proc_queue.qsize()} groups!") 

        for i in range(0, self.num_threads):
            thr = Threading.thread(target=run_extractor_on_everything, args=(i,))
            thr.start()

    


    def run_extractor_on_everything(self, thread_id):
        print(f"Started extractor thread: {thread_id}")

        while True: 
            if self.proc_queue.empty():
                print(f"Queue is empty in thread {thread_id}... BREAKING!") 
                break

            thrup = self.proc_queue.get()
            fpath, fhash, fsize = thrup

            event = deepcopy(get_event_template(thrup, mode, self.mdata_dir))
            auto_write = False

            t0 = time.time()
            try: 
                result = base_extractor(event)
            except Exception as e: 
                auto_write = True
                print('oy')







if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-c', '--csv')
    parser.add_argument('-m', '--mode')
    parser.add_argument('-d', '--dir')
    parser.add_argument('-x', '--extractor')

    args = parser.parse_args()
    er = ExtractorRunner(args.csv, args.mode, args.extractor, args.dir)
