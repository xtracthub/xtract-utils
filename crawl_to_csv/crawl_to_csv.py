
import os
import csv
import json
import time
import boto3
import argparse
import threading
from queue import Queue


"""
We want to do a many-threaded pull-down of a crawl and make it available as CSV file. 
"""


class CrawlToCSV:
    def __init__(self, crawl_id):
        self.crawl_id = crawl_id
        self.files_to_write = Queue()

        self.files_pulled = 0
        self.files_written = 0

        self.num_reader_threads = 20

        self.client = boto3.client('sqs',
                                   aws_access_key_id=os.environ["aws_access"],
                                   aws_secret_access_key=os.environ["aws_secret"],
                                   region_name='us-east-1')

        response = self.client.get_queue_url(QueueName=f'crawl_{self.crawl_id}',
                                             QueueOwnerAWSAccountId='576668000072')  # TODO: env variable

        self.sqs_qname = response["QueueUrl"]

        self.kill = False

    def writer_thread(self):
        filename = f"crawl_{self.crawl_id}.csv"
        if os.path.isfile(filename):
            print("PATH ALREADY EXISTS. IMPLODING...")
            self.kill = True
            exit()

        with open(f"crawl_{self.crawl_id}.csv", "w") as g:
            csv_writer = csv.writer(g)

            field_names = ['path', 'crawl_timestamp', 'size']

            csv_writer.writerow(field_names)
            while True:
                if not self.files_to_write.empty():
                    file_obj = self.files_to_write.get()
                    path = file_obj['path']
                    crawl_timestamp = file_obj['crawl_timestamp']
                    file_size = file_obj['size']
                    csv_writer.writerow([path, crawl_timestamp, file_size])
                    self.files_written += 1
                    # PICK APART THE COMPONENT
                else:
                    print("[Writer] Can't find files. Sleeping for 5s...")
                    time.sleep(5)

    def pull_from_sqs_thr(self):

        while True:

            # This means we would be overwriting a new file.
            if self.kill:
                print("[SQS Poll] Thread will not overwrite file. Imploding...")
                break

            del_list = []
            try:
                sqs_response = self.client.receive_message(
                    QueueUrl=self.sqs_qname,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=1)
            except Exception as e:
                print(f"Exception caught: {e}. Sleeping 5s")
                time.sleep(5)
                continue

            for message in sqs_response["Messages"]:
                message_body = message["Body"]

                del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                                 'Id': message["MessageId"]})

                mdata = json.loads(message_body)

                files = mdata['files']
                crawl_timestamp = mdata['metadata']['crawl_timestamp']

                filename_size_map = dict()
                for group in mdata['groups']:
                    for file_obj in group['files']:
                        path = file_obj['path']
                        file_size = file_obj['metadata']['physical']['size']
                        filename_size_map[path] = file_size

                for file in files:
                    file['crawl_timestamp'] = crawl_timestamp
                    file['size'] = filename_size_map[file['path']]

                for file in files:
                    self.files_to_write.put(file)
                    self.files_pulled += 1

                    print(f"Files pulled:\t{self.files_pulled}")
                    print(f"Files written:\t{self.files_written}")

                # TODO: bring this back when we're ready to go live.
                if len(del_list) > 0:
                    self.client.delete_message_batch(
                        QueueUrl=self.sqs_qname,
                        Entries=del_list)

    def crawl_to_csv(self):
        # Step 1: start writer thread
        w_thr = threading.Thread(target=self.writer_thread, args=())
        print("Starting writer thread...")
        w_thr.start()

        print(f"Wait for an exit signal in case of file overwrite...")
        time.sleep(5)

        for i in range(0, self.num_reader_threads):
            r_thr = threading.Thread(target=self.pull_from_sqs_thr, args=())
            r_thr.start()
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--crawl_id", required=True)
    args = parser.parse_args()

    crawl_obj = CrawlToCSV(crawl_id=args.crawl_id)
    crawl_obj.crawl_to_csv()
