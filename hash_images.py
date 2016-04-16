import requests
import csv
import time
from elasticsearch import Elasticsearch
from hashlib import sha1
from query_builder import QueryBuilder


class HashImages:
    def __init__(self):
        """
        Create the elasticsearch and query builder instances
        :return:
        """
        # Create session to set the maximum retries to 3
        self.session = requests.Session()
        self.http_adapter = requests.adapters.HTTPAdapter(max_retries=3)
        self.https_adapter = requests.adapters.HTTPAdapter(max_retries=3)
        self.session.mount('http://', self.http_adapter)
        self.session.mount('https://', self.https_adapter)

        # Create elasticsearch instance
        url = 'https://memex:qRJfu2uPkMLmH9cp@els.istresearch.com:19200/'
        self.elasticsearch = Elasticsearch(url, timeout=10000)

        self.INDEX = 'memex-domains'
        self.DOC_TYPE = 'escorts'
        self.SCROLL_TIME = '10m'
        self.SEARCH_TYPE = 'scan'

        self.query_builder = QueryBuilder()

    def hash_images(self, filename, scroll_id=None, scroll_count=0, records_processed=0):
        """
        Will generate hash for each image
        :param filename: location of file used to save the scrolls
        :param scroll_id: current scroll id
        :param scroll_count: current amount of scrolls accessed
        :param records_processed: current amount of records accessed
        :return:
        """
        # If we don't have a scroll id then this is the first call and an initial search must be done
        if not scroll_id:
            query = {'query': {'match_all': {}}}
            page = self.elasticsearch.search(index=self.INDEX,
                                             doc_type=self.DOC_TYPE,
                                             scroll=self.SCROLL_TIME,
                                             search_type=self.SEARCH_TYPE,
                                             body=query)
        else:
            page = self.elasticsearch.scroll(scroll_id=scroll_id, scroll=self.SCROLL_TIME)

        # Increment the scroll_count
        scroll_count += 1

        # The hits array will be empty when there is nothing else to scroll
        if page['hits']:
            # Update the records processed with the amount of records in the scroll
            records_processed += len(page['hits']['hits'])
            # Write the new scroll before we start processing so it can be picked up
            self.write_file(filename, {'scroll_id': page['_scroll_id'],
                                       'number_of_scrolls': scroll_count,
                                       'number_of_records': records_processed,
                                       'status': 'open'})
            hashes = []
            hits = page['hits']['hits']
            for hit in hits:
                try:
                    image_url = hit.get('_source').get('obj_stored_url')
                    if image_url:
                        http_error_retries = 0
                        http_timeout_retries = 0
                        # If we have retried 3 times for either reason then let's stop
                        while http_error_retries < 3 and http_timeout_retries < 3:
                            image = requests.get(image_url, timeout=6)
                            h = sha1()
                            h.update(image.text.encode('utf8'))
                            hash_dict = {'hash': h.hexdigest(),
                                         'parent': hit.get('_source').get('obj_parent'),
                                         'url': hit.get('_source').get('obj_original_url'),
                                         'timestamp': hit.get('_source').get('timestamp'),
                                         'stored_url': image_url,
                                         'doc_id': hit.get('_id')}
                            hashes.append(hash_dict)
                            break
                except requests.exceptions.HTTPError:
                    # Http errors are rare, if they happen increment 1 to the http_retry and try again
                    http_error_retries += 1
                except requests.exceptions.Timeout:
                    # Increment the retry and try again
                    http_timeout_retries += 1
                except requests.exceptions.RequestException:
                    # Some other request error occurred. Save black result
                    hash_dict = {'hash': None, 'parent': None, 'url': image_url}
                    hashes.append(hash_dict)
                    break
            self.query_builder.insert('image_hashes', values=hashes, bulk=True)
        # If there are no hits the page is empty and we are done
        else:
            self.write_file(filename, {'scroll_id': 'done',
                                       'number_of_scrolls': scroll_count,
                                       'number_of_records': records_processed,
                                       'status': 'open'})

    def read_file(self, filename):
        """
        Will read the file used to keep the scroll information and return the information as a dictionary
        :param filename: location of file that has the scroll information
        :return: the information from the file as a dictionary
        """
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                return row

    def write_file(self, filename, row):
        """
        Will write to the file with the scroll information
        :param filename: location of file that has the scroll information
        :param row: dictionary of information to write to file
        :return:
        """
        with open(filename, 'w') as csvfile:
            fieldnames = ['scroll_id', 'number_of_scrolls', 'number_of_records', 'status']

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(row)

    def run(self, filename):
        """
        Runs the hash images
        :param filename:
        :return:
        """
        while True:
            # First read the file
            row = self.read_file(filename)
            while row['status'] == 'closed':
                # Wait five seconds and try again
                time.sleep(5)
                row = self.read_file(filename)

            # If we make it here then we are about to start processing the scroll, close the file from reading
            row['status'] = 'closed'
            self.write_file(filename, row)

            # This is the first record
            if row['scroll_id'] == 'start':
                self.hash_images(filename)
            # We are done in this case
            elif row['scroll_id'] == 'done':
                exit(0)
            # We are in the middle of processing (most likely case)
            else:
                self.hash_images(filename,
                                 scroll_id=row['scroll_id'],
                                 scroll_count=int(row['number_of_scrolls']),
                                 records_processed=int(row['number_of_records']))

HashImages().run('scroll_keeper.csv')
