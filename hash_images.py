import requests
import time
import certifi
import elasticsearch
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
        self.elasticsearch = Elasticsearch(url, timeout=120, verify_certs=True, ca_certs=certifi.where())

        self.INDEX = 'memex-domains'
        self.DOC_TYPE = 'escorts'
        self.SCROLL_TIME = '60m'
        self.SEARCH_TYPE = 'scan'

        self.query_builder = QueryBuilder()

    def hash_images(self, scroll_id=None, scroll_count=0, records_processed=0):
        """
        Will generate hash for each image
        :param scroll_id: current scroll id
        :param scroll_count: current amount of scrolls accessed
        :param records_processed: current amount of records accessed
        :return:
        """
        # If we don't have a scroll id then this is the first call and an initial search must be done
        if not scroll_id:
            query = {'filter': {'bool': {'must': {'term': {'content_type': ['image', 'jpeg']}}}}}

            page = self.elasticsearch.search(index=self.INDEX,
                                             doc_type=self.DOC_TYPE,
                                             scroll=self.SCROLL_TIME,
                                             search_type=self.SEARCH_TYPE,
                                             body=query)
        else:
            try:
                page = self.elasticsearch.scroll(scroll_id=scroll_id, scroll=self.SCROLL_TIME)
            except elasticsearch.ConnectionTimeout as e:
                print 'We have a timeut error'
                print e
                self.query_builder.update('scroll_info', {'status': 'open'})
                self.run()
            except elasticsearch.TransportError as e:
                print 'We have a transport error'
                print e
                self.query_builder.update('scroll_info', {'status': 'open'})
                self.run()
            except elasticsearch.ConnectionError as e:
                print 'We have a connection error'
                print e
                self.query_builder.update('scroll_info', {'status': 'open'})
                self.run()
            except elasticsearch.NotFoundError as e:
                print 'We have a not found error'
                print e
                self.query_builder.update('scroll_info', {'status': 'open'})
                self.run()
            except elasticsearch.RequestError as e:
                print 'We have a request error'
                print e
                self.query_builder.update('scroll_info', {'status': 'open'})
                self.run()
            except elasticsearch.ElasticsearchException as e:
                print 'We have some other error'
                print e
                self.query_builder.update('scroll_info', {'status': 'open'})
                self.run()



        # Increment the scroll_count
        scroll_count += 1

        # The hits array will be empty when there is nothing else to scroll
        if page['hits']:
            # Update the records processed with the amount of records in the scroll
            records_processed += len(page['hits']['hits'])
            # Write the new scroll before we start processing so it can be picked up
            self.query_builder.update('scroll_info', {'scroll': page['_scroll_id'],
                                                      'scrolls_processed': scroll_count,
                                                      'records_processed': records_processed,
                                                      'status': 'open'})
            # This process is timing out so I'm going to continue where we left off
            hashes = []
            hits = page['hits']['hits']
            for hit in hits:
                try:
                    image_url = hit.get('_source').get('obj_stored_url')
                except AttributeError:
                    # Very rare error if happens just skip the record
                    continue
                if image_url:
                    http_error_retries = 0
                    http_timeout_retries = 0
                    # If we have retried 3 times for either reason then let's stop
                    while http_error_retries < 3 and http_timeout_retries < 3:
                        try:
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
                        except requests.HTTPError:
                            # Http errors are rare, if they happen increment 1 to the http_retry and try again
                            http_error_retries += 1
                        except requests.exceptions.Timeout:
                            # Increment the retry and try again
                            http_timeout_retries += 1
                        except:
                            # Some other request error occurred. Save black result
                            hash_dict = {'hash': None,
                                         'parent': hit.get('_source').get('obj_parent'),
                                         'url': hit.get('_source').get('obj_original_url'),
                                         'timestamp': hit.get('_source').get('timestamp'),
                                         'stored_url': hit.get('_source').get('obj_stored_url'),
                                         'doc_id': hit.get('_id')}
                            hashes.append(hash_dict)
            self.query_builder.insert('image_hashes4', values=hashes, bulk=True)

        # If there are no hits the page is empty and we are done
        else:
            self.query_builder.update('scroll_info', {'scroll': 'done',
                                                      'scrolls_processed': scroll_count,
                                                      'records_processed': records_processed,
                                                      'status': 'open'})

    def run(self):
        """
        Runs the hash images
        :return:
        """
        while True:
            # Try to open the database and close the scroll
            where = 'WHERE status = \'open\''
            rows_updated = self.query_builder.update('scroll_info', values={'status': 'closed'}, where=where)
            while rows_updated == 0:
                # Wait five second and try again
                print 'The scroll is in use, waiting 5 seconds and trying again'
                time.sleep(5)
                rows_updated = self.query_builder.update('scroll_info', values={'status': 'closed'}, where=where)

            row = self.query_builder.select('scroll_info')[0]
            # This is the first record
            if row['scroll'] == 'start':
                self.hash_images()
            # We are done in this case
            elif row['scroll'] == 'done':
                exit(0)
            # We are in the middle of processing (most likely case)
            else:
                self.hash_images(scroll_id=row['scroll'],
                                 scroll_count=int(row['scrolls_processed']),
                                 records_processed=int(row['records_processed']))

HashImages().run()


