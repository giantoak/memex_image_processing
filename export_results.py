import csv
from query_builder import QueryBuilder

class ExportResults:
    def __init__(self):
        self.query_builder = QueryBuilder()

    def export(self, amount_of_records, export_file):
        limit = 1000
        index = 0
        export_results = []

        while (limit * index) < amount_of_records:
            results = self.query_builder.select('image_hashes2', order_by='ORDER BY id', limit=limit, offset=limit*index)
            for result in results:
                export_results.append(result)

            index += 1

        with open(export_file, 'wb') as f:
            fieldnames = sorted(list(set(k for d in export_results for k in d)))
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            w.writeheader()
            w.writerows(export_results)
            f.close()

ExportResults().export(100000, 'hashed_images.csv')

