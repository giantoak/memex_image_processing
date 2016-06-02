import datetime
from query_builder import QueryBuilder

class OpenScroll:
    def __init__(self):
        self.query_builder = QueryBuilder()

    def check_scroll(self):
        start_time = datetime.datetime.now()
        while True:
            columns = ['status']
            where = 'WHERE status = \'closed\''
            status = self.query_builder.select('scroll_info', columns=columns, where=where)
            if status:
                elapsed_time = datetime.datetime.now() - start_time
                if elapsed_time > datetime.timedelta(minutes=5):
                    print 'Opening scroll due to inactivity'
                    self.query_builder.update('scroll_info', {'status': 'open'})
                    start_time = datetime.datetime.now()
            else:
                start_time = datetime.datetime.now()

OpenScroll().check_scroll()