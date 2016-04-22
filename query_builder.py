import psycopg2
import psycopg2.extras

class QueryBuilder():
    def __init__(self):
        """self.config = configs

        self.connection_string = (
                            'dbname=' + self.config['psql_db'] + ' ' +
                            'user=' + self.config['psql_username'] + ' ' +
                            'host=' + self.config['psql_host'] + ' ' +
                            'password=' + self.config['psql_password'] + ' ' +
                            'port=' + self.config['psql_port']
                            )"""

        self.connection_string = 'dbname=memex_images user=memex_user host=memex-images.cpld6ftkyyoj.us-gov-west-1.rds.amazonaws.com password=memex0010 port=5432'

        self.database_connection = psycopg2.connect(self.connection_string)
        self.database_connection.autocommit = True
        self.cursor = self.database_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def insert(self, table, values, bulk = False):
        if values:
            # If it's bulk then we use the first values as our keys, otherwise we can use the values passed
            if bulk:
                use_values = values[0]
            else:
                use_values = values

            # Start the insert statement
            statement = 'INSERT INTO ' + table + ' '
            statement += '('

            # Place each key from the dictionary as the column name to insert
            for key, value in use_values.iteritems():
                statement += key + ', '

            statement = statement[:-2]
            statement += ') '

            # If we do a bulk insert we have to get the values from each dictionary and save them with a unique key
            if bulk:
                row = 0 # Row is used to append to the key name to make it unique
                insert_values = {}
                statement += 'VALUES '

                for current_values in values:
                    current_dict = {}
                    statement += '('

                    for key, value in current_values.iteritems():
                        # Save the values to a dictionary for use later
                        current_dict[key + str(row)] = current_values[key]
                        statement += '%(' + key + str(row) + ')s,'

                    row += 1
                    # Take the values we just saved and save them to the dictionary to be passed with the sql statement
                    insert_values.update(current_dict)
                    statement = statement[:-1]
                    statement += '),'

                statement = statement[:-1]

            else:
                statement += 'VALUES ('

                for key, value in values.iteritems():

                    if value:
                        statement += '%(' + key + ')s,'

                statement = statement[:-1]
                statement += ')'

            statement += ' RETURNING id'

            try:
                if bulk:
                    self.cursor.execute(statement, insert_values)
                else:
                    self.cursor.execute(statement, values)
                return self.cursor.fetchone()['id']
            except Exception as e:
                print 'There was an error inserting the rows'
                print e
                self.database_connection.rollback()

    def select(self, table, columns = None, where = None, order_by = None, limit = None, offset = None):
        statement = 'SELECT '

        if columns:
            for column in columns:
                statement += column + ', '
            statement = statement[:-2]
            statement += ' '
        else:
            statement += '* '

        statement += 'FROM ' + table + ' '
        if where:
            statement += where + ' '
        if order_by:
            statement += order_by + ' '
        if limit:
            statement += 'LIMIT ' + str(limit) + ' '
        if offset:
            statement += 'OFFSET ' + str(offset) + ' '

        self.cursor.execute(statement)
        return self.cursor.fetchall()

    def update(self, table, values, where = None):
        statement = ('UPDATE ' + table + ' SET ')

        for key, value in values.iteritems():
            statement += key + '=%(' + key + ')s,'

        statement = statement[:-1]

        if where:
            statement += ' ' + where
        try:
            self.cursor.execute(statement, values)
            return self.cursor.rowcount
        except Exception as e:
            print 'There was an error updating rows'
            print e
            self.database_connection.rollback()