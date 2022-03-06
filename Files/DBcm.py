import mysql.connector as mysql


class UseDatabase:
    """
       This is a class to automate the opening and closing of a connection to a mysql server
       using Python.

       It will return a cursor that can be used to execute script.

       Attributes:
           config (dict): Please provide in the below format
           config = {'host': 'localhost', 'user': 'test',
          'password': 'testpasswd', 'database': 'houseprices'}
       """
    def __init__(self, config: dict) -> None:
        self.config = config

    def __enter__(self):
        self.conn = mysql.connect(**self.config)
        self.cursor = self.conn.cursor(buffered=True)
        return self.cursor

    def __exit__(self, exc_type, exc_value, exc_trace) -> None:
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
