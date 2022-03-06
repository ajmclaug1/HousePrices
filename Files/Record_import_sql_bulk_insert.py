import pandas as pd
import timeit
from DBcm import UseDatabase


class RecordImport:
    def __init__(self, columns: list, config: dict, no_rows: int ,save_loc: str):
        self.columns = columns
        self.config = config
        self.no_rows = no_rows
        self.save_loc = save_loc
        self.start_time = timeit.default_timer()

    def __enter__(self):
        return self

    def create_db(self):
        with UseDatabase(self.config) as cursor:
            cursor.execute("Drop table if exists houseprices_temp;")
            cursor.execute("""CREATE TABLE houseprices_temp (
                              `ID` int NOT NULL AUTO_INCREMENT,
                              `SellPrice` int NOT NULL,
                              `Datesold` date NOT NULL,
                              `PostCode` varchar(40) DEFAULT NULL,
                              `FirstLine` varchar(100) DEFAULT NULL,
                              `SecondLine` varchar(100) DEFAULT NULL,
                              `ThirdLine` varchar(100) DEFAULT NULL,
                              `FourthLine` varchar(100) DEFAULT NULL,
                              `Town` varchar(100) DEFAULT NULL,
                              `City` varchar(100) DEFAULT NULL,
                              `County` varchar(100) DEFAULT NULL,
                              primary key(`ID`) );""" )
            print("HP_Table_Created")

    def insert_rows(self):
        try:
            fh = pd.read_csv(self.save_loc, usecols=self.columns, header=None,
                         chunksize=self.no_rows)
        except Exception as e:
            print(e)
            print("""Please check the CSV filepath in RunDB is under the 
                    variable 'saveloc'""")
            exit()
        comitted = self.no_rows
        errors = []
        rows_start_time = timeit.default_timer()
        for chunk in fh:
            temp_start_time = timeit.default_timer()
            chunk.fillna(0, inplace=True)
            try:
                    val = []
                    sql = """ INSERT INTO houseprices_temp(SellPrice, DateSold , PostCode, 
                    FirstLine, SecondLine, ThirdLine,FourthLine,
                    Town,City, County) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
                    for index, row in chunk.iterrows():
                        val.append((str(row[1]), str(row[2]), str(row[3]), str(row[7]), str(row[8]), str(row[9]), str(row[10]), str(row[11]), str(row[12]), str(row[13])))
                    with UseDatabase(self.config) as cursor:
                        cursor.executemany(sql, val)
                    complete = (comitted/26657357) * 100
                    print('Committed %s' % (comitted, ))
                    elapsed = timeit.default_timer() - temp_start_time
                    print("This commit took %s seconds, %s percent complete" % (round(elapsed, 2), round(complete, 2)))
                    comitted += self.no_rows
            except Exception as e:
                errors.append(e)
                print(e)
                print("error")
        print(errors)
        elapsed = timeit.default_timer() - rows_start_time
        print("Inserting rows took %s seconds to complete" % elapsed)

    def __exit__(self, exc_type, exc_value, exc_traceback)-> None:
        self.elapsed = timeit.default_timer() - self.start_time
        print(self.elapsed)


if __name__ == "__main__":
    config = {'host': 'localhost', 'user': 'test',
              'password': 'testpasswd', 'database': 'houseprices'}
    save_loc = r"C:\Users\ajmcl\Downloads\pp-complete.csv"
    no_rows = 260000

    columns = [1, 2, 3, 7, 8, 9, 10, 11, 12, 13]
    with RecordImport(columns, config, no_rows, save_loc) as RI:
        RI.create_db()
        RI.insert_rows()
