from Normalize_DB import Normalize
from Record_import_sql_bulk_insert import RecordImport
import timeit

Whole_Script_start_time = timeit.default_timer()

config = {'host': 'localhost', 'user': 'test',
          'password': 'testpasswd', 'database': 'houseprices'}

columns = [1, 2, 3, 7, 8, 9, 10, 11, 12, 13]

no_rows = 533200

save_loc = r"pp-complete.csv"

with RecordImport(columns, config, no_rows, save_loc) as RI:
    RI.create_db()
    RI.insert_rows()


SQLcolumns = {'SellPrice': 'int', 'Datesold': 'Date', 'PostCode': 'Varchar(8)', 'FirstLine': 'Varchar(100)',
              'SecondLine': 'Varchar(100)',
              'ThirdLine': 'Varchar(100)', 'FourthLine': 'Varchar(50)', 'Town': 'Varchar(45)',
              'City': 'Varchar(45)', 'County': 'Varchar(45)'}

with Normalize(config, SQLcolumns) as N:
    N.fk_table_maker()
    N.create_normalised_table()
    N.add_foreign_key()
    N.drop_houseprices_temp()
    

Whole_script_elapsed = timeit.default_timer() - Whole_Script_start_time
print("Entire process took %s seconds to complete" % Whole_script_elapsed)
