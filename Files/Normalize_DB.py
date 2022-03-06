from DBcm import UseDatabase
import timeit

class Normalize:
    def __init__(self, config: dict, SQLcolumns: dict) -> None:
        self.config = config
        self.SQLcolumns = SQLcolumns
        self.start_time_total = timeit.default_timer()

    def __enter__(self):
        return self

    def fk_table_maker(self) -> None:
        for k, v in self.SQLcolumns.items():
            start_time = timeit.default_timer()
            with UseDatabase(self.config) as cursor:
                cursor.execute("""Drop table if exists %s""" % (k))
                cursor.execute("""create table %s (Id int Not Null AUTO_INCREMENT, %s %s,
                                  primary key (Id));""" % (k, k, v))
                cursor.execute("""insert into %s(%s)
                                    select distinct %s from houseprices_temp;""" % (k, k, k))
                elapsed = timeit.default_timer() - start_time
                print("%s Committed, took %s seconds" % (k, round(elapsed,2)))

    def create_normalised_table(self) -> None:
        with UseDatabase(self.config) as cursor:
            cursor.execute("""Drop table if exists houseprices""")
            cursor.execute("""CREATE TABLE houseprices
        AS (SELECT houseprices_temp.id, SellPrice.id as SellPriceID, datesold.id as DateSoldID, city.id as City, 
        PostCode.id as PostcodeID, FirstLine.id as FirstlineID, SecondLine.id as SecondLineID, 
        ThirdLine.id as ThirdLineID, FourthLine.id as FourthLineID, Town.id as TownID, County.id as CountyID
        FROM houseprices_temp
        left join SellPrice on sellprice.sellprice = houseprices_temp.SellPrice
        left join DateSold on DateSold.DateSold = houseprices_temp.DateSold
        left join PostCode on PostCode.PostCode = houseprices_temp.PostCode
        left join FirstLine on FirstLine.FirstLine= houseprices_temp.FirstLine
        left join SecondLine on SecondLine.SecondLine = houseprices_temp.SecondLine
        left join ThirdLine on ThirdLine.ThirdLine = houseprices_temp.ThirdLine
        left join FourthLine on FourthLine.FourthLine = houseprices_temp.FourthLine
        left join Town on Town.Town = houseprices_temp.Town
        left join city on city.city = houseprices_temp.city
        left join County on County.County = houseprices_temp.County);""")
            print("Normalised table created")


    def drop_houseprices_temp(self) -> None:
        with UseDatabase(self.config) as cursor:
            cursor.execute("""Drop table houseprices_temp""")
            print("Houseprices temp table dropped")

    def add_foreign_key(self) -> None:
        print("Adding foreign key")
        for k, v in self.SQLcolumns.items():
            print("Adding %s foreign key"% k)
            with UseDatabase(self.config) as cursor:
                cursor.execute("alter table houseprices ADD FOREIGN KEY(%sID) REFERENCES %s(id);" % (k, k))
                print("Foreign Key added for %sID"% k)

    def __exit__(self, exc_type, exc_value, exc_traceback) -> str:
        self.elapsed = timeit.default_timer() - self.start_time_total
        print("Normalization took %s seconds to complete" % self.elapsed)


if __name__ == "__main__":
    config = {'host': 'localhost', 'user': 'test',
              'password': 'testpasswd', 'database': 'houseprices'}
    SQLcolumns = {'Sell_Price': 'int', 'Datesold': 'Date', 'PostCode': 'Varchar(8)', 'FirstLine': 'Varchar(100)',
                  'SecondLine': 'Varchar(100)',
                  'ThirdLine': 'Varchar(100)', 'FourthLine': 'Varchar(50)', 'Town': 'Varchar(45)',
                  'City': 'Varchar(45)', 'County': 'Varchar(45)'}

    with Normalize(config, SQLcolumns) as N:
        N.fk_table_maker()
        N.create_normalised_table()
        N.add_foreign_key()
        N.drop_houseprices_temp()
        N.add_foreign_key()
