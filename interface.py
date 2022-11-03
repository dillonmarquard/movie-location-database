import sqlite3
import pandas as pd
import os

class interface:
    def __init__(self):
        self._db = sqlite3.connect("cse111_movie_rdb.sqlite")
        self._cur = self._db.cursor()

    def load_schema(self):
        try:
            with open("sandbox_schema/db_schema.sql", 'r') as sql_file:
                sql = sql_file.read()
            tmp = self._cur.executescript(sql)
            self._db.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: load_schema")
        
    def load_data(self):
        
        try:
            for row in pd.read_csv('data/Studios.csv').itertuples():
                self._cur.execute("""insert into Studios values (?, ?)""", row[1:] )
            for row in pd.read_csv('data/MovieLocation.csv').itertuples():
                self._cur.execute("""insert into MovieLocation values (?, ?)""", row[1:] )
            for row in pd.read_csv('data/MovieGenre.csv').itertuples():
                self._cur.execute("""insert into MovieGenre values (?, ?)""", row[1:] )
            for row in pd.read_csv('data/MovieDirector.csv').itertuples():
                self._cur.execute("""insert into MovieDirector values (?, ?)""", row[1:] )
            for row in pd.read_csv('data/Movies.csv').itertuples():
                self._cur.execute("""insert into Movies values (?, ?, ?, ?)""", row[1:] )
            for row in pd.read_csv('data/Locations.csv').itertuples():
                self._cur.execute("""insert into Locations values (?, ?, ?)""", row[1:] )
            for row in pd.read_csv('data/Genres.csv').itertuples():
                self._cur.execute("""insert into Genres values (?, ?)""", row[1:] )
            for row in pd.read_csv('data/Directors.csv').itertuples():
                self._cur.execute("""insert into Directors values (?, ?, ?)""", row[1:] )
            for row in pd.read_csv('data/Actors.csv').itertuples():
                self._cur.execute("""insert into Actors values (?, ?, ?, ?, ?)""", row[1:] )
            self._db.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: load_data")

    def add_movie(self,_name,_studio,_year):
        pass

    def parse_output(self):
        pass 

def main():
    _fct = interface()
    _fct.load_schema()
    _fct.load_data()
    _fct._db.close()

if __name__ == "__main__":
    main()
