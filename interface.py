import sqlite3
import pandas as pd
import os

class interface:
    def __init__(self):
        self._conn = sqlite3.connect("cse111_movie_rdb.sqlite")
        self._cur = self._conn.cursor()
        self._conn.commit()

    def clear_schema(self):
        try:
            self._cur.execute("""drop table Studios""")
            self._cur.execute("""drop table MovieLocation""")
            self._cur.execute("""drop table MovieGenre""")
            self._cur.execute("""drop table MovieDirector""")
            self._cur.execute("""drop table Movies""")
            self._cur.execute("""drop table Locations""")
            self._cur.execute("""drop table Genres""")
            self._cur.execute("""drop table Directors""")
            self._cur.execute("""drop table Actors""")
            self._cur.execute("""drop table MovieActor""")
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: clear_schema")

    def load_schema(self):
        try:
            with open("sandbox_schema/db_schema.sql", 'r') as sql_file:
                sql = sql_file.read()
            tmp = self._cur.executescript(sql)
            self._conn.commit()
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
            for row in pd.read_csv('data/MovieActor.csv').itertuples():
                self._cur.execute("""insert into MovieActor values (?, ?)""", row[1:] )
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: load_data")

    ### Get id
    def get_studio_id(self,_name):
        res = self._cur.execute("""select id from Studios where name = ?""",[_name])
        tmp = res.fetchall()
        if len(tmp) == 0:
            self.add_studio(_name)
            return self.get_studio_id(_name)
        else:
            return tmp[0][0]

    def get_movie_id(self,_title,_year):
        res = self._cur.execute("""select id from Movies where title = ? and year = ?""",[_title,_year])
        tmp = res.fetchall()
        if len(tmp) == 0:
            return None
        else:
            return tmp[0][0]

    def get_director_id(self,_firstname,_lastname):
        res = self._cur.execute("""select id from Directors where firstname = ? and lastname = ?""",[_firstname,_lastname])
        tmp = res.fetchall()
        if len(tmp) == 0:
            self.add_director(_firstname,_lastname)
            return self.get_director_id(_firstname,_lastname)
        else:
            return tmp[0][0]

    def get_actor_id(self,_firstname,_lastname):
        res = self._cur.execute("""select id from Actors where firstname = ? and lastname = ?""",[_firstname,_lastname])
        tmp = res.fetchall()
        if len(tmp) == 0:
            self.add_actor(_firstname,_lastname)
            return self.get_actor_id(_firstname,_lastname)
        else:
            return tmp[0][0]

    ### Add row
    def add_studio(self,_name):
        self._cur.execute("""insert into Studios (name) values (?)""",[_name])

    def add_movie(self,_id, _title, _year, _studio):
        try:
            _studio_id = self.get_studio_id(_studio)
            self._cur.execute("""insert into Movies (id, studio_id, title, year) values (?, ?, ?, ?)""",[_id,_studio_id,_title,_year])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_movie")

    def add_location(self,_address,_info):
        try:
            self._cur.execute("""insert into Locations (address, info) values (?, ?)""",[_address,_info])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_location")

    def add_actor(self,_fistname,_lastname,_born):
        try:
            self._cur.execute("""insert into Actors (firstname, lastname, born) values (?, ?, ?)""",[_fistname,_lastname,_born])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_actor")

    def add_director(self,_fistname,_lastname):
        try:
            self._cur.execute("""insert into Directors (firstname, lastname) values (?, ?)""",[_fistname,_lastname])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_director")
    # 
    def view_collection(self):
        pass

    # idk
    def parse_output(self):
        pass 

def main():
    _fct = interface()
    _fct.clear_schema()
    print("----------")
    _fct.load_schema()
    print("----------")
    _fct.load_data()
    print("----------")

    _fct.add_movie("tt0000001","Random movie name: the presequel",2000,"doesnt exist studios")
    ####################
    _fct._conn.close()

if __name__ == "__main__":
    main()
