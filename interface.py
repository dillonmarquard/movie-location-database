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
            print("SUCCESS: clear_schema")
        except sqlite3.Error as er:
            print(er)
            print("ERROR: clear_schema")

    def load_schema(self):
        try:
            with open("sandbox_schema/db_schema.sql", 'r') as sql_file:
                sql = sql_file.read()
            tmp = self._cur.executescript(sql)
            self._conn.commit()
            print("SUCCESS: load_schema")
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
            print("SUCCESS: load_data")
        except sqlite3.Error as er:
            print(er)
            print("ERROR: load_data")

    ### Fetch Primary Key / Insert into Table
    def get_studio_id(self,_name):
        if _name == None:
            return None
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
            self.add_movie(_title,_year)
            return self.get_movie_id(_title,_year) # can't infer movie studio
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

    def get_location_id(self,_address):
        res = self._cur.execute("""select id from Locations where address = ?""",[_address])
        tmp = res.fetchall()
        if len(tmp) == 0:
            self.add_location(_address)
            return self.get_location_id(_address)
        else:
            return tmp[0][0]

    def get_genre_id(self,_genre):
        res = self._cur.execute("""select id from Genres where name = ?""",[_genre])
        tmp = res.fetchall()
        if len(tmp) == 0:
            self.add_(_genre)
            return self.get_genre_id(_genre)
        else:
            return tmp[0][0]

    ### Insert into core tables
    def add_studio(self,_name):
        try:
            self._cur.execute("""insert into Studios (name) values (?)""",[_name])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_studio")

    def add_movie(self, _title, _year, _studio=None):
        try:
            _studio_id = self.get_studio_id(_studio)
            self._cur.execute("""insert into Movies (studio_id, title, year) values (?, ?, ?)""",[_studio_id,_title,_year])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_movie")

    def add_location(self,_address,_info=None):
        try:
            self._cur.execute("""insert into Locations (address, info) values (?, ?)""",[_address,_info])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_location")

    def add_actor(self,_firstname,_lastname,_born):
        try:
            self._cur.execute("""insert into Actors (firstname, lastname, born) values (?, ?, ?)""",[_firstname,_lastname,_born])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_actor")

    def add_director(self,_firstname,_lastname):
        try:
            self._cur.execute("""insert into Directors (firstname, lastname) values (?, ?)""",[_firstname,_lastname])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_director")
    
    ### INSERT Relationships
    def add_movielocation(self,_title,_year,_address):
        try:
            _movie_id = self.get_movie_id(_title,_year)
            _location_id = self.get_location_id(_address)
            self._cur.execute("""insert into MovieLocation (movie_id, location_id) values (?, ?)""",[_movie_id, _location_id])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_movielocation")
    
    def add_moviegenre(self, _title,_year,_genre):
        try:
            _movie_id = self.get_movie_id(_title,_year)
            _genre_id = self.get_genre_id(_genre)
            self._cur.execute("""insert into MovieGenre (movie_id, genre_id) values (?, ?)""",[_movie_id, _genre_id])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: add_moviegenre")

    # DELETE
    def delete_movielocation(self, _title,_year,_address):
        try:
            _movie_id = self.get_movie_id(_title,_year)
            _location_id = self.get_location_id(_address)
            self._cur.execute("""delete from MovieGenre where movie_id = ? and location_id = ? limit 1""",[_movie_id,_location_id])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: delete_movielocation")
    
    # UPDATE
    def update_movie(self,_title,_year,_new_title=None,_new_year=None,_new_studio_name=None):
        try:
            tmp = self._cur.execute("""select id, studio_id from Movies where title = ? and year = ?""",[_title,_year]).fetchall()
            if len(tmp) == 0:
                print("ERROR: Movie not in database")
                return
            movie_id = tmp[0][0]
            if _new_title == None:
                _new_title = _title
            if _new_year == None:
                _new_year = _year
            if _new_studio_name == None:
                _new_studio_id = tmp[0][1]
            else:
                _new_studio_id = self.get_studio_id(_new_studio_name)
            self._cur.execute("""update Movies set title = ?, year = ?, studio_id = ? where id = ?""",[_new_title,_new_year,_new_studio_id, movie_id])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: update_movie")

    def update_location(self, _address, new_address=None, new_info=None):
        try:
            tmp = self._cur.execute("""select id, address, info from Locations where address = ?""", [_address]).fetchall()
            if len(tmp) == 0:
                print("ERROR: Address not in database")
                return
            location_id = tmp[0][0]
            if new_address == None:
                new_address = _address
            if new_info == None:
                new_info = tmp[0][2]
            else:
                new_info = self.get_location_id(_address)
            self._cur.execute("""update Locations set address = ?, info = ? where id = ?""",[new_address, new_info, location_id])
            self._conn.commit()
        except sqlite3.Error as er:
            print(er)
            print("ERROR: update_location")

    # VIEWS
    def view_movies(self):
        try:
            tmp = self._cur.execute("""
                select Movies.title, Movies.year, Studios.name, Locations.address
                from Movies
                inner join MovieLocation on Movies.id = MovieLocation.movie_id
                inner join Locations on MovieLocation.location_id = Locations.id
                inner join Studios on Movies.studio_id = Studios.id
                """).fetchall()
            res = "{:<32} {:>4} {:<32} {:<32}".format("Title","Year","Studio","Address")
            for row in tmp:
                res += "\n{:<32} {:>4} {:<32} {:<32}".format(row[0],row[1],row[2],row[3])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_collection_by_location")

    def view_collection_by_year(self, _year):
        try:
            tmp = self._cur.execute("""
                select Movies.title, Movies.year
                from Movies
                where year = ?""",[_year]).fetchall()
            res = "{:<32} {:>4}".format("Title","Year")
            for row in tmp:
                res += "\n{:<32} {:>4}".format(row[0],row[1])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_collection_by_year")
        
    def view_collection_by_studio(self, _studio):
        try:
            tmp = self._cur.execute("""
                select Movies.title, Movies.year
                from Movies
                inner join Studios on Movies.studio_id = Studios.id
                where Studios.name = ?""",[_studio]).fetchall()
            res = "{:<32} {:>4}".format("Title","Year")
            for row in tmp:
                res += "\n{:<32} {:>4}".format(row[0],row[1])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_collection_by_studio")

    def view_collection_by_location(self, _location):
        try:
            tmp = self._cur.execute("""
                select Movies.title, Movies.year
                from Movies
                inner join MovieLocation on Movies.id = MovieLocation.movie_id
                where Genres.location_id = ?""",[_location]).fetchall()
            res = "{:<32} {:>4}".format("Title","Year")
            for row in tmp:
                res += "\n{:<32} {:>4}".format(row[0],row[1])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_collection_by_location")
    
    def view_collection_by_yr_range(self, year_begin, year_end):
        try:
            tmp = self._cur.execute("""
                select Movies.title, Movies.year
                from Movies
                where year between ? and ?""",[year_begin, year_end]).fetchall()
            res = "{:<32} {:>4}".format("Title","Year")
            for row in tmp:
                res += "\n{:<32} {:>4}".format(row[0],row[1])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_collection_by_year")

    # Displays movies where all actors are still alive
    def view_collection_by_living_actor(self):
        try:
            tmp = self._cur.execute("""
                select distinct Movies.title, Movies.year
                from Movies
                inner join MovieActor on Movies.id = MovieActor.movie_id
                inner join Actors on MovieActor.actor_id = Actors.id
                except
                select Movies.title, Movies.year
                from Movies
                inner join MovieActor on Movies.id = MovieActor.movie_id
                inner join Actors on MovieActor.actor_id = Actors.id
                where Actors.died is not null""").fetchall()
            res = "{:<32} {:>4}".format("Title","Year")
            for row in tmp:
                res += "\n{:<32} {:>4}".format(row[0],row[1])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_collection_by_location")

    def view_collection_by_genre(self, _genre):
        try:
            tmp = self._cur.execute("""
                select Movies.title, Movies.year
                from Movies
                inner join MovieGenre on Movies.id = MovieGenre.movie_id
                inner join Genres on MovieGenre.genre_id = Genres.id
                where Genres.name = ?""",[_genre]).fetchall()
            res = "{:<32} {:>4}".format("Title","Year")
            for row in tmp:
                res += "\n{:<32} {:>4}".format(row[0],row[1])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_collection_by_genre")

    def view_collection_by_director(self, _firstname, _lastname):
        try:
            tmp = self._cur.execute("""
                select Movies.title, Movies.year
                from Movies
                inner join MovieDirector on Movies.id = MovieDirector.movie_id
                inner join Directors on MovieDirector.director_id = Directors.id
                where Directors.firstname = ? and Directors.lastname = ?""",[_firstname, _lastname]).fetchall()
            res = "{:<32} {:>4}".format("Title","Year")
            for row in tmp:
                res += "\n{:<32} {:>4}".format(row[0],row[1])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_collection_by_director")

    def view_collection_by_actor(self, _firstname, _lastname):
        try:
            tmp = self._cur.execute("""
                select Movies.title, Movies.year
                from Movies
                inner join MovieActor on Movies.id = MovieActor.movie_id
                inner join Actors on MovieActor.actor_id = Actors.id
                where Actors.firstname = ? and Actors.lastname = ?""",[_firstname, _lastname]).fetchall()
            res = "{:<32} {:>4}".format("Title","Year")
            for row in tmp:
                res += "\n{:<32} {:>4}".format(row[0],row[1])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_collection_by_actor")
    
    def view_actors_by_genre(self,_genre):
        try:
            tmp = self._cur.execute("""
                select distinct Actors.firstname, Actors.lastname
                from Movies
                inner join MovieActor on Movies.id = MovieActor.movie_id
                inner join Actors on MovieActor.actor_id = Actors.id
                inner join MovieGenre on Movies.id = MovieGenre.movie_id
                where Genre.name = ?""",[_genre]).fetchall()
            res = "{:<12} {:<12}".format("First Name","Last Name")
            for row in tmp:
                res += "\n{:<12} {:<12}".format(row[0],row[1])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_actors_by_genre")

    def view_highest_actor_pair(self):
        try:
            tmp = self._cur.execute("""
                select a1.firstname, a1.lastname, a2.firstname, a2.lastname, count(*) films_together
                from MovieActor ma1
                inner join MovieActor ma2 on ma1.movie_id = ma2.movie_id and ma1.actor_id > ma2.actor_id
                inner join Actors a1 on ma1.actor_id = a1.id
                inner join Actors a2 on ma2.actor_id = a2.id 
                group by 1,2,3,4
                having films_together > 1
                order by 5 desc""").fetchall()
            res = "{:<12} {:<12} {:<12} {:<12} {:<16} ".format("First Name","Last Name", "First Name","Last Name", "Movies Together")
            for row in tmp:
                res += "\n{:<12} {:<12} {:<12} {:<12} {:<8}".format(row[0],row[1], row[2], row[3], row[4])
            return res
        except sqlite3.Error as er:
            print(er)
            print("ERROR: view_highest_actor_pair")

    # idk
    def parse_output(self):
        pass 

def main():
    _fct = interface()
    _fct.clear_schema()
    print("--------------------")
    _fct.load_schema()
    print("--------------------")
    _fct.load_data()
    print("--------------------")

    #_fct.add_movie("Random movie name: the presequel",2000,"doesnt exist studios")
    _fct.add_movielocation("Random movie name: the presequel",2000,"123 Elmo Street")
    _fct.update_movie("Random movie name: the presequel",2000,_new_year=2010)
    _fct.update_movie("Random movie name: the presequel",2010,_new_title="adjusted title",_new_studio_name="BBC Films")
    _fct.add_actor("John","Smith","1/1/2000")

    print(_fct.view_collection_by_genre("Horror"))
    print()
    print(_fct.view_collection_by_director("Ridley","Scott"))
    print()
    print("View Collection by Horror")
    print(_fct.view_collection_by_genre("Horror"))
    print()
    print(_fct.view_collection_by_actor("John","Candy"))
    print()
    _fct.add_moviegenre("Dear John", 2010, "Horror")
    print(_fct.view_collection_by_genre("Horror"))
    print()
    print(_fct.view_collection_by_living_actor())
    print()
    print(_fct.view_highest_actor_pair())
    print()
    print(_fct.view_movies())
if __name__ == "__main__":
    main()
