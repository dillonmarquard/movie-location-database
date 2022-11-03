import sqlite3

class interface:
    def __init__(self):
        self.db = sqlite3.connect("movie_db.sqlite")
        self._cur = self.db.cursor()

    def load_schema(self):
        
        try:
            with open("sandbox_schema/db_schema.sql", 'r') as sql_file:
                sql = sql_file.read()
            tmp = self._cur.executescript(sql)
        except:
            print("ERROR: load_schema")
        
    def load_data(self):
        pass

    def add_movie(self,_name,_studio,_year):
        pass

    def parse_output(self):
        pass 

def main():
    _fct = interface()
    _fct.load_schema()

if __name__ == "__main__":
    main()
