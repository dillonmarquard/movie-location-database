import os
from flask import Flask, render_template, request, redirect, url_for
from interface import interface 

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/movie')
def movie():
    _fct = interface()
    res = _fct.view_movies()
    _fct._cur.close()
    return render_template('movie.html',data = res)

@app.route('/movie/add')
def movie_add():
    return render_template('movie_add.html')

@app.post('/data1')
def data1():
    form_data = request.form
    data = {x:y for x,y in form_data.items()}
    _fct = interface()
    _fct.add_movie(data['movie'],int(data['year']),data['studio'])
    return redirect(url_for('movie'))

if __name__ == "__main__":
    _fct = interface()
    _fct.clear_schema()
    _fct.load_schema()
    _fct.load_data()
    app.run(debug=True)

