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

@app.route('/movie/add/location')
def movie_location():
    return render_template('movie_location.html')

@app.post('/data2')
def data2():
    form_data = request.form
    data = {x:y for x,y in form_data.items()}
    _fct = interface()
    _fct.add_movielocation(data['movie'],int(data['year']),data['address'])
    return redirect(url_for('movie'))

@app.post('/data3')
def data3():
    form_data = request.form
    print(form_data)
    data = {x:y for x,y in form_data.items()}
    print(data)
    _fct = interface()
    _fct.delete_movielocation(data['movie'],int(data['year']),data['address'])
    return redirect(url_for('movie'))

@app.route('/location/edit')
def edit_location():
    return render_template('edit_location.html')

@app.post('/data4')
def data4():
    form_data = request.form
    print(form_data)
    data = {x:y for x,y in form_data.items()}
    print(data)
    _fct = interface()
    _fct.update_location(data['old_address'],new_address=data['new_address'])
    return redirect(url_for('index'))

if __name__ == "__main__":
    _fct = interface()
    _fct.clear_schema()
    _fct.load_schema()
    _fct.load_data()
    app.run(debug=True)
