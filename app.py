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

@app.route('/movie/delete')
def movie_delete():
    _fct = interface()
    res = _fct.view_movies_table()
    _fct._cur.close()
    return render_template('movie_delete.html',data = res)

@app.post('/data10')
def data10():
    form_data = request.form
    data = {x:y for x,y in form_data.items()}
    _fct = interface()
    _fct.delete_movie(data['movie'],int(data['year']))
    return redirect(url_for('movie_delete'))

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
    data = {x:y for x,y in form_data.items()}
    _fct = interface()
    _fct.delete_movielocation(data['movie'],int(data['year']),data['address'])
    return redirect(url_for('movie'))

@app.route('/location/edit')
def edit_location():
    return render_template('edit_location.html')

# TO DO for Nick View Collection by yr_range, studio, location, etc.
@app.route('/view/year/<begin>/<end>')
def view_by_yr_range(begin,end):
    _fct =  interface()
    res = _fct.view_collection_by_yr_range(begin,end)
    return render_template('view_year.html',data = res)

@app.route('/view/studio/<StudioName>')
def view_by_studio_name(StudioName):
    _fct =  interface()
    res = _fct.view_collection_by_studio(StudioName)
    return render_template('view_by_studio_name.html',data = res)

@app.post('/view_by_studio')
def view_studio_name():
    form_data = request.form
    data = {x:y for x,y in form_data.items()}
    return redirect(url_for('view_by_studio_name', StudioName=data["StudioName"]))

#Views for Living Actor
@app.route('/view/living_actor')
def view_by_living_actor():
    _fct =  interface()
    res = _fct.view_collection_by_living_actor()
    return render_template('view_by_living_actor.html',data = res)

@app.route('/view/highest_actor_pair')
def view_by_highest_actors():
    _fct =  interface()
    res = _fct.view_highest_actor_pair()
    return render_template('highest_actor.html',data = res)

# Views for Location Name
@app.route('/view/location/<LocationName>')
def view_by_location_name(LocationName):
    _fct =  interface()
    res = _fct.view_collection_by_location(LocationName)
    return render_template('view_by_address.html',data = res)

@app.post('/view_by_location')
def view_location():
    form_data = request.form
    data = {x:y for x,y in form_data.items()}
    return redirect(url_for('view_by_location_name', LocationName=data["Address"]))

# Views for Actors
@app.route('/view/actors/<Genre>')
def view_by_actors(Genre):
    _fct =  interface()
    res = _fct.view_actors_by_genre(Genre)
    return render_template('actors.html',data = res)

@app.post('/view_by_actors')
def view_actors():
    form_data = request.form
    data = {x:y for x,y in form_data.items()}
    return redirect(url_for('view_by_actors', Genre=data["Genre"]))


@app.route('/view')
def view():
    return render_template('view.html')

@app.post('/view_year')
def view_year():
    form_data = request.form
    data = {x:y for x,y in form_data.items()}
    return redirect(url_for('view_by_yr_range', begin=data["StartYear"], end=data["EndYear"]))


@app.post('/data4')
def data4():
    form_data = request.form
    data = {x:y for x,y in form_data.items()}
    _fct = interface()
    _fct.update_location(data['old_address'],new_address=data['new_address'])
    return redirect(url_for('index'))

if __name__ == "__main__":
    _fct = interface()
    _fct.clear_schema()
    _fct.load_schema()
    _fct.load_data()
    app.run(debug=True)

