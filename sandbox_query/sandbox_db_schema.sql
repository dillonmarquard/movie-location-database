create table Movie (
    id TEXT primary key not null, -- from imdb
    studio_id INTEGER not null,
    title TEXT not null,
    year INTEGER not null
);

create table Studios (
    id INTEGER primary key not null,
    name TEXT not null
);

create table Locations (
    id INTEGER  not null,
    address TEXT unique not null,
    info TEXT not null,
    primary_key(id,address)
);

create table Genres (
    id INTEGER not null,
    name TEXT unique not null,
    primary_key(id,name)
);

create table Actors (
    id INTEGER primary key not null,
    firstname TEXT not null,
    lastname TEXT not null,
    born DATE not null,
    died DATE
);

create table Directors (
    id INTEGER primary key not null,
    firstname TEXT not null,
    lastname TEXT not null,
);

create table MovieGenre (
    movie_id TEXT not null,
    genre_id INTEGER not null,
    primary_key(movie_id,genre_id)
);

create table MovieLocation (
    movie_id TEXT not null,
    location_id INTEGER not null
);

create table MovieActor (
    movie_id TEXT not null,
    actor_id INTEGER not NULL,
    primary_key(movie_id,actor_id)
);

create table MovieDirector (
    movie_id TEXT not null,
    director_id INTEGER not null,
    primary_key(movie_id,director_id)
);