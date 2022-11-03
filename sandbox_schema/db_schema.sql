create table if not exists Movies (
    id TEXT primary key not null, -- from imdb
    studio_id INTEGER not null,
    title TEXT not null,
    year INTEGER not null
);

create table if not exists Studios (
    id INTEGER primary key not null,
    name TEXT not null
);

create table if not exists Locations (
    id INTEGER primary key not null,
    address TEXT unique not null,
    info TEXT not null
);

create table if not exists Genres (
    id INTEGER not null,
    name TEXT unique not null,
    primary key (id,name)
);

create table if not exists Actors (
    id INTEGER primary key not null,
    firstname TEXT not null,
    lastname TEXT not null,
    born DATE not null,
    died DATE
);

create table if not exists Directors (
    id INTEGER primary key not null,
    firstname TEXT not null,
    lastname TEXT not null
);

create table if not exists MovieGenre (
    movie_id TEXT not null,
    genre_id INTEGER not null,
    primary key (movie_id,genre_id)
);

create table if not exists MovieLocation (
    movie_id TEXT not null,
    location_id INTEGER not null
);

create table if not exists MovieActor (
    movie_id TEXT not null,
    actor_id INTEGER not NULL,
    primary key (movie_id,actor_id)
);

create table if not exists MovieDirector (
    movie_id TEXT not null,
    director_id INTEGER not null,
    primary key (movie_id,director_id)
);