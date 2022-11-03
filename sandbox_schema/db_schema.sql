create table Movies (
    id TEXT unique not null, -- from imdb
    studio_id INTEGER not null,
    title TEXT not null,
    year INTEGER not null,
    primary key (title,year)
);

create table Studios (
    id INTEGER primary key autoincrement,
    name TEXT unique not null
);

create table Locations (
    id INTEGER primary key autoincrement,
    address TEXT unique not null,
    info TEXT
);

create table Genres (
    id INTEGER primary key autoincrement,
    name TEXT unique not null
);

create table Actors (
    id INTEGER primary key autoincrement,
    firstname TEXT not null,
    lastname TEXT not null,
    born DATE,
    died DATE,
    unique (firstname, lastname)
);

create table Directors (
    id INTEGER primary key autoincrement,
    firstname TEXT not null,
    lastname TEXT not null,
    unique (firstname, lastname)
);

create table MovieGenre (
    movie_id TEXT not null,
    genre_id INTEGER not null,
    primary key (movie_id,genre_id)
);

create table MovieLocation (
    movie_id TEXT not null,
    location_id INTEGER not null
);

create table MovieActor (
    movie_id TEXT not null,
    actor_id INTEGER not NULL,
    primary key (movie_id,actor_id)
);

create table MovieDirector (
    movie_id TEXT not null,
    director_id INTEGER not null,
    primary key (movie_id,director_id)
);