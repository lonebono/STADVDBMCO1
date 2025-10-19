-- IMDb Dimensional Model – Final Design
-- Supports analysis of international film distribution, localization, and reception

-- Dimension Tables

CREATE TABLE IF NOT EXISTS dim_region 
(
    region_id SERIAL PRIMARY KEY,
    region_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_language 
(
    language_id SERIAL PRIMARY KEY,
    language_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_genre 
(
    genre_id SERIAL PRIMARY KEY,
    genre_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_time 
(
    time_id SERIAL PRIMARY KEY,
    year INTEGER,
    decade INTEGER, 
);

-- Fact Table
CREATE TABLE IF NOT EXISTS fact_film_version (
    fact_id serial,
    tconst text NOT NULL,
    region_id integer NOT NULL DEFAULT -1,
    language_id integer NOT NULL DEFAULT -1,
    time_id integer NOT NULL DEFAULT -1,
    is_original_title BOOLEAN,
    average_rating NUMERIC(3,1),
    num_votes integer
    PRIMARY KEY (fact_id)
);

-- Bridge Table for genres
CREATE TABLE IF NOT EXISTS fact_genre_bridge (
    fact_id integer NOT NULL,
    genre_id integer NOT NULL,
    PRIMARY KEY (fact_id, genre_id),
    FOREIGN KEY (fact_id) REFERENCES fact_film_version(fact_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id) ON DELETE RESTRICT
);

-- Insert "Unknown"
INSERT INTO dim_region (region_id, region_name) 
VALUES (-1, 'Unknown') ON CONFLICT (region_id) DO NOTHING;

INSERT INTO dim_language (language_id, language_name) 
VALUES (-1, 'Unknown') ON CONFLICT (language_id) DO NOTHING;

INSERT INTO dim_genre (genre_id, genre_name) 
VALUES (-1, 'Unknown') ON CONFLICT (genre_id) DO NOTHING;

INSERT INTO dim_time (time_id, year, decade, era) 
VALUES (-1, NULL, NULL, 'Unknown') ON CONFLICT (time_id) DO NOTHING;

-- Foreign Key Constraints for Fact Table
ALTER TABLE fact_film_version
    ADD CONSTRAINT fk_region FOREIGN KEY (region_id)
    REFERENCES dim_region (region_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE fact_film_version
    ADD CONSTRAINT fk_language FOREIGN KEY (language_id)
    REFERENCES dim_language (language_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE fact_film_version
    ADD CONSTRAINT fk_time FOREIGN KEY (time_id)
    REFERENCES dim_time (time_id) ON UPDATE CASCADE ON DELETE RESTRICT;