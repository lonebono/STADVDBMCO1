-- IMDb Dimension Model WIP
-- Combines and transforms raw IMDb data into a dimensional model for analysis

-- Dimension Tables

CREATE TABLE IF NOT EXISTS dim_region
(
    region_id serial PRIMARY KEY,
    region_name text
);

CREATE TABLE IF NOT EXISTS dim_language
(
    language_id serial PRIMARY KEY,
    language_name text
);

CREATE TABLE IF NOT EXISTS dim_genre
(
    genre_id serial PRIMARY KEY,
    genre_name text
);

-- Fact Table
CREATE TABLE IF NOT EXISTS fact_table
(
    fact_id serial,
    tconst text NOT NULL,
    region_id integer NOT NULL DEFAULT -1,
    language_id integer NOT NULL DEFAULT -1,
    genre_id integer NOT NULL DEFAULT -1,
    is_original_title boolean,
    average_ratings numeric(3,1),
    num_votes integer,
    PRIMARY KEY (fact_id)
);

-- Foreign Key Constraints

ALTER TABLE IF EXISTS fact_table
    ADD CONSTRAINT fact_region_fk FOREIGN KEY (region_id)
    REFERENCES public.dim_region (region_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE RESTRICT
    NOT VALID;

ALTER TABLE IF EXISTS fact_table
    ADD CONSTRAINT fact_language_fk FOREIGN KEY (language_id)
    REFERENCES public.dim_language (language_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE RESTRICT
    NOT VALID;


ALTER TABLE IF EXISTS fact_table
    ADD CONSTRAINT fact_genre_fk FOREIGN KEY (genre_id)
    REFERENCES public.dim_genre (genre_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE RESTRICT
    NOT VALID;