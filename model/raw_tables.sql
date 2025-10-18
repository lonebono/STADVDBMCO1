-- IMDb RAW SOURCE TABLES

-- 1. title.akas
CREATE TABLE IF NOT EXISTS title_akas (
  titleId TEXT,
  ordering INTEGER,
  title TEXT,
  region TEXT,
  language TEXT,
  types TEXT,
  attributes TEXT,
  isoriginaltitle BOOLEAN
);

-- 2. title.basics
CREATE TABLE IF NOT EXISTS title_basics (
  tconst TEXT PRIMARY KEY,
  titleType TEXT,
  primaryTitle TEXT,
  originalTitle TEXT,
  isAdult BOOLEAN,
  startYear INTEGER,
  endYear INTEGER,
  runtimeMinutes INTEGER,
  genres TEXT
);

-- 3. title.ratings
CREATE TABLE IF NOT EXISTS title_ratings (
  tconst TEXT PRIMARY KEY,
  averageRating NUMERIC,
  numVotes INTEGER
);

