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
  isOriginalTitle BOOLEAN
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

-- 3. title.crew
CREATE TABLE IF NOT EXISTS title_crew (
  tconst TEXT PRIMARY KEY,
  directors TEXT,
  writers TEXT
);

-- 4. title.episode
CREATE TABLE IF NOT EXISTS title_episode (
  tconst TEXT PRIMARY KEY,
  parentTconst TEXT,
  seasonNumber INTEGER,
  episodeNumber INTEGER
);

-- 5. title.principals
CREATE TABLE IF NOT EXISTS title_principals (
  tconst TEXT,
  ordering INTEGER,
  nconst TEXT,
  category TEXT,
  job TEXT,
  characters TEXT
);

-- 6. title.ratings
CREATE TABLE IF NOT EXISTS title_ratings (
  tconst TEXT PRIMARY KEY,
  averageRating NUMERIC,
  numVotes INTEGER
);

-- 7. name.basics
CREATE TABLE IF NOT EXISTS name_basics (
  nconst TEXT PRIMARY KEY,
  primaryName TEXT,
  birthYear INTEGER,
  deathYear INTEGER,
  primaryProfession TEXT,
  knownForTitles TEXT
);
