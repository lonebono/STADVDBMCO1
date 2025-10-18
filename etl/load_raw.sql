-- IMDb RAW SOURCE IMPORTS

-- 1. title.akas
\COPY title_akas(titleid, ordering, title, region, language, types, attributes, isoriginaltitle) FROM 'data/title.akas.tsv' WITH (FORMAT text, DELIMITER E'\t', NULL '\N', ENCODING 'UTF8');

-- 2. title.basics
\COPY title_basics(tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) FROM 'data/title.basics.tsv' WITH (FORMAT text, DELIMITER E'\t', NULL '\N', ENCODING 'UTF8');

-- 3. title.ratings
\COPY title_ratings(tconst, averageRating, numVotes) FROM 'data/title.ratings.tsv' WITH (FORMAT text, DELIMITER E'\t', NULL '\N', ENCODING 'UTF8');