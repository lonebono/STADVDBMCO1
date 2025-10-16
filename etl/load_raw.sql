-- IMDb RAW SOURCE IMPORTS

-- 1. title.akas
\COPY title_akas FROM 'data/title.akas.tsv' (FORMAT text, DELIMITER E'\t', NULL '\N', ENCODING 'UTF8');

-- 2. title.basics