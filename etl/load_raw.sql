-- IMDb RAW SOURCE IMPORTS

-- 1. title.akas
\COPY title_akas FROM 'data/title.akas.tsv' (FORMAT csv, DELIMITER E'\t', HEADER true, NULL '\N');

-- 2. title.basics