-- Data Wrangling SQL Script
 
 -- This script contains SQL commands for transforming and cleaning raw data.
 
 -- Insert default 'Unknown' entries into dimension tables to handle missing foreign key references
INSERT INTO public.dim_region (region_id, region_name)
VALUES (-1, 'Unknown');

INSERT INTO public.dim_language (language_id, language_name)
VALUES (-1, 'Unknown');

INSERT INTO public.dim_genre (genre_id, genre_name)
VALUES (-1, 'Unknown');

