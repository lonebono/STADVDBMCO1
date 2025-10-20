from config import supabase_engine
from load.upload_utils import upload_to_supabase
from transform.fact_film_version import build_fact_film_version

def load_fact_film_version(
    title_akas, title_basics, title_ratings,
    dim_region, dim_language, dim_time
):
    fact_film_version = build_fact_film_version(
        title_akas, title_basics, title_ratings,
        dim_region, dim_language, dim_time
    )
    upload_to_supabase(fact_film_version, "fact_film_version", supabase_engine)
