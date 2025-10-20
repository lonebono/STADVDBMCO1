from config import supabase_engine
from load.upload_utils import upload_to_supabase
from transform.fact_genre_bridge import build_fact_genre_bridge

def load_fact_genre_bridge(title_basics, dim_genre):
    fact_genre_bridge = build_fact_genre_bridge(title_basics, dim_genre)
    upload_to_supabase(fact_genre_bridge, "fact_genre_bridge", supabase_engine)
