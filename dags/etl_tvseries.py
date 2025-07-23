from airflow.decorators import dag, task
import pendulum
from datetime import timedelta
from include.mdbs.tmdb_ingestor import TMDBIngestor
from include.mdbs.omdb_enricher import OMDbEnricher
from include.scrapers.metacritic_scraper import MetacriticScraper
from include.scrapers.tomatos_scraper import RottenTomatoesScraper
from include.scrapers.ratings_models import validate_ratings
import os
import psycopg2

# Default args for the DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=DEFAULT_ARGS,
    schedule='@weekly',
    start_date=pendulum.now().subtract(days=1),
    catchup=False,
    tags=['tvseries', 'etl'],
)
def tvseries_etl_pipeline():
    @task()
    def ingest_tmdb():
        # Implement TMDB ingestion logic
        tmdb = TMDBIngestor()
        series = tmdb.fetch_top_rated_series(page=1)
        return {'series': series}

    @task()
    def enrich_omdb(series_data):
        # Implement OMDb enrichment logic
        enricher = OMDbEnricher()
        enriched = []
        for s in series_data['series']:
            ratings = enricher.fetch_ratings(s['title'], s.get('year'))
            s['omdb_ratings'] = ratings
            enriched.append(s)
        return {'series': enriched}

    @task()
    def enrich_scrapers(series_data):
        # Implement enrichment with Metacritic and Rotten Tomatoes scrapers
        metacritic = MetacriticScraper()
        enriched = []
        with RottenTomatoesScraper() as rt_scraper:
            for s in series_data['series']:
                title = s.get('title')
                year = s.get('year')
                s['metacritic_ratings'] = metacritic.get_ratings(title, year)
                s['rotten_tomatoes_ratings'] = rt_scraper.get_ratings(title, year)
                enriched.append(s)
        return {'series': enriched}

    @task()
    def clean_and_validate(series_data):
        # Validate and clean ratings for each series using Pydantic model
        cleaned = []
        for s in series_data['series']:
            # Validate OMDb ratings if present
            if 'omdb_ratings' in s and s['omdb_ratings']:
                s['omdb_ratings'] = validate_ratings({
                    'title': s.get('title', ''),
                    'year': s.get('year', 0),
                    **(s['omdb_ratings'] or {})
                })
            # Validate Metacritic ratings if present
            if 'metacritic_ratings' in s and s['metacritic_ratings']:
                s['metacritic_ratings'] = validate_ratings({
                    'title': s.get('title', ''),
                    'year': s.get('year', 0),
                    **(s['metacritic_ratings'] or {})
                })
            # Validate Rotten Tomatoes ratings if present
            if 'rotten_tomatoes_ratings' in s and s['rotten_tomatoes_ratings']:
                s['rotten_tomatoes_ratings'] = validate_ratings({
                    'title': s.get('title', ''),
                    'year': s.get('year', 0),
                    **(s['rotten_tomatoes_ratings'] or {})
                })
            cleaned.append(s)
        return {'series': cleaned}

    @task()
    def load_to_postgres(series_data):
        # Load cleaned series data into PostgreSQL (star schema, SCD2)
        # Requires psycopg2: pip install psycopg2-binary
        conn = psycopg2.connect(
            dbname=os.getenv('PGDATABASE', 'seriesdb'),
            user=os.getenv('PGUSER', 'postgres'),
            password=os.getenv('PGPASSWORD', 'postgres'),
            host=os.getenv('PGHOST', 'localhost'),
            port=os.getenv('PGPORT', '5432'),
        )
        cur = conn.cursor()
        # Example: Insert into series and ratings tables (simplified, no SCD2 logic)
        for s in series_data['series']:
            cur.execute('''
                INSERT INTO series (tmdb_id, title, release_year, genres, language, plot)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tmdb_id) DO NOTHING
            ''', (
                s.get('tmdb_id'),
                s.get('title'),
                s.get('year'),
                str(s.get('genres')),
                s.get('language'),
                s.get('overview'),
            ))
            cur.execute('''
                INSERT INTO ratings (series_id, imdb_rating, imdb_count, tomatoes_critic, metacritic, metauser, metauser_count, start_date, end_date, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE, NULL, TRUE)
                ON CONFLICT (series_id) DO NOTHING
            ''', (
                s.get('tmdb_id'),
                (s.get('omdb_ratings') or {}).get('imdb_rating'),
                (s.get('omdb_ratings') or {}).get('imdb_count'),
                (s.get('rotten_tomatoes_ratings') or {}).get('critic_score'),
                (s.get('metacritic_ratings') or {}).get('critic_score'),
                (s.get('metacritic_ratings') or {}).get('user_score'),
                (s.get('metacritic_ratings') or {}).get('user_count'),
            ))
        conn.commit()
        cur.close()
        conn.close()
        return 'Loaded to PostgreSQL'

    # Task dependencies
    raw = ingest_tmdb()
    omdb = enrich_omdb(raw)
    enriched = enrich_scrapers(omdb)
    cleaned = clean_and_validate(enriched)
    load_to_postgres(cleaned)

tvseries_etl_pipeline = tvseries_etl_pipeline() 