# 📺 TV Series ETL Pipeline

## Overview
This project implements a scalable, modular ETL pipeline to extract, enrich, and store high-quality TV series metadata and ratings. It is designed to support Tableau dashboards, advanced analytics, and future ML/recommendation use cases.

## Features
- Fetches structured data from TMDB
- Enriches metadata with OMDb and custom web scrapers (Metacritic, Rotten Tomatoes)
- Cleans and validates ratings data
- Modular, monitorable, and ML-ready pipeline
- Airflow orchestration (Astronomer)
- PostgreSQL data warehouse (star schema)
- Tableau dashboard support

## Architecture
- **Airflow DAGs** orchestrate the ETL pipeline
- **Scrapers** (Metacritic, Rotten Tomatoes) implemented in Python (requests, Selenium, BeautifulSoup)
- **OMDb API** for additional ratings enrichment
- **PostgreSQL** for analytics-ready storage
- **dbt** for transformations (planned)
- **Great Expectations** for data validation (planned)
- **Docker** for containerization

```
TMDB API → [Raw Ingestion] → OMDb API Enrichment → Scrapers Enrichment → [Cleaning/Transformation] → PostgreSQL → Tableau
```

## Scrapers Module
- Located in `include/scrapers/`
- **BaseScraper**: Abstract class for all scrapers, handles robots.txt, user agent, and title normalization
- **HtmlScraper**: For static HTML sites (requests)
- **SeleniumScraper**: For dynamic sites (Selenium WebDriver)
- **MetacriticScraper**: Scrapes Metacritic TV ratings (static HTML)
- **RottenTomatoesScraper**: Scrapes Rotten Tomatoes TV ratings (dynamic, Selenium)
- **Ratings Model**: Pydantic model for validation (`ratings_models.py`)
- **main.py**: Example/test runner for scrapers

## Airflow/DAGs
- Example DAG in `dags/exampledag.py` (template for future ETL DAGs)
- Uses Airflow TaskFlow API for modular, idempotent tasks
- To be extended for TMDB/OMDb ingestion, enrichment, and loading

## Database Design
- Star schema for analytics and ML
- **series**: series_id (PK), title, release_year, genres, language, network, plot
- **ratings**: series_id (FK), imdb_rating, imdb_count, tomatoes_critic, tomatoes_critic_count, metacritic, metacritic_count, metauser, metauser_count, start_date, end_date, is_current
- SCD2 (Slowly Changing Dimension Type 2) for ratings history

## How to Run
1. Clone the repo and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set up ChromeDriver for Selenium (see `.env` or pass path explicitly)
3. Run scrapers test:
   ```bash
   python -m include.scrapers.main
   ```
4. (Planned) Run Airflow DAGs for full ETL

## Future Work
- Implement TMDB and OMDb ingestion modules
- Extend Airflow DAGs for full ETL orchestration
- Add dbt models and Great Expectations validation
- Integrate with Tableau and ML pipelines
- Add more scrapers (e.g., IMDb)

## References
- See `brd/BRD.md` for full business requirements and architecture
