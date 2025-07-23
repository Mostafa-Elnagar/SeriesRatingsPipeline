import os
import requests

class TMDBIngestor:
    """
    Ingests top-rated TV series metadata from the TMDB API.
    """
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise ValueError("TMDB API key must be set in TMDB_API_KEY environment variable or passed explicitly.")

    def fetch_top_rated_series(self, page: int = 1, language: str = "en-US"):
        url = f"{self.BASE_URL}/tv/top_rated"
        params = {
            "api_key": self.api_key,
            "language": language,
            "page": page
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        series_list = []
        for item in data.get("results", []):
            series_list.append({
                "title": item.get("name"),
                "year": int(item.get("first_air_date", "0000")[:4]) if item.get("first_air_date") else None,
                "genres": item.get("genre_ids", []),
                "language": item.get("original_language"),
                "overview": item.get("overview"),
                "tmdb_id": item.get("id"),
                "popularity": item.get("popularity"),
                "vote_average": item.get("vote_average"),
                "vote_count": item.get("vote_count"),
            })
        return series_list 