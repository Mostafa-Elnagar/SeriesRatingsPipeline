"""
omdb_enricher.py
Module for enriching series metadata with ratings from the OMDb API.
"""
import os
import requests

class OMDbEnricher:
    """
    Enriches series metadata with ratings from the OMDb API.
    """
    BASE_URL = "http://www.omdbapi.com/"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("OMDB_API_KEY")
        if not self.api_key:
            raise ValueError("OMDb API key must be set in OMDB_API_KEY environment variable or passed explicitly.")

    def fetch_ratings(self, title: str, year: int = None):
        params = {
            "apikey": self.api_key,
            "t": title,
            "type": "series"
        }
        if year:
            params["y"] = str(year)
        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("Response") != "True":
            return None
        ratings = {
            "imdb_rating": data.get("imdbRating"),
            "imdb_count": data.get("imdbVotes"),
            "tomatoes_rating": None,
            "metacritic_rating": data.get("Metascore"),
        }
        for r in data.get("Ratings", []):
            if r["Source"] == "Rotten Tomatoes":
                ratings["tomatoes_rating"] = r["Value"]
        return ratings 