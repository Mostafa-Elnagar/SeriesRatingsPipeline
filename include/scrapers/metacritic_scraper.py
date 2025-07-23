from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from .base_scraper import BaseScraper, HtmlScraper, logger
import re
from typing import Optional, Dict
from .ratings_models import validate_ratings


class MetacriticScraper(HtmlScraper):
    """
    Scraper for Metacritic TV series ratings.
    """
    def __init__(self):
        super().__init__(base_url="https://www.metacritic.com/")

    def get_ratings(self, series_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        """
        Fetch and parse ratings for a given series and year. 
        Retries with -{year} suffix if year mismatch.
        """
        sep = '-'

        formatted_title =  BaseScraper._preprocess_title(series_title, sep=sep)
        result = self._fetch_and_validate(formatted_title, year)
        if result:
            return result
        logger.info(f"Retrying with title: {formatted_title}{sep}{year}")
        retry_title = f"{formatted_title}{sep}{year}"
        return self._fetch_and_validate(retry_title, year)


    def _fetch_and_validate(self, formatted_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        url = urljoin(self.base_url, f"tv/{formatted_title}")
        html_content: Optional[str] = self._fetch_page(url)
        if not html_content:
            return None
        ratings = self._parse_content(html_content)
        scraped_year = ratings.get("year")

        if scraped_year != year: # Integrity check
            logger.warning(f"Year mismatch for {formatted_title}: expected {year}, found {scraped_year}.")
            return None

        return ratings

    def _parse_content(self, html_content: str) -> Dict[str, int | float | None]:
        soup = BeautifulSoup(html_content, 'html.parser')
        ratings: Dict[str, int | float | None] = {
            "critic_score": None,
            "critic_count": None,
            "user_score": None,
            "user_count": None,
            "year": None,
        }
        # Extract year
        hero_metadata = soup.find("div", attrs={"data-testid": "hero-metadata"})
        try:
            ratings["year"] = int(hero_metadata.find("li").find("span").text.strip()) # type: ignore
        except (AttributeError, ValueError):
            logger.debug("[DEBUG] Could not extract year. First 500 chars of HTML:\n%s", html_content[:500])
        # Ensure correct types for Pydantic
        for key in ("critic_count", "user_count", "year"):
            if not (isinstance(ratings[key], int) and not isinstance(ratings[key], bool)):
                ratings[key] = None
        for key in ("critic_score", "user_score"):
            if not isinstance(ratings[key], float):
                ratings[key] = None
        
        # Extract critic info
        critic_info = soup.find("div", attrs={"data-testid": "critic-score-info"})
        if isinstance(critic_info, Tag):
            score_div = critic_info.find("div", class_="c-siteReviewScore")
            if isinstance(score_div, Tag):
                score_span = score_div.find("span")
                if isinstance(score_span, Tag) and score_span.text:
                    try:
                        ratings["critic_score"] = float(score_span.text.strip())
                    except ValueError:
                        pass
            review_span = critic_info.find("a", attrs={"data-testid": "critic-path"})
            if isinstance(review_span, Tag) and review_span.text:
                review_text = review_span.get_text(strip=True)
                match = re.search(r"(\d+)", review_text)
                if match:
                    try:
                        ratings["critic_count"] = int(match.group(1))
                    except ValueError:
                        pass
        # Extract user info
        user_info = soup.find("div", attrs={"data-testid": "user-score-info"})
        if isinstance(user_info, Tag):
            score_div = user_info.find("div", class_="c-siteReviewScore")
            if isinstance(score_div, Tag):
                score_span = score_div.find("span")
                if isinstance(score_span, Tag) and score_span.text:
                    try:
                        ratings["user_score"] = float(score_span.text.strip())
                    except ValueError:
                        pass
            review_span = user_info.find("a", attrs={"data-testid": "user-path"})
            if isinstance(review_span, Tag) and review_span.text:
                review_text = review_span.get_text(strip=True)
                match = re.search(r"Based on ([\d,]+) User Ratings", review_text)
                if match:
                    try:
                        ratings["user_count"] = int(match.group(1).replace(",", ""))
                    except ValueError:
                        pass
        # Validate using ratings_models
        return validate_ratings(ratings)
