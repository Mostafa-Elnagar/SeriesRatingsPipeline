# rotten_tomatoes_scraper.py

from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from .base_scraper import BaseScraper
from .base_scraper import logger, SeleniumScraper
from typing import Optional, Dict
import re



class RottenTomatoesScraper(SeleniumScraper):
    """
    Scraper for Rotten Tomatoes TV series ratings.
    """
    def __init__(self):
        super().__init__(base_url="https://www.rottentomatoes.com/")

    
    def get_ratings(self, series_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        """
        Fetch and parse ratings for a given series and year. Retries with _{year} suffix if not found or year mismatch.
        """
        sep = '_'
        if not series_title:
            logger.error("Series title cannot be empty.")
            return None
        formatted_title = BaseScraper._preprocess_title(series_title, sep=sep)
        result = self._fetch_and_validate(formatted_title, year)
        if result:
            return result
        logger.info(f"Retrying with title: {formatted_title}{sep}{year}")
        retry_title = f"{formatted_title}{sep}{year}"
        return self._fetch_and_validate(retry_title, year)

    def _fetch_and_validate(self, formatted_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        url = urljoin(self.base_url, f"tv/{formatted_title}/")
        html_content = self._fetch_page(url)
        if not html_content:
            return None
        ratings = self._parse_content(html_content)
        scraped_year = ratings.get("year")
        if scraped_year == year:
            return ratings
        if scraped_year is not None:
            logger.warning(f"Year mismatch for {formatted_title}: expected {year}, found {scraped_year}.")
        return None

    def _parse_content(self, html_content: str) -> Dict[str, int | float | None]:
        soup = BeautifulSoup(html_content, "html.parser")
        ratings: Dict[str, int | float | None] = {
            "critic_score": None,
            "critic_count": None,
            "user_score": None,
            "year": None,
        }
        metadata_props = soup.find_all("rt-text", attrs={"slot": "metadataProp"})
        for prop in metadata_props:
            if isinstance(prop, Tag) and prop.text:
                match = re.search(r"(19|20)\d{2}", prop.text)
                if match:
                    try:
                        ratings["year"] = int(match.group(0))
                        break
                    except Exception:
                        pass
        if ratings["year"] is None:
            year_span = soup.find("span", attrs={"slot": "year"})
            if isinstance(year_span, Tag) and year_span.text:
                try:
                    ratings["year"] = int(year_span.text.strip())
                except Exception:
                    pass
        if ratings["year"] is None:
            title_tag = soup.find("title")
            if isinstance(title_tag, Tag) and title_tag.text:
                match = re.search(r"(19|20)\d{2}", title_tag.text)
                if match:
                    ratings["year"] = int(match.group(0))
        if ratings["year"] is None:
            match = re.search(r"(19|20)\d{2}", soup.text)
            if match:
                ratings["year"] = int(match.group(0))
        if ratings["year"] is None:
            logger.debug("[DEBUG] Could not extract year. First 500 chars of HTML:\n%s", html_content[:500])
        media_scorecard = soup.find("media-scorecard")
        if isinstance(media_scorecard, Tag):
            critic_score_text = media_scorecard.find("rt-text", attrs={"slot": "criticsScore"})
            if isinstance(critic_score_text, Tag) and critic_score_text.text:
                try:
                    ratings["critic_score"] = float(critic_score_text.text.strip().replace('%', ''))
                except ValueError:
                    pass
            audience_score_text = media_scorecard.find("rt-text", attrs={"slot": "audienceScore"})
            if isinstance(audience_score_text, Tag) and audience_score_text.text:
                try:
                    ratings["user_score"] = float(audience_score_text.text.strip().replace('%', ''))
                except ValueError:
                    pass
        overlay = soup.find("media-scorecard-overlay")
        if isinstance(overlay, Tag):
            fresh = overlay.find("rt-text", attrs={"slot": "criticsFreshCount"})
            rotten = overlay.find("rt-text", attrs={"slot": "criticsRottenCount"})
            try:
                fresh_count = int(fresh.text.strip()) if isinstance(fresh, Tag) and fresh.text else 0
                rotten_count = int(rotten.text.strip()) if isinstance(rotten, Tag) and rotten.text else 0
                ratings["critic_count"] = fresh_count + rotten_count
            except Exception:
                pass
        return ratings