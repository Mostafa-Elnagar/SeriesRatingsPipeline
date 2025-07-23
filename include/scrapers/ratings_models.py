from typing import Optional
from pydantic import BaseModel, ValidationError, validator
import logging
import datetime

logger = logging.getLogger("scraper")

class Ratings(BaseModel):
    title: str
    year: int
    critic_score: Optional[float] = None
    critic_count: Optional[int] = None
    user_score: Optional[float] = None
    user_count: Optional[int] = None

    @validator('year')
    def year_range(cls, v):
        current_year = datetime.datetime.now().year
        if not isinstance(v, int) or not (1928 <= v <= current_year): # 1928 the first tv show ever
            raise ValueError(f'year must be int between 1900 and {current_year}')
        return v

def validate_ratings(ratings: dict) -> dict:
    """
    Validate and coerce a raw ratings dict using the Ratings model.
    Returns the validated dict, or the original dict if validation fails.
    """
    try:
        validated = Ratings(**ratings)
        return validated.model_dump()
    except ValidationError as e:
        logger.error(f"Ratings validation error: {e}")
        return ratings