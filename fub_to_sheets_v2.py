"""
FUB to Sheets - Version 2.0
Complete rewrite with enhanced features, security, and maintainability

Author: Joseph "Eugy" Williams 
Date: December 2024
"""

import os
import sys
import time
import math
import json
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# =========================================================================
# CONFIGURATION
# =========================================================================

class Config:
    """Centralized configuration with validation"""
    
    # Follow Up Boss
    FUB_API_KEY = os.getenv("FUB_API_KEY")
    FUB_BASE_URL = "https://api.followupboss.com/v1"
    
    # Google Sheets
    GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
    GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    
    # SMTP Email
    SMTP_ENABLED = os.getenv("SMTP_ENABLED", "true").lower() == "true"
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    EMAIL_TO = os.getenv("EMAIL_TO", os.getenv("SMTP_USERNAME"))
    EMAIL_SUBJECT_PREFIX = os.getenv("EMAIL_SUBJECT_PREFIX", "[FUB Daily]")
    
    # Performance Settings
    REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.2"))
    DEFAULT_FETCH_LIMIT = int(os.getenv("DEFAULT_FETCH_LIMIT", "100"))
    MAX_PARALLEL_WORKERS = int(os.getenv("MAX_PARALLEL_WORKERS", "5"))
    ENABLE_STAGE_SYNC = os.getenv("ENABLE_STAGE_SYNC", "false").lower() == "true"
    
    # Caching
    ENABLE_CACHE = os.getenv("ENABLE_CACHE", "true").lower() == "true"
    CACHE_MAX_AGE_MINUTES = int(os.getenv("CACHE_MAX_AGE_MINUTES", "30"))
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        errors = []
        
        if not cls.FUB_API_KEY:
            errors.append("FUB_API_KEY is required")
        
        if not cls.GOOGLE_SHEET_ID:
            errors.append("GOOGLE_SHEET_ID is required")
        
        if not Path(cls.GOOGLE_SERVICE_ACCOUNT_FILE).exists():
            errors.append(f"Google service account file not found: {cls.GOOGLE_SERVICE_ACCOUNT_FILE}")
        
        if cls.SMTP_ENABLED:
            if not cls.SMTP_USERNAME:
                errors.append("SMTP_USERNAME is required when SMTP is enabled")
            if not cls.SMTP_PASSWORD:
                errors.append("SMTP_PASSWORD is required when SMTP is enabled")
        
        if errors:
            raise RuntimeError(
                "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )
        
        return True


# =========================================================================
# LOGGING SETUP
# =========================================================================

def setup_logging() -> logging.Logger:
    """Configure comprehensive logging"""
    os.makedirs("logs", exist_ok=True)
    
    timestamp = datetime.now().strftime("%y%m%d.%H%M")
    log_filename = f"logs/{timestamp}.log"
    
    # Formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_filename,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    
    # Root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Suppress noisy loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('google').setLevel(logging.WARNING)
    
    logger.info(f"Logging initialized → {log_filename}")
    return logger


# Initialize logger
logger = setup_logging()


# =========================================================================
# CUSTOM EXCEPTIONS
# =========================================================================

class FUBError(Exception):
    """Base exception for FUB-related errors"""
    pass

class FUBAPIError(FUBError):
    """API request failed"""
    pass

class RateLimitExceeded(FUBError):
    """Rate limit exceeded"""
    pass

class DataValidationError(Exception):
    """Data validation failed"""
    pass


# =========================================================================
# DATA CACHE
# =========================================================================

class DataCache:
    """Simple file-based cache for API data"""
    
    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.enabled = Config.ENABLE_CACHE
    
    def get(self, key: str, max_age_minutes: int = None) -> Optional[Any]:
        """Retrieve cached data if fresh"""
        if not self.enabled:
            return None
        
        max_age = max_age_minutes or Config.CACHE_MAX_AGE_MINUTES
        cache_file = self.cache_dir / f"{key}.json"
        
        if not cache_file.exists():
            return None
        
        # Check age
        age_minutes = (time.time() - cache_file.stat().st_mtime) / 60
        if age_minutes > max_age:
            logger.debug(f"Cache expired for {key} (age: {age_minutes:.1f}m)")
            return None
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            logger.debug(f"Cache hit for {key} (age: {age_minutes:.1f}m)")
            return data
        except Exception as e:
            logger.warning(f"Cache read error for {key}: {e}")
            return None
    
    def set(self, key: str, data: Any):
        """Store data in cache"""
        if not self.enabled:
            return
        
        cache_file = self.cache_dir / f"{key}.json"
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f)
            logger.debug(f"Cached {key}")
        except Exception as e:
            logger.warning(f"Cache write error for {key}: {e}")
    
    def clear(self):
        """Clear all cache files"""
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()
        logger.info("Cache cleared")


# Global cache instance
cache = DataCache()


# =========================================================================
# FUB API CLIENT
# =========================================================================

class FUBClient:
    """Follow Up Boss API client with enhanced error handling"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = (Config.FUB_API_KEY, "")
        self.session.headers["Accept"] = "application/json"
        self.base_url = Config.FUB_BASE_URL
        logger.info("FUB API client initialized")
    
    def _fetch_with_retry(
        self, 
        url: str, 
        params: Dict = None, 
        max_retries: int = 3
    ) -> Dict:
        """Fetch with exponential backoff retry logic"""
        params = params or {}
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"GET {url} (attempt {attempt + 1}/{max_retries})")
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    wait_time = int(retry_after) if retry_after else min(2 ** attempt * 5, 60)
                    logger.warning(f"Rate limited. Waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout (attempt {attempt + 1}/{max_retries})")
                if attempt == max_retries - 1:
                    raise FUBAPIError(f"Request timeout after {max_retries} attempts")
                time.sleep(2 ** attempt)
                
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error: {e}")
                if attempt == max_retries - 1:
                    raise FUBAPIError(f"HTTP error after {max_retries} attempts: {e}")
                time.sleep(2 ** attempt)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                if attempt == max_retries - 1:
                    raise FUBAPIError(f"Request failed after {max_retries} attempts: {e}")
                time.sleep(2 ** attempt)
        
        raise FUBAPIError(f"Max retries ({max_retries}) exceeded")
    
    def fetch_collection(
        self, 
        path: str, 
        collection_key: str, 
        params: Dict = None,
        use_cache: bool = True
    ) -> List[Dict]:
        """Generic paginator for FUB endpoints"""
        cache_key = f"{path}_{json.dumps(params, sort_keys=True)}"
        
        # Try cache first
        if use_cache:
            cached_data = cache.get(cache_key)
            if cached_data is not None:
                logger.info(f"Using cached data for {path}")
                return cached_data
        
        all_items = []
        params = dict(params or {})
        params.setdefault("limit", Config.DEFAULT_FETCH_LIMIT)
        
        next_token = None
        page = 0
        
        logger.info(f"Fetching {path}...")
        
        while True:
            page += 1
            if next_token:
                params["next"] = next_token
                params.pop("offset", None)
            
            url = f"{self.base_url}{path}"
            data = self._fetch_with_retry(url, params)
            
            items = data.get(collection_key, [])
            all_items.extend(items)
            
            if page % 5 == 0:
                logger.info(f"  {path}: page {page}, {len(all_items)} items so far")
            
            meta = data.get("_metadata", {})
            next_token = meta.get("next")
            if not next_token:
                break
            
            time.sleep(Config.REQUEST_SLEEP_SECONDS)
        
        logger.info(f"✓ Fetched {len(all_items)} items from {path} ({page} pages)")
        
        # Cache the result
        if use_cache:
            cache.set(cache_key, all_items)
        
        return all_items
    
    def fetch_people(self) -> List[Dict]:
        """Fetch all contacts"""
        params = {
            "fields": "allFields",
            "includeTrash": "true",
        }
        return self.fetch_collection("/people", "people", params)
    
    def fetch_calls(self) -> List[Dict]:
        """Fetch all calls"""
        return self.fetch_collection("/calls", "calls")
    
    def fetch_events(self, limit: int = 100) -> List[Dict]:
        """Fetch events (website & property activity)"""
        params = {"limit": limit}
        return self.fetch_collection("/events", "events", params)
    
    def fetch_text_messages_for_person(self, person_id: str) -> List[Dict]:
        """Fetch text messages for a single person"""
        params = {
            "personId": person_id,
            "limit": 100,
        }
        try:
            return self.fetch_collection(
                "/textMessages", 
                "textMessages", 
                params,
                use_cache=False  # Don't cache individual person texts
            )
        except Exception as e:
            logger.warning(f"Failed to fetch texts for person {person_id}: {e}")
            return []
    
    def fetch_text_messages_parallel(self, people: List[Dict]) -> List[Dict]:
        """Fetch text messages in parallel for better performance"""
        logger.info(f"Fetching text messages for {len(people)} people (parallel)...")
        
        all_texts = []
        
        def fetch_for_person(person):
            pid = person.get("id")
            if not pid:
                return []
            return self.fetch_text_messages_for_person(pid)
        
        with ThreadPoolExecutor(max_workers=Config.MAX_PARALLEL_WORKERS) as executor:
            futures = {executor.submit(fetch_for_person, p): p for p in people}
            
            completed = 0
            for future in as_completed(futures):
                texts = future.result()
                if texts:
                    all_texts.extend(texts)
                completed += 1
                
                if completed % 50 == 0:
                    logger.info(f"  Text messages: {completed}/{len(people)} people processed")
        
        logger.info(f"✓ Fetched {len(all_texts)} total text messages")
        return all_texts
    
    def update_person_stage(self, person_id: str, new_stage: str):
        """Update the stage of a person"""
        if not new_stage or not Config.ENABLE_STAGE_SYNC:
            return
        
        url = f"{self.base_url}/people/{person_id}"
        payload = {"stage": new_stage}
        
        try:
            response = self.session.put(url, json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"Updated stage for {person_id} → {new_stage}")
        except Exception as e:
            logger.error(f"Failed to update stage for {person_id}: {e}")


# =========================================================================
# SCORING ENGINE
# =========================================================================

class ScoringConfig:
    """Configurable weights for lead scoring"""
    
    # Heat Score Weights
    HEAT_WEIGHTS = {
        "website_visit": 2.0,
        "property_viewed": 3.0,
        "property_favorited": 4.0,
        "property_shared": 2.0,
        "call_inbound": 3.0,
        "text_inbound": 3.0,
    }
    
    # Recency Bonuses (days -> bonus points)
    RECENCY_TIERS = [
        (3, 20),
        (7, 15),
        (14, 10),
        (30, 5),
        (float('inf'), 0)
    ]
    
    # Priority Composite Weights
    PRIORITY_WEIGHTS = {
        "heat": 0.45,
        "value": 0.25,
        "relationship": 0.30,
    }
    
    # Stage Multipliers
    STAGE_MULTIPLIERS = {
        "Hot Lead": 1.3,
        "Active Buyer": 1.2,
        "Active Seller": 1.2,
        "Nurture": 1.0,
        "New Lead": 0.9,
        "Cold": 0.7,
        "Closed": 0.0,
        "Trash": 0.0,
    }


class LeadScorer:
    """Enhanced lead scoring engine"""
    
    def __init__(self, config: ScoringConfig = None):
        self.config = config or ScoringConfig()
    
    def calculate_heat_score(
        self,
        website_visits_7d: int,
        properties_viewed_7d: int,
        properties_favorited: int,
        properties_shared: int,
        calls_inbound: int,
        texts_inbound: int,
        days_since_last_touch: int
    ) -> Tuple[float, Dict]:
        """Calculate heat score with breakdown"""
        w = self.config.HEAT_WEIGHTS
        
        engagement = (
            website_visits_7d * w["website_visit"]
            + properties_viewed_7d * w["property_viewed"]
            + properties_favorited * w["property_favorited"]
            + properties_shared * w["property_shared"]
            + calls_inbound * w["call_inbound"]
            + texts_inbound * w["text_inbound"]
        )
        
        # Recency bonus
        recency_bonus = 0
        for threshold, bonus in self.config.RECENCY_TIERS:
            if days_since_last_touch <= threshold:
                recency_bonus = bonus
                break
        
        raw_score = engagement + recency_bonus
        final_score = max(0, min(100, round(raw_score, 1)))
        
        breakdown = {
            "engagement": round(engagement, 1),
            "recency_bonus": recency_bonus,
            "final_score": final_score
        }
        
        return final_score, breakdown
    
    def calculate_value_score(
        self,
        avg_price_viewed: float,
        price_std_dev: float,
        max_avg_price: float
    ) -> Tuple[float, Dict]:
        """Calculate value score based on price point and consistency"""
        if avg_price_viewed <= 0 or max_avg_price <= 0:
            return 0.0, {"price_component": 0, "consistency_component": 0}
        
        # Price component
        price_component = (avg_price_viewed / max_avg_price) * 50.0
        
        # Consistency component
        cluster_confidence = 1.0 - (price_std_dev / avg_price_viewed)
        cluster_confidence = max(0.0, min(1.0, cluster_confidence))
        consistency_component = cluster_confidence * 50.0
        
        raw_score = price_component + consistency_component
        final_score = max(0, min(100, round(raw_score, 1)))
        
        breakdown = {
            "price_component": round(price_component, 1),
            "consistency_component": round(consistency_component, 1),
            "final_score": final_score
        }
        
        return final_score, breakdown
    
    def calculate_relationship_score(
        self,
        calls_inbound: int,
        calls_outbound: int,
        texts_inbound: int,
        texts_total: int
    ) -> Tuple[float, Dict]:
        """Calculate relationship strength score"""
        inbound_contacts = calls_inbound + texts_inbound
        total_contacts = calls_inbound + calls_outbound + texts_total
        
        if total_contacts > 0:
            inbound_ratio = inbound_contacts / total_contacts
        else:
            inbound_ratio = 0.0
        
        inbound_ratio = max(0.0, min(1.0, inbound_ratio))
        ratio_component = inbound_ratio * 50.0
        
        # Volume component (capped)
        capped_contacts = min(inbound_contacts, 10)
        volume_component = capped_contacts * 5.0
        
        raw_score = ratio_component + volume_component
        final_score = max(0, min(100, round(raw_score, 1)))
        
        breakdown = {
            "inbound_ratio": round(inbound_ratio, 2),
            "ratio_component": round(ratio_component, 1),
            "volume_component": round(volume_component, 1),
            "final_score": final_score
        }
        
        return final_score, breakdown
    
    def calculate_priority_score(
        self,
        heat_score: float,
        value_score: float,
        relationship_score: float,
        stage: str = ""
    ) -> Tuple[float, Dict]:
        """Calculate composite priority score with stage multiplier"""
        w = self.config.PRIORITY_WEIGHTS
        stage_mult = self.config.STAGE_MULTIPLIERS.get(stage, 1.0)
        
        raw_score = (
            heat_score * w["heat"]
            + value_score * w["value"]
            + relationship_score * w["relationship"]
        ) * stage_mult
        
        final_score = max(0, min(100, round(raw_score, 1)))
        
        breakdown = {
            "heat_contribution": round(heat_score * w["heat"], 1),
            "value_contribution": round(value_score * w["value"], 1),
            "relationship_contribution": round(relationship_score * w["relationship"], 1),
            "stage_multiplier": stage_mult,
            "final_score": final_score
        }
        
        return final_score, breakdown


# =========================================================================
# DATA PROCESSING
# =========================================================================

def validate_person(person: Dict) -> bool:
    """Validate person record"""
    if not person.get("id"):
        logger.warning("Person missing ID field")
        return False
    return True


def parse_datetime_safe(dt_str: Any) -> Optional[datetime]:
    """Safely parse datetime string"""
    if not dt_str:
        return None
    
    if isinstance(dt_str, datetime):
        return dt_str
    
    try:
        # Try ISO format first
        return datetime.fromisoformat(str(dt_str).replace('Z', '+00:00'))
    except Exception:
        pass
    
    # Try other common formats
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
        try:
            return datetime.strptime(str(dt_str), fmt)
        except ValueError:
            continue
    
    return None


def parse_date_safe(date_str: Any) -> Optional[date]:
    """Safely parse date string"""
    date_str = (date_str or "").strip()
    if not date_str:
        return None
    
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    logger.debug(f"Could not parse date: {date_str}")
    return None


def flatten_person_base(person: Dict) -> Dict:
    """Extract base person fields"""
    return {
        "id": person.get("id"),
        "firstName": person.get("firstName", ""),
        "lastName": person.get("lastName", ""),
        "stage": person.get("stage", ""),
        "source": person.get("source", ""),
        "leadTypeTags": ", ".join(person.get("leadTypeTags", [])) if person.get("leadTypeTags") else "",
        "created": person.get("created", ""),
        "updated": person.get("updated", ""),
        "ownerId": person.get("ownerId", ""),
        "primaryEmail": person.get("emails", [{}])[0].get("value", "") if person.get("emails") else "",
        "primaryPhone": person.get("phones", [{}])[0].get("value", "") if person.get("phones") else "",
        "company": person.get("company", ""),
        "website": person.get("website", ""),
        "lastActivity": person.get("lastActivity", ""),
    }


def build_person_stats(
    calls: List[Dict],
    texts: List[Dict],
    events: List[Dict]
) -> Dict[str, Dict]:
    """Build per-person statistics from activities"""
    logger.info("Building person statistics...")
    
    stats = defaultdict(lambda: {
        "calls_outbound": 0,
        "calls_inbound": 0,
        "texts_total": 0,
        "texts_inbound": 0,
        "website_visits": 0,
        "website_visits_last_7": 0,
        "properties_viewed": 0,
        "properties_viewed_last_7": 0,
        "properties_favorited": 0,
        "properties_shared": 0,
        "last_website_visit": None,
        "avg_price_viewed": None,
        "price_view_std_dev": 0.0,
    })
    
    now = datetime.now(timezone.utc)
    seven_days_ago = now - timedelta(days=7)
    
    # Process calls
    for call in calls:
        pid = call.get("personId")
        if not pid:
            continue
        
        direction = call.get("direction", "").lower()
        if direction == "outbound":
            stats[pid]["calls_outbound"] += 1
        elif direction == "inbound":
            stats[pid]["calls_inbound"] += 1
    
    # Process texts
    for text in texts:
        pid = text.get("personId")
        if not pid:
            continue
        
        stats[pid]["texts_total"] += 1
        
        direction = text.get("direction", "").lower()
        if direction == "inbound":
            stats[pid]["texts_inbound"] += 1
    
    # Process events
    for event in events:
        pid = event.get("personId")
        if not pid:
            continue
        
        event_type = event.get("type", "")
        event_time = parse_datetime_safe(event.get("created"))
        
        if event_type == "website":
            stats[pid]["website_visits"] += 1
            if event_time and event_time >= seven_days_ago:
                stats[pid]["website_visits_last_7"] += 1
            if event_time:
                if stats[pid]["last_website_visit"] is None or event_time > stats[pid]["last_website_visit"]:
                    stats[pid]["last_website_visit"] = event_time
        
        elif event_type == "property_viewed":
            stats[pid]["properties_viewed"] += 1
            if event_time and event_time >= seven_days_ago:
                stats[pid]["properties_viewed_last_7"] += 1
        
        elif event_type == "property_favorited":
            stats[pid]["properties_favorited"] += 1
        
        elif event_type == "property_shared":
            stats[pid]["properties_shared"] += 1
    
    # Calculate average price and std dev per person
    for event in events:
        pid = event.get("personId")
        if not pid or event.get("type") != "property_viewed":
            continue
        
        price = event.get("propertyListPrice")
        if price:
            try:
                price_val = float(price)
                # Track prices for this person
                if "price_list" not in stats[pid]:
                    stats[pid]["price_list"] = []
                stats[pid]["price_list"].append(price_val)
            except (ValueError, TypeError):
                pass
    
    # Compute avg and std dev
    for pid, data in stats.items():
        prices = data.get("price_list", [])
        if prices:
            avg = sum(prices) / len(prices)
            data["avg_price_viewed"] = avg
            
            if len(prices) > 1:
                variance = sum((p - avg) ** 2 for p in prices) / len(prices)
                data["price_view_std_dev"] = math.sqrt(variance)
        
        # Convert datetime to string for serialization
        if data["last_website_visit"]:
            data["last_website_visit"] = data["last_website_visit"].isoformat()
    
    logger.info(f"✓ Built statistics for {len(stats)} people")
    return dict(stats)


def compute_daily_activity_stats(
    events: List[Dict],
    people_by_id: Dict[str, Dict]
) -> Dict:
    """Compute daily activity statistics for email reporting"""
    logger.info("Computing daily activity statistics...")
    
    today = datetime.now(timezone.utc).date()
    
    stats = {
        "total_events_today": 0,
        "website_visits_today": 0,
        "properties_viewed_today": 0,
        "unique_visitors_today": set(),
        "top_active_leads": [],
    }
    
    activity_by_person = defaultdict(int)
    
    for event in events:
        event_time = parse_datetime_safe(event.get("created"))
        if not event_time or event_time.date() != today:
            continue
        
        stats["total_events_today"] += 1
        
        event_type = event.get("type", "")
        pid = event.get("personId")
        
        if event_type == "website":
            stats["website_visits_today"] += 1
            if pid:
                stats["unique_visitors_today"].add(pid)
        
        elif event_type == "property_viewed":
            stats["properties_viewed_today"] += 1
        
        if pid:
            activity_by_person[pid] += 1
    
    # Top 5 most active today
    top_active = sorted(
        activity_by_person.items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]
    
    for pid, count in top_active:
        person = people_by_id.get(str(pid), {})
        name = f"{person.get('firstName', '')} {person.get('lastName', '')}".strip()
        stats["top_active_leads"].append({
            "name": name or "Unknown",
            "activity_count": count
        })
    
    stats["unique_visitors_today"] = len(stats["unique_visitors_today"])
    
    logger.info(f"✓ Daily stats: {stats['total_events_today']} events, {stats['unique_visitors_today']} unique visitors")
    return stats


# =========================================================================
# GOOGLE SHEETS
# =========================================================================

# Contacts header definition
CONTACTS_HEADER = [
    "id", "firstName", "lastName", "stage", "source", "leadTypeTags",
    "created", "updated", "ownerId", "primaryEmail", "primaryPhone",
    "company", "website", "lastActivity", "last_website_visit",
    "avg_price_viewed", "website_visits", "properties_viewed",
    "properties_favorited", "properties_shared", "calls_outbound",
    "calls_inbound", "texts_total", "texts_inbound", "emails_received",
    "emails_sent", "heat_score", "value_score", "relationship_score",
    "priority_score", "next_action", "next_action_date"
]


def get_sheets_client():
    """Get Google Sheets client"""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    
    try:
        creds = Credentials.from_service_account_file(
            Config.GOOGLE_SERVICE_ACCOUNT_FILE,
            scopes=scopes
        )
        return gspread.authorize(creds)
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Google Sheets client: {e}")


def get_or_create_worksheet(spreadsheet, title: str):
    """Get existing worksheet or create new one"""
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Creating new worksheet: {title}")
        return spreadsheet.add_worksheet(title=title, rows=1000, cols=50)


def write_table_to_worksheet(worksheet, header: List[str], rows: List[List]):
    """Write table data to worksheet with header"""
    if not rows:
        logger.warning(f"No rows to write to {worksheet.title}")
        return
    
    # Clear existing content
    worksheet.clear()
    
    # Write header + rows
    all_data = [header] + rows
    
    # Batch update for performance
    worksheet.update(all_data, value_input_option='USER_ENTERED')
    logger.info(f"✓ Wrote {len(rows)} rows to {worksheet.title}")


def build_contact_rows(
    people: List[Dict],
    person_stats: Dict[str, Dict],
    persisted_actions: Dict[str, Dict]
) -> List[List]:
    """Build contact rows with enhanced scoring"""
    logger.info(f"Building contact rows for {len(people)} people...")
    
    # Calculate max average price for normalization
    max_avg_price = 1.0
    for stats in person_stats.values():
        avg_price = stats.get("avg_price_viewed")
        if avg_price and avg_price > max_avg_price:
            max_avg_price = avg_price
    
    scorer = LeadScorer()
    rows = []
    now = datetime.now(timezone.utc)
    
    for i, person in enumerate(people):
        if not validate_person(person):
            continue
        
        base = flatten_person_base(person)
        pid = person.get("id")
        pid_str = str(pid)
        
        stats = person_stats.get(pid, {})
        saved = persisted_actions.get(pid_str, {})
        
        # Calculate days since last touch
        last_web_str = stats.get("last_website_visit")
        last_web = parse_datetime_safe(last_web_str)
        last_act = parse_datetime_safe(base.get("lastActivity"))
        last_touch = last_web or last_act
        
        if last_touch:
            days_since = (now - last_touch).days
        else:
            days_since = 365
        
        # Calculate scores
        heat_score, _ = scorer.calculate_heat_score(
            website_visits_7d=int(stats.get("website_visits_last_7", 0)),
            properties_viewed_7d=int(stats.get("properties_viewed_last_7", 0)),
            properties_favorited=int(stats.get("properties_favorited", 0)),
            properties_shared=int(stats.get("properties_shared", 0)),
            calls_inbound=int(stats.get("calls_inbound", 0)),
            texts_inbound=int(stats.get("texts_inbound", 0)),
            days_since_last_touch=days_since
        )
        
        value_score, _ = scorer.calculate_value_score(
            avg_price_viewed=float(stats.get("avg_price_viewed", 0) or 0),
            price_std_dev=float(stats.get("price_view_std_dev", 0) or 0),
            max_avg_price=max_avg_price
        )
        
        relationship_score, _ = scorer.calculate_relationship_score(
            calls_inbound=int(stats.get("calls_inbound", 0)),
            calls_outbound=int(stats.get("calls_outbound", 0)),
            texts_inbound=int(stats.get("texts_inbound", 0)),
            texts_total=int(stats.get("texts_total", 0))
        )
        
        priority_score, _ = scorer.calculate_priority_score(
            heat_score=heat_score,
            value_score=value_score,
            relationship_score=relationship_score,
            stage=base.get("stage", "")
        )
        
        # Build row
        row = [
            base.get("id"),
            base.get("firstName"),
            base.get("lastName"),
            base.get("stage"),
            base.get("source"),
            base.get("leadTypeTags"),
            base.get("created"),
            base.get("updated"),
            base.get("ownerId"),
            base.get("primaryEmail"),
            base.get("primaryPhone"),
            base.get("company"),
            base.get("website"),
            base.get("lastActivity"),
            last_web_str,
            stats.get("avg_price_viewed"),
            stats.get("website_visits", 0),
            stats.get("properties_viewed", 0),
            stats.get("properties_favorited", 0),
            stats.get("properties_shared", 0),
            stats.get("calls_outbound", 0),
            stats.get("calls_inbound", 0),
            stats.get("texts_total", 0),
            stats.get("texts_inbound", 0),
            0,  # emails_received (not implemented)
            0,  # emails_sent (not implemented)
            heat_score,
            value_score,
            relationship_score,
            priority_score,
            saved.get("next_action", ""),
            saved.get("next_action_date", ""),
        ]
        
        rows.append(row)
        
        if (i + 1) % 100 == 0:
            logger.info(f"  Processed {i + 1}/{len(people)} contacts")
    
    logger.info(f"✓ Built {len(rows)} contact rows")
    return rows


def build_top_n_by_column(
    contact_rows: List[List],
    column_name: str,
    n: int = 20
) -> List[List]:
    """Sort and return top N rows by column value"""
    try:
        idx = CONTACTS_HEADER.index(column_name)
    except ValueError:
        logger.error(f"Column {column_name} not found in header")
        return []
    
    usable = []
    for row in contact_rows:
        val = row[idx]
        if val in (None, "", "None"):
            continue
        try:
            num = float(val)
            usable.append((num, row))
        except (TypeError, ValueError):
            continue
    
    usable.sort(key=lambda t: t[0], reverse=True)
    top = [r for _, r in usable[:n]]
    
    logger.info(f"Built top {n} by {column_name}: {len(top)} rows")
    return top


def build_call_list_rows(
    contact_rows: List[List],
    max_rows: int = 50
) -> List[List]:
    """Build daily call list"""
    logger.info("Building call list...")
    
    idx = {name: i for i, name in enumerate(CONTACTS_HEADER)}
    
    prio_i = idx["priority_score"]
    stage_i = idx["stage"]
    action_i = idx["next_action"]
    date_i = idx["next_action_date"]
    
    today = date.today()
    candidates = []
    
    for row in contact_rows:
        # Stage filter
        stage = (row[stage_i] or "").strip()
        if stage in ("Closed", "Trash"):
            continue
        
        # Priority filter
        try:
            prio = float(row[prio_i] or 0)
        except (ValueError, TypeError):
            prio = 0.0
        
        if prio < 45:
            continue
        
        # Next action filter
        action = (row[action_i] or "").strip()
        if action and action.lower() != "call":
            continue
        
        # Date filter
        date_str = (row[date_i] or "").strip()
        due = parse_date_safe(date_str)
        if due and due > today:
            continue
        
        # Sort key
        overdue_rank = 1
        due_sort = 99999999
        if due:
            overdue_rank = 0
            due_sort = due.toordinal()
        
        candidates.append((prio, overdue_rank, due_sort, row))
    
    candidates.sort(key=lambda t: (-t[0], t[1], t[2]))
    result = [t[3] for t in candidates[:max_rows]]
    
    logger.info(f"✓ Built call list: {len(result)} contacts")
    return result


def format_contacts_sheet(spreadsheet, worksheet, num_data_rows: int):
    """Apply formatting to contacts sheet"""
    try:
        worksheet.freeze(rows=1)
        logger.debug("Froze header row")
    except Exception as e:
        logger.warning(f"Could not freeze header: {e}")
    
    try:
        worksheet.set_basic_filter()
        logger.debug("Applied basic filter")
    except Exception as e:
        logger.warning(f"Could not set filter: {e}")


def reorder_worksheets(spreadsheet):
    """Reorder worksheets with main tabs first, backups last"""
    logger.info("Reordering worksheets...")
    
    worksheets = spreadsheet.worksheets()
    
    main_order = [
        "Contacts",
        "Call List Today",
        "Top Priority 20",
        "Top Value 20",
        "Top Heat 20",
    ]
    
    def is_backup(title):
        if len(title) != 11 or "." not in title:
            return False
        try:
            datetime.strptime(title, "%y%m%d.%H%M")
            return True
        except ValueError:
            return False
    
    main_tabs = []
    backup_tabs = []
    other_tabs = []
    
    for ws in worksheets:
        if ws.title in main_order:
            main_tabs.append(ws)
        elif is_backup(ws.title):
            backup_tabs.append(ws)
        else:
            other_tabs.append(ws)
    
    # Sort main tabs by desired order
    main_sorted = []
    for title in main_order:
        for ws in main_tabs:
            if ws.title == title:
                main_sorted.append(ws)
    
    backup_sorted = sorted(backup_tabs, key=lambda w: w.title)
    new_order = main_sorted + other_tabs + backup_sorted
    
    if new_order and new_order != worksheets:
        try:
            spreadsheet.reorder_worksheets(new_order)
            logger.info("✓ Worksheets reordered")
        except Exception as e:
            logger.warning(f"Could not reorder worksheets: {e}")


# =========================================================================
# EMAIL REPORTING
# =========================================================================

def send_top_priority_email(
    contact_rows: List[List],
    top_priority_rows: List[List],
    daily_stats: Dict
):
    """Send email report with top priority contacts"""
    if not Config.SMTP_ENABLED:
        logger.info("SMTP disabled, skipping email")
        return
    
    logger.info("Preparing email report...")
    
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    # Build email content
    subject = f"{Config.EMAIL_SUBJECT_PREFIX} Top Priority List - {datetime.now().strftime('%Y-%m-%d')}"
    
    body_lines = [
        "<html><body style='font-family: Arial, sans-serif;'>",
        f"<h2>Daily FUB Brief - {datetime.now().strftime('%B %d, %Y')}</h2>",
        "",
        "<h3>📊 Today's Activity</h3>",
        "<ul>",
        f"<li>Total Events: <strong>{daily_stats.get('total_events_today', 0)}</strong></li>",
        f"<li>Website Visits: <strong>{daily_stats.get('website_visits_today', 0)}</strong></li>",
        f"<li>Properties Viewed: <strong>{daily_stats.get('properties_viewed_today', 0)}</strong></li>",
        f"<li>Unique Visitors: <strong>{daily_stats.get('unique_visitors_today', 0)}</strong></li>",
        "</ul>",
        "",
    ]
    
    # Top active leads today
    if daily_stats.get('top_active_leads'):
        body_lines.extend([
            "<h3>🔥 Most Active Today</h3>",
            "<ol>",
        ])
        for lead in daily_stats['top_active_leads']:
            body_lines.append(f"<li>{lead['name']} - {lead['activity_count']} activities</li>")
        body_lines.append("</ol>")
    
    # Top priority contacts
    idx = {name: i for i, name in enumerate(CONTACTS_HEADER)}
    
    body_lines.extend([
        "",
        f"<h3>⭐ Top {len(top_priority_rows)} Priority Contacts</h3>",
        "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>",
        "<tr style='background-color: #4CAF50; color: white;'>",
        "<th>Name</th>",
        "<th>Stage</th>",
        "<th>Priority Score</th>",
        "<th>Heat</th>",
        "<th>Value</th>",
        "<th>Phone</th>",
        "</tr>",
    ])
    
    for row in top_priority_rows[:20]:
        name = f"{row[idx['firstName']]} {row[idx['lastName']]}".strip()
        stage = row[idx['stage']] or ""
        priority = row[idx['priority_score']] or 0
        heat = row[idx['heat_score']] or 0
        value = row[idx['value_score']] or 0
        phone = row[idx['primaryPhone']] or ""
        
        body_lines.append(
            f"<tr>"
            f"<td>{name}</td>"
            f"<td>{stage}</td>"
            f"<td style='text-align: center;'><strong>{priority}</strong></td>"
            f"<td style='text-align: center;'>{heat}</td>"
            f"<td style='text-align: center;'>{value}</td>"
            f"<td>{phone}</td>"
            f"</tr>"
        )
    
    body_lines.extend([
        "</table>",
        "",
        "<p style='margin-top: 30px; color: #666;'>",
        "This automated report was generated by your FUB to Sheets integration.",
        "</p>",
        "</body></html>",
    ])
    
    html_body = "\n".join(body_lines)
    
    # Send email
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"EUG Morning FUB Brief <{Config.SMTP_USERNAME}>"
        msg["To"] = Config.EMAIL_TO
        
        msg.attach(MIMEText(html_body, "html"))
        
        with smtplib.SMTP(Config.SMTP_SERVER, Config.SMTP_PORT) as server:
            server.starttls()
            server.login(Config.SMTP_USERNAME, Config.SMTP_PASSWORD)
            server.send_message(msg)
        
        logger.info(f"✓ Email sent to {Config.EMAIL_TO}")
    
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


# =========================================================================
# MAIN EXECUTION
# =========================================================================

def main():
    """Main execution function"""
    start_time = time.time()
    
    try:
        logger.info("=" * 70)
        logger.info("FUB TO SHEETS v2.0 - Starting sync")
        logger.info("=" * 70)
        
        # Validate configuration
        Config.validate()
        logger.info("✓ Configuration validated")
        
        # Initialize FUB client
        fub = FUBClient()
        
        # Fetch data from FUB
        people = fub.fetch_people()
        calls = fub.fetch_calls()
        texts = fub.fetch_text_messages_parallel(people)
        events = fub.fetch_events()
        
        # Build lookups
        people_by_id = {str(p.get("id")): p for p in people if p.get("id")}
        
        # Compute statistics
        daily_stats = compute_daily_activity_stats(events, people_by_id)
        person_stats = build_person_stats(calls, texts, events)
        
        # Initialize Google Sheets
        logger.info("Connecting to Google Sheets...")
        gc = get_sheets_client()
        sh = gc.open_by_key(Config.GOOGLE_SHEET_ID)
        logger.info(f"✓ Connected to sheet: {sh.title}")
        
        # Read persisted actions from existing Contacts sheet
        persisted_actions = {}
        try:
            existing_ws = sh.worksheet("Contacts")
            existing_values = existing_ws.get_all_values()
            if existing_values:
                header = existing_values[0]
                try:
                    id_idx = header.index("id")
                    action_idx = header.index("next_action")
                    date_idx = header.index("next_action_date")
                    
                    for row in existing_values[1:]:
                        if id_idx < len(row):
                            pid = row[id_idx]
                            if pid:
                                rec = {}
                                if action_idx < len(row):
                                    rec["next_action"] = row[action_idx]
                                if date_idx < len(row):
                                    rec["next_action_date"] = row[date_idx]
                                if rec:
                                    persisted_actions[str(pid)] = rec
                    
                    logger.info(f"✓ Loaded {len(persisted_actions)} persisted actions")
                except ValueError:
                    logger.warning("Could not find action columns in existing sheet")
        except gspread.exceptions.WorksheetNotFound:
            logger.info("No existing Contacts sheet found")
        
        # Build contact rows
        contact_rows = build_contact_rows(people, person_stats, persisted_actions)
        
        # Create backup with timestamp
        backup_name = datetime.now().strftime("%y%m%d.%H%M")
        logger.info(f"Creating backup: {backup_name}")
        backup_ws = sh.add_worksheet(
            title=backup_name,
            rows=len(contact_rows) + 1,
            cols=len(CONTACTS_HEADER)
        )
        write_table_to_worksheet(backup_ws, CONTACTS_HEADER, contact_rows)
        
        # Update main Contacts sheet
        contacts_ws = get_or_create_worksheet(sh, "Contacts")
        write_table_to_worksheet(contacts_ws, CONTACTS_HEADER, contact_rows)
        format_contacts_sheet(sh, contacts_ws, len(contact_rows))
        
        # Build and write call list
        call_list_rows = build_call_list_rows(contact_rows, max_rows=50)
        call_list_ws = get_or_create_worksheet(sh, "Call List Today")
        write_table_to_worksheet(call_list_ws, CONTACTS_HEADER, call_list_rows)
        
        # Build and write top lists
        top_priority = build_top_n_by_column(contact_rows, "priority_score", n=20)
        top_priority_ws = get_or_create_worksheet(sh, "Top Priority 20")
        write_table_to_worksheet(top_priority_ws, CONTACTS_HEADER, top_priority)
        
        top_value = build_top_n_by_column(contact_rows, "value_score", n=20)
        top_value_ws = get_or_create_worksheet(sh, "Top Value 20")
        write_table_to_worksheet(top_value_ws, CONTACTS_HEADER, top_value)
        
        top_heat = build_top_n_by_column(contact_rows, "heat_score", n=20)
        top_heat_ws = get_or_create_worksheet(sh, "Top Heat 20")
        write_table_to_worksheet(top_heat_ws, CONTACTS_HEADER, top_heat)
        
        # Reorder worksheets
        reorder_worksheets(sh)
        
        # Send email report
        send_top_priority_email(contact_rows, top_priority, daily_stats)
        
        # Summary
        elapsed = time.time() - start_time
        logger.info("=" * 70)
        logger.info("SYNC COMPLETED SUCCESSFULLY")
        logger.info(f"  Total contacts: {len(contact_rows)}")
        logger.info(f"  Call list: {len(call_list_rows)} contacts")
        logger.info(f"  Top priority: {len(top_priority)} contacts")
        logger.info(f"  Runtime: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Process interrupted by user")
        sys.exit(1)
    
    except Exception as e:
        logger.error("=" * 70)
        logger.error("FATAL ERROR")
        logger.error("=" * 70)
        logger.error(str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
