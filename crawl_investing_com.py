#!/usr/bin/env python3
from __future__ import annotations
"""
Investing.com homepage crawler & scraper — archives news articles from
investing.com into structured markdown + YAML metadata with downloaded images.

This site blocks sitemap and robots.txt (403), so we use a BFS homepage crawl
starting from https://www.investing.com/news/.

Usage:
    python3 crawl_investing_com.py                  # incremental run
    python3 crawl_investing_com.py --force           # re-fetch everything
    python3 crawl_investing_com.py --slug X          # fetch only slug X
    python3 crawl_investing_com.py --recrawl         # re-discover URLs
    python3 crawl_investing_com.py --max-pages 200   # limit crawl pages
    python3 crawl_investing_com.py --list-urls       # crawl only, print URLs

Output: investing_com/<slug>/meta.yaml + content.md, investing_com/images/
Dependencies: pip3 install requests beautifulsoup4 pyyaml lxml
"""

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
_MISSING = []
for _mod, _pkg in [
    ("requests", "requests"),
    ("bs4", "beautifulsoup4"),
    ("yaml", "pyyaml"),
    ("lxml", "lxml"),
]:
    try:
        __import__(_mod)
    except ImportError:
        _MISSING.append(_pkg)
if _MISSING:
    print(
        "Missing dependencies. Install with:\n"
        f"  pip3 install --user --break-system-packages {' '.join(_MISSING)}"
    )
    raise SystemExit(1)

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

import argparse
import collections
import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, unquote

import requests
import yaml
from bs4 import BeautifulSoup, NavigableString, Tag

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "www.investing.com"
START_URL = "https://www.investing.com/news/"
BASE_DIR = Path("/Users/shakirfattani/kaam/news-scrappers/investing_com")
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"
DISCOVERED_URLS_FILE = BASE_DIR / ".discovered-urls.json"

DEFAULT_MAX_PAGES = 500
DEFAULT_DEPTH = 10
CRAWL_WORKERS = 3
SCRAPE_WORKERS = 5
CRAWL_DELAY = 1.0
REQUEST_DELAY = 1.0

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Paths we want to follow during crawl (content sections)
CRAWL_SEED_PATHS = [
    "/news/",
    "/analysis/",
    "/stock-market-news/",
    "/forex-news/",
    "/commodities-news/",
    "/cryptocurrency-news/",
    "/economic-indicators/",
    "/earnings/",
]

# Patterns to recognise content pages
CONTENT_PATH_PATTERNS = [
    "/news/", "/articles/", "/press-release/", "/blogs/",
    "/insights/", "/market-insights/", "/latest-insights/", "/wealth-insights/",
    "/posts/", "/newsroom/", "/announcements/", "/banking-mantra/",
    "/opinion/", "/future/", "/business/", "/lifestyle/",
    "/life-and-living/", "/your-money/", "/awareness/", "/research/",
    "/reports/", "/market/", "/mediacenter/", "/numbers-and-statistics/",
    "/publications/", "/spotlight/", "/economy/", "/stock-market/",
    "/forex-news/", "/commodities-news/", "/cryptocurrency-news/", "/world-news/",
    "/economic-indicators/", "/earnings/", "/analysis/", "/topic/",
    "/speeches/", "/review/", "/originals/", "/news-release/",
]

# Investing.com content selectors — tried in order
CONTENT_SELECTORS = [
    "div.articlePage",
    "div.WYSIWYG",
    "div[class*='articlePage']",
    "div[class*='article_WYSIWYG']",
    "div[data-test='article-body']",
    "div.article_WYSIWYG__O0255",
    "div[class*='WYSIWYG']",
    "div.contentSectionDetails",
    "div.articleBodyContent",
    "div.article-body",
    "div[class*='ArticleBody']",
    "div[class*='article-body']",
    "div.body-content",
    "div.story-body",
    "div.content-area",
    "article .content",
    "div.article-content",
    "article",
    "main",
    "div.content",
    "div#content",
]

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup", ".subscription-widget",
    ".comments", ".comment-section", ".author-bio",
    ".breadcrumb", ".pagination",
    ".ad", ".advertisement", "[class*='promo']",
    "[class*='banner']", "[class*='popup']", "[class*='modal']",
    ".disclaimer", ".cookie-banner",
    ".article-share", ".share-icons", ".social-icons",
    "[class*='RelatedStories']", "[class*='related-story']",
    "[class*='also-read']", "[class*='AlsoRead']",
    "[class*='relatedArticles']", "[class*='RelatedArticles']",
    ".tags-wrapper", ".article-tags-social",
    ".gallery-overlay", ".article-gallery-overlay",
    "script", "style", "noscript", "svg", "button", "iframe",
    "[class*='ad-']", "[class*='Ad-']", "[id*='google_ads']",
    "[class*='adUnit']", "[class*='dfp-']",
    ".login-wall", ".subscribe-wall", ".paywall",
    "[class*='paywall']", "[class*='subscribe-to-read']",
    "[class*='premium-content']",
    "[class*='InlineSignup']", "[class*='inlineSignup']",
    "[class*='promoBar']", "[class*='PromoBar']",
]

SKIP_URL_PATTERNS = re.compile(
    r"(/page/\d+|/search\b|/login|/signup|/register|/account|"
    r"/settings|/cart|/checkout|/api/|/graphql|/rss|/feed|"
    r"/author/|/authors/|/tag/[^/]*/?$|/tags/?$|/about-us|"
    r"/privacy|/terms|/contact|/advertise|/subscribe/?$|"
    r"/portfolio|/pro/|/brokers/|/tools/|/rates-bonds/|"
    r"/indices/|/equities/|/currencies/|/crypto/|/funds/|"
    r"/etfs/|/members/|/central-banks/|/holiday-calendar|"
    r"/economic-calendar|/technical/|/sentiment/|"
    r"\.(css|js|json|xml|rss|atom|woff2?|ttf|eot|pdf|mp4|mp3)$)",
    re.IGNORECASE,
)

# Patterns that indicate an article URL (investing.com uses numeric IDs)
# e.g. /news/stock-market-news/article-headline-12345
ARTICLE_ID_RE = re.compile(r"-(\d{4,})$")

DATE_SEGMENT_RE = re.compile(r"^(19|20)\d{2}$")
NUMERIC_ONLY_RE = re.compile(r"^\d+$")

# WordPress image proxy pattern
WP_PROXY_RE = re.compile(r"^https?://i[0-3]\.wp\.com/(.+)")

# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "DNT": "1",
})

# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict[str, Any]:
    if FETCH_STATE_FILE.exists():
        with open(FETCH_STATE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_state(state: dict[str, Any]) -> None:
    FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FETCH_STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)


def load_discovered_urls() -> dict[str, Any]:
    if DISCOVERED_URLS_FILE.exists():
        with open(DISCOVERED_URLS_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_discovered_urls(discovered: dict[str, Any]) -> None:
    DISCOVERED_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DISCOVERED_URLS_FILE, "w", encoding="utf-8") as fh:
        json.dump(discovered, fh, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# URL utilities
# ---------------------------------------------------------------------------

def normalise_url(url: str) -> str:
    """Normalise a URL: strip fragments, trailing slashes, query params."""
    parsed = urlparse(url)
    # Keep only scheme, netloc, path
    path = parsed.path.rstrip("/")
    if not path:
        path = "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def is_internal_url(url: str) -> bool:
    """Check if URL belongs to investing.com."""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    return host == DOMAIN or host.endswith(f".{DOMAIN}")


def is_static_asset(url: str) -> bool:
    """Check if URL points to a static asset."""
    parsed = urlparse(url)
    path = parsed.path.lower()
    static_exts = (
        ".css", ".js", ".json", ".xml", ".rss", ".atom",
        ".woff", ".woff2", ".ttf", ".eot",
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".avif",
        ".mp4", ".mp3", ".pdf", ".ico",
    )
    return any(path.endswith(ext) for ext in static_exts)


def is_non_content_scheme(url: str) -> bool:
    """Check for non-HTTP schemes."""
    return url.startswith(("mailto:", "tel:", "javascript:", "data:", "#"))


def should_skip_url(url: str) -> bool:
    """Determine if a URL should be skipped entirely."""
    if is_non_content_scheme(url):
        return True
    if not is_internal_url(url):
        return True
    if is_static_asset(url):
        return True

    parsed = urlparse(url)
    path = parsed.path

    if SKIP_URL_PATTERNS.search(path):
        return True

    # Skip query-heavy URLs (investing.com doesn't use query params for articles)
    if parsed.query:
        return True

    return False


def is_content_section(url: str) -> bool:
    """Check if URL falls within a content section we want to crawl."""
    parsed = urlparse(url)
    path = parsed.path
    for pattern in CONTENT_PATH_PATTERNS:
        if pattern in path:
            return True
    return False


# ---------------------------------------------------------------------------
# BFS Crawl
# ---------------------------------------------------------------------------

def extract_links(html: str, base_url: str) -> list[str]:
    """Extract all internal links from HTML."""
    soup = BeautifulSoup(html, "lxml")
    links: list[str] = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()

        if is_non_content_scheme(href):
            continue

        # Resolve relative URLs
        absolute = urljoin(base_url, href)
        normalised = normalise_url(absolute)

        if not should_skip_url(normalised) and is_content_section(normalised):
            links.append(normalised)

    return links


def crawl(
    start_url: str,
    max_pages: int = DEFAULT_MAX_PAGES,
    delay: float = CRAWL_DELAY,
) -> dict[str, Any]:
    """
    BFS crawl from start_url, return dict of discovered URLs with metadata.
    """
    queue: collections.deque[tuple[str, int]] = collections.deque()
    visited: set[str] = set()
    discovered: dict[str, Any] = {}

    # Seed the queue with the start URL and all section pages
    seed_urls = [start_url]
    for path in CRAWL_SEED_PATHS:
        seed_urls.append(f"https://{DOMAIN}{path}")

    for seed in seed_urls:
        norm = normalise_url(seed)
        if norm not in visited:
            queue.append((norm, 0))
            visited.add(norm)

    pages_fetched = 0

    print(f"[crawl] Starting BFS from {start_url} (max_pages={max_pages})")

    while queue and pages_fetched < max_pages:
        url, depth = queue.popleft()

        if depth > DEFAULT_DEPTH:
            continue

        try:
            time.sleep(delay)
            resp = SESSION.get(url, timeout=30, allow_redirects=True)
            resp.raise_for_status()
            pages_fetched += 1

            # Track the final URL after redirects
            final_url = normalise_url(resp.url)

            # Extract links
            new_links = extract_links(resp.text, final_url)

            for link in new_links:
                if link not in visited:
                    visited.add(link)
                    queue.append((link, depth + 1))

            # Classify this URL
            url_type = classify_url(resp.text, url)
            discovered[url] = {
                "type": url_type,
                "depth": depth,
                "slug": None,  # assigned later for content pages
            }

            if pages_fetched % 25 == 0:
                content_count = sum(
                    1 for v in discovered.values() if v["type"] == "content"
                )
                print(
                    f"[crawl] {pages_fetched} pages fetched, "
                    f"{len(discovered)} discovered, "
                    f"{content_count} content pages"
                )

        except Exception as exc:
            print(f"[crawl] Failed {url}: {exc}")
            discovered[url] = {"type": "error", "depth": depth, "slug": None}

    content_count = sum(1 for v in discovered.values() if v["type"] == "content")
    print(
        f"[crawl] Done: {pages_fetched} pages fetched, "
        f"{len(discovered)} total URLs, {content_count} content pages"
    )

    return discovered


def classify_url(html: str, url: str) -> str:
    """
    Classify a URL as 'content', 'listing', or 'skip' based on HTML analysis.
    """
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Skip utility pages
    if SKIP_URL_PATTERNS.search(path):
        return "skip"

    # Must be in a content section
    if not is_content_section(url):
        return "skip"

    soup = BeautifulSoup(html, "lxml")

    # Check for article indicators
    has_article_meta = False

    # og:type = article
    og_type = soup.find("meta", property="og:type")
    if og_type and og_type.get("content", "").lower() == "article":
        has_article_meta = True

    # article:published_time
    pub_time = soup.find("meta", property="article:published_time")
    if pub_time:
        has_article_meta = True

    # JSON-LD datePublished
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script_tag.string or "")
            if isinstance(ld, list):
                ld = ld[0] if ld else {}
            if isinstance(ld, dict):
                if ld.get("datePublished") or ld.get("@type") in (
                    "NewsArticle", "Article", "BlogPosting", "Report",
                ):
                    has_article_meta = True
        except (json.JSONDecodeError, TypeError):
            pass

    # Check for a single h1 (article title)
    h1_tags = soup.find_all("h1")
    has_single_h1 = len(h1_tags) == 1

    # Check word count in main content area
    container = _find_content_container(soup)
    if container:
        text = container.get_text(separator=" ", strip=True)
        word_count = len(text.split())
    else:
        text = soup.get_text(separator=" ", strip=True)
        word_count = len(text.split())

    # Investing.com article URLs typically end with a numeric ID
    segments = [s for s in path.split("/") if s]
    has_article_slug = len(segments) >= 2

    # Check URL for article ID pattern (e.g., headline-12345)
    if segments:
        last_seg = segments[-1]
        has_numeric_id = bool(ARTICLE_ID_RE.search(last_seg))
    else:
        has_numeric_id = False

    # Content page: has article metadata OR has substantial content with slug
    if has_article_meta and word_count >= 100:
        return "content"

    if has_numeric_id and word_count >= 100:
        return "content"

    if has_single_h1 and has_article_slug and word_count >= 200:
        return "content"

    # Listing page: section root or link-heavy
    if word_count < 200:
        # Check if section-only URL
        section_path = "/" + "/".join(segments) + "/"
        for pat in CONTENT_PATH_PATTERNS:
            if section_path == pat:
                return "listing"

        # Link-heavy pages
        links = soup.select("article a, main a, h2 a, h3 a")
        if len(links) > 10:
            return "listing"

    # If content has article meta but low word count, still treat as content
    if has_article_meta:
        return "content"

    # Default to listing for section pages, skip for everything else
    if is_content_section(url) and has_article_slug:
        if word_count >= 200:
            return "content"
        return "listing"

    return "skip"


def _find_content_container(soup: BeautifulSoup) -> Tag | None:
    """Find the main article content container."""
    for selector in CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            return el
    return None


# ---------------------------------------------------------------------------
# URL classification
# ---------------------------------------------------------------------------

def is_article_url(url: str) -> bool:
    """Return True if the URL looks like an article, not a listing."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    if SKIP_URL_PATTERNS.search(path):
        return False

    segments = [s for s in path.split("/") if s]
    if len(segments) < 2:
        return False

    # Pure section pages
    section_path = "/" + "/".join(segments) + "/"
    for pat in CONTENT_PATH_PATTERNS:
        if section_path == pat:
            return False

    # Must have some alphabetic content in the last segment
    last_seg = segments[-1]
    if not re.search(r"[a-zA-Z]", last_seg):
        return False

    return True


def detect_content_type(url_path: str) -> tuple[str, str | None]:
    """
    Extract content-type and category from URL path.

    /news/forex-news/slug         -> ('forex-news', 'news')
    /analysis/stock-market/slug   -> ('stock-market', 'analysis')
    /news/slug                    -> ('news', None)
    /earnings/slug                -> ('earnings', None)
    """
    segments = [s for s in url_path.strip("/").split("/") if s]
    if len(segments) < 2:
        return (segments[0] if segments else "general", None)

    path_segments = segments[:-1]

    # Strip date segments
    path_segments = [
        s for s in path_segments
        if not DATE_SEGMENT_RE.match(s) and not (len(s) <= 2 and s.isdigit())
    ]

    if not path_segments:
        return ("general", None)

    if len(path_segments) >= 2:
        return (path_segments[-1], path_segments[-2])

    return (path_segments[0], None)


def generate_slug(url_path: str, existing_slugs: set[str]) -> str:
    """
    Generate a slug from the last meaningful path segment.

    /news/headline-here-12345    -> 'headline-here-12345'
    /news/2026/07/headline-here  -> 'headline-here'
    /analysis/stock-report       -> 'stock-report'
    """
    segments = [s for s in url_path.strip("/").split("/") if s]
    if not segments:
        return "index"

    # Walk from the end, skip date-like and purely numeric segments
    candidate = None
    for seg in reversed(segments):
        if DATE_SEGMENT_RE.match(seg):
            continue
        if len(seg) <= 2 and seg.isdigit():
            continue
        candidate = seg
        break

    if candidate is None:
        candidate = segments[-1]

    # Normalize
    slug = candidate.lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    slug = slug.strip("-")

    if not slug:
        slug = "article"

    # Handle collisions
    base_slug = slug
    counter = 2
    while slug in existing_slugs:
        slug = f"{base_slug}-{counter}"
        counter += 1

    return slug


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------

def extract_date(
    soup: BeautifulSoup,
    url: str,
    headers: dict[str, str] | None = None,
) -> str | None:
    """Extract publish date using priority chain."""

    # 1. article:published_time
    meta_pub = soup.find("meta", property="article:published_time")
    if meta_pub and meta_pub.get("content"):
        return _normalize_date(meta_pub["content"])

    # 2. meta name=date / publish-date
    for name in ("date", "publish-date", "publish_date", "pubdate"):
        meta_d = soup.find("meta", attrs={"name": name})
        if meta_d and meta_d.get("content"):
            return _normalize_date(meta_d["content"])

    # 3. <time datetime>
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        return _normalize_date(time_el["datetime"])

    # 4. class containing "date" or "timestamp"
    for attr_pattern in ["date", "timestamp", "publish"]:
        el = soup.find(attrs={"class": re.compile(attr_pattern, re.I)})
        if el:
            text = el.get_text(strip=True)
            parsed = _try_parse_date_text(text)
            if parsed:
                return parsed

    # 5. JSON-LD datePublished
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script_tag.string or "")
            if isinstance(ld, list):
                ld = ld[0] if ld else {}
            if isinstance(ld, dict):
                dp = ld.get("datePublished")
                if dp:
                    return _normalize_date(dp)
        except (json.JSONDecodeError, TypeError):
            pass

    # 6. URL path date segments
    m = re.search(r"/(20\d{2})/(\d{1,2})/(\d{1,2})/", url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 7. HTTP Last-Modified
    if headers:
        lm = headers.get("Last-Modified")
        if lm:
            return _normalize_date(lm)

    return None


def _normalize_date(raw: str) -> str:
    """Normalize various date formats to YYYY-MM-DD."""
    raw = raw.strip()
    # ISO-8601 variants
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", raw)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # RFC 2822 / HTTP date
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    # Fallback
    if len(raw) >= 10 and raw[4] == "-":
        return raw[:10]
    return raw[:10] if len(raw) >= 10 else raw


def _try_parse_date_text(text: str) -> str | None:
    """Try to parse a human-readable date string."""
    months = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12",
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "jun": "06", "jul": "07", "aug": "08", "sep": "09",
        "oct": "10", "nov": "11", "dec": "12",
    }
    text_lower = text.lower().strip()
    # Month DD, YYYY
    m = re.search(r"([a-z]+)\s+(\d{1,2}),?\s+(\d{4})", text_lower)
    if m and m.group(1) in months:
        return f"{m.group(3)}-{months[m.group(1)]}-{int(m.group(2)):02d}"
    # DD Month YYYY
    m = re.search(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", text_lower)
    if m and m.group(2) in months:
        return f"{m.group(3)}-{months[m.group(2)]}-{int(m.group(1)):02d}"
    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------

def extract_tags(soup: BeautifulSoup) -> list[str]:
    """Extract tags from multiple sources, deduplicate."""
    tags: set[str] = set()

    # 1. meta keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        for t in meta_kw["content"].split(","):
            t = t.strip().lower()
            if t and len(t) < 80:
                tags.add(t)

    # 2. article:tag (multiple)
    for meta_tag in soup.find_all("meta", property="article:tag"):
        val = (meta_tag.get("content") or "").strip().lower()
        if val:
            tags.add(val)

    # 3. JSON-LD keywords
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script_tag.string or "")
            if isinstance(ld, list):
                ld = ld[0] if ld else {}
            if isinstance(ld, dict):
                kw = ld.get("keywords")
                if isinstance(kw, list):
                    for k in kw:
                        tags.add(str(k).strip().lower())
                elif isinstance(kw, str):
                    for k in kw.split(","):
                        k = k.strip().lower()
                        if k:
                            tags.add(k)
                for field in ("about", "mentions"):
                    items = ld.get(field, [])
                    if isinstance(items, dict):
                        items = [items]
                    for item in items:
                        if isinstance(item, dict):
                            name = item.get("name", "").strip().lower()
                            if name:
                                tags.add(name)
        except (json.JSONDecodeError, TypeError):
            pass

    # 4. Visible tag links
    for selector in [
        'a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a",
        '[class*="tag-link"]', '[class*="Tag-link"]',
        ".topic-tag a", ".article-tag a",
        '[class*="articleTags"] a', '[class*="ArticleTags"] a',
    ]:
        for a_tag in soup.select(selector):
            text = a_tag.get_text(strip=True).lower()
            if text and len(text) < 80:
                tags.add(text)

    # 5. Investing.com specific: breadcrumb-based categories
    for a_tag in soup.select(".breadcrumbs a, [class*='breadcrumb'] a"):
        text = a_tag.get_text(strip=True).lower()
        if text and len(text) < 80 and text not in ("home", "investing.com"):
            tags.add(text)

    return sorted(tags)


# ---------------------------------------------------------------------------
# Image downloading
# ---------------------------------------------------------------------------

def download_image(img_url: str, slug: str) -> str | None:
    """
    Download image to images/ dir. Return local filename or None on failure.
    Handles WordPress image proxies (i0-i3.wp.com).
    """
    if not img_url or img_url.startswith("data:"):
        return None

    # Resolve WordPress proxy URLs
    wp_match = WP_PROXY_RE.match(img_url)
    if wp_match:
        original_path = wp_match.group(1)
    else:
        original_path = urlparse(img_url).path

    # Determine extension
    ext_path = original_path.split("?")[0]
    ext = os.path.splitext(ext_path)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp"):
        ext = ".jpg"

    # Build filename
    url_hash = hashlib.md5(img_url.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    filepath = IMAGES_DIR / filename

    if filepath.exists():
        return filename

    try:
        resp = SESSION.get(img_url, timeout=20, stream=True)
        resp.raise_for_status()
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb") as fh:
            for chunk in resp.iter_content(8192):
                fh.write(chunk)
        return filename
    except Exception as exc:
        print(f"[warn] Image download failed {img_url}: {exc}")
        return None


# ---------------------------------------------------------------------------
# HTML to Markdown converter
# ---------------------------------------------------------------------------

SKIP_TAGS = frozenset([
    "script", "style", "noscript", "svg", "button", "iframe",
    "nav", "input", "select", "textarea", "form",
])


def html_to_markdown(element: Tag, slug: str, depth: int = 0) -> str:
    """Recursively convert an HTML element tree to markdown."""
    if isinstance(element, NavigableString):
        text = str(element)
        if not text.strip():
            return ""
        return text.replace("\n", " ")

    if not isinstance(element, Tag):
        return ""

    tag_name = element.name.lower() if element.name else ""

    if tag_name in SKIP_TAGS:
        return ""

    # Check for noise classes/ids
    classes = " ".join(element.get("class", []))
    el_id = element.get("id", "")
    noise_patterns = [
        "related", "recommend", "social", "share", "comment", "sidebar",
        "newsletter", "subscribe", "promo", "banner", "popup", "modal",
        "ad-", "advertisement", "breadcrumb", "pagination", "cookie",
        "also-read", "AlsoRead", "RelatedStories", "login-wall",
        "paywall", "subscribe-wall", "inlineSignup", "promoBar",
    ]
    combined = f"{classes} {el_id}".lower()
    if any(p.lower() in combined for p in noise_patterns):
        return ""

    # Headings
    if tag_name in ("h1", "h2", "h3", "h4", "h5", "h6"):
        level = int(tag_name[1])
        text = _children_text(element, slug, depth)
        if text.strip():
            return f"\n\n{'#' * level} {text.strip()}\n\n"
        return ""

    # Paragraph
    if tag_name == "p":
        text = _children_text(element, slug, depth)
        if text.strip():
            return f"\n\n{text.strip()}\n\n"
        return ""

    # Bold
    if tag_name in ("strong", "b"):
        text = _children_text(element, slug, depth)
        if text.strip():
            return f"**{text.strip()}**"
        return ""

    # Italic
    if tag_name in ("em", "i"):
        text = _children_text(element, slug, depth)
        if text.strip():
            return f"*{text.strip()}*"
        return ""

    # Links
    if tag_name == "a":
        href = element.get("href", "")
        if element.find("img"):
            return _children_text(element, slug, depth)
        text = _children_text(element, slug, depth)
        if text.strip() and href:
            return f"[{text.strip()}]({href})"
        return text

    # Images
    if tag_name == "img":
        src = element.get("src") or element.get("data-src") or ""
        alt = element.get("alt", "")
        if not src:
            return ""
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = f"https://{DOMAIN}{src}"
        filename = download_image(src, slug)
        if filename:
            return f"![{alt}](../images/{filename})"
        return f"![{alt}]({src})"

    # Picture
    if tag_name == "picture":
        img = element.find("img")
        if img:
            return html_to_markdown(img, slug, depth)
        source = element.find("source")
        if source and source.get("srcset"):
            srcset = source["srcset"].split(",")[0].strip().split(" ")[0]
            if srcset.startswith("//"):
                srcset = "https:" + srcset
            filename = download_image(srcset, slug)
            if filename:
                return f"![](../images/{filename})"
        return ""

    # Figure
    if tag_name == "figure":
        parts = []
        for child in element.children:
            if isinstance(child, Tag):
                if child.name == "figcaption":
                    cap = child.get_text(strip=True)
                    if cap:
                        parts.append(f"\n*{cap}*\n")
                else:
                    parts.append(html_to_markdown(child, slug, depth))
        return "\n".join(parts)

    # Blockquote
    if tag_name == "blockquote":
        text = _children_text(element, slug, depth)
        if text.strip():
            lines = text.strip().split("\n")
            quoted = "\n".join(f"> {line}" for line in lines)
            return f"\n\n{quoted}\n\n"
        return ""

    # Pre / Code
    if tag_name == "pre":
        code_el = element.find("code")
        if code_el:
            lang = ""
            code_classes = code_el.get("class", [])
            for c in code_classes:
                if c.startswith("language-"):
                    lang = c.replace("language-", "")
                    break
            text = code_el.get_text()
            return f"\n\n```{lang}\n{text}\n```\n\n"
        return f"\n\n```\n{element.get_text()}\n```\n\n"

    if tag_name == "code" and (not element.parent or element.parent.name != "pre"):
        return f"`{element.get_text()}`"

    # Lists
    if tag_name in ("ul", "ol"):
        items = []
        for i, li in enumerate(element.find_all("li", recursive=False)):
            text = _children_text(li, slug, depth + 1)
            if text.strip():
                prefix = f"{i + 1}. " if tag_name == "ol" else "- "
                items.append(f"{prefix}{text.strip()}")
        if items:
            return "\n\n" + "\n".join(items) + "\n\n"
        return ""

    if tag_name == "li":
        return _children_text(element, slug, depth)

    # Table
    if tag_name == "table":
        return _convert_table(element, slug, depth)

    # HR
    if tag_name == "hr":
        return "\n\n---\n\n"

    # BR
    if tag_name == "br":
        return "\n"

    # Default: recurse children
    return _children_text(element, slug, depth)


def _children_text(element: Tag, slug: str, depth: int) -> str:
    parts = []
    for child in element.children:
        parts.append(html_to_markdown(child, slug, depth))
    return "".join(parts)


def _convert_table(table: Tag, slug: str, depth: int) -> str:
    """Convert an HTML table to markdown table."""
    rows: list[list[str]] = []
    for tr in table.find_all("tr"):
        cells = []
        for cell in tr.find_all(["th", "td"]):
            text = cell.get_text(strip=True).replace("|", "\\|")
            cells.append(text)
        if cells:
            rows.append(cells)

    if not rows:
        return ""

    max_cols = max(len(r) for r in rows)
    for row in rows:
        while len(row) < max_cols:
            row.append("")

    header = "| " + " | ".join(rows[0]) + " |"
    separator = "| " + " | ".join("---" for _ in rows[0]) + " |"
    body_rows = [
        "| " + " | ".join(row) + " |"
        for row in rows[1:]
    ]

    return "\n\n" + "\n".join([header, separator] + body_rows) + "\n\n"


def _collapse_newlines(text: str) -> str:
    """Collapse 3+ consecutive newlines to 2."""
    return re.sub(r"\n{3,}", "\n\n", text)


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------

def find_content_container(soup: BeautifulSoup) -> Tag | None:
    """Find the main article content container."""
    for selector in CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            return el
    return None


def strip_noise(container: Tag) -> None:
    """Remove noise elements from the content container (in-place)."""
    for selector in NOISE_SELECTORS:
        try:
            for el in container.select(selector):
                el.decompose()
        except Exception:
            pass


def is_listing_page(soup: BeautifulSoup, content_text: str) -> bool:
    """Detect if the page is a listing/index page rather than an article."""
    og_type = soup.find("meta", property="og:type")
    if og_type and og_type.get("content", "").lower() == "article":
        return False

    pub_time = soup.find("meta", property="article:published_time")
    if pub_time:
        return False

    word_count = len(content_text.split())
    if word_count < 200:
        links = soup.select("article a, main a")
        if len(links) > 10:
            return True

    title_el = soup.find("title")
    if title_el:
        title_text = title_el.get_text().lower()
        if any(kw in title_text for kw in ["archive", "all posts", "page 2", "category:"]):
            return True

    return False


# ---------------------------------------------------------------------------
# Page processing
# ---------------------------------------------------------------------------

def process_page(
    url: str,
    crawl_depth: int,
    state: dict[str, Any],
    existing_slugs: set[str],
    content_hashes: dict[str, str],
    force: bool = False,
) -> dict[str, Any] | None:
    """
    Fetch and process a single article page.
    Returns updated state entry or None on skip/failure.
    """
    parsed_url = urlparse(url)

    # Generate slug
    slug = generate_slug(parsed_url.path, existing_slugs)
    existing_slugs.add(slug)

    slug_dir = BASE_DIR / slug
    content_file = slug_dir / "content.md"
    meta_file = slug_dir / "meta.yaml"

    # Rate limiting
    time.sleep(REQUEST_DELAY)

    # Fetch page
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[fail] {slug} — {exc}")
        return None

    # Compute content hash for incremental check
    page_hash = hashlib.md5(resp.text.encode()).hexdigest()

    # Incremental check (after fetch, using content hash)
    if not force:
        stored = state.get(slug)
        if (
            stored
            and stored.get("content_hash") == page_hash
            and content_file.exists()
        ):
            print(f"[skip] {slug} — unchanged")
            return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Find content container
    container = find_content_container(soup)
    if container is None:
        print(f"[skip] {slug} — no content container found")
        return None

    # Strip noise
    strip_noise(container)

    # Convert to markdown
    md_text = html_to_markdown(container, slug)
    md_text = _collapse_newlines(md_text).strip()

    # Word count check
    word_count = len(md_text.split())
    if word_count < 200:
        if is_listing_page(soup, md_text):
            print(f"[skip] {slug} — listing page ({word_count} words)")
            return None

    # Content dedup
    content_hash = hashlib.md5(md_text.encode()).hexdigest()
    if content_hash in content_hashes:
        dup_slug = content_hashes[content_hash]
        print(f"[dedup] {slug} — duplicate of {dup_slug}")
        return None
    content_hashes[content_hash] = slug

    # Paywall detection
    truncated = False
    paywall_indicators = [
        ".login-wall", ".subscribe-wall", ".paywall",
        "[class*='paywall']", "[class*='subscribe-to-read']",
        "[class*='premium-content']", "[class*='ProPaywall']",
        "[class*='proPaywall']",
    ]
    for sel in paywall_indicators:
        if soup.select_one(sel):
            truncated = True
            break
    if word_count < 100 and not truncated:
        if soup.find(string=re.compile(r"subscribe|sign.?in|log.?in.*to.*read", re.I)):
            truncated = True

    # Extract metadata
    title = _extract_title(soup)
    brief = _extract_brief(soup)
    pub_date = extract_date(soup, url, dict(resp.headers))
    tags = extract_tags(soup)
    content_type, category = detect_content_type(parsed_url.path)

    # Build meta.yaml
    meta: dict[str, Any] = {
        "title": title,
        "publish-date": pub_date,
        "change-frequency": "unknown",
        "short-brief": brief,
        "source-url": url,
        "content-type": content_type,
        "category": category,
        "crawl-depth": crawl_depth,
        "tags": tags if tags else [],
    }
    if truncated:
        meta["truncated"] = True

    # Write files
    slug_dir.mkdir(parents=True, exist_ok=True)

    with open(meta_file, "w", encoding="utf-8") as fh:
        yaml.dump(
            meta, fh,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

    with open(content_file, "w", encoding="utf-8") as fh:
        fh.write(md_text)
        fh.write("\n")

    print(f"[done] {slug} ({word_count} words)")

    return {
        "slug": slug,
        "content_hash": page_hash,
        "md_hash": content_hash,
    }


def _extract_title(soup: BeautifulSoup) -> str:
    """Extract article title."""
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text

    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()

    title_el = soup.find("title")
    if title_el:
        text = title_el.get_text(strip=True)
        text = re.sub(r"\s*[\|–—-]\s*Investing\.com.*$", "", text)
        return text

    return "Untitled"


def _extract_brief(soup: BeautifulSoup) -> str:
    """Extract short brief / description."""
    og = soup.find("meta", property="og:description")
    if og and og.get("content"):
        return og["content"].strip()

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()

    return ""


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------

def replace_internal_links(base_dir: Path) -> int:
    """
    Scan all content.md files and replace internal investing.com links
    with local relative paths where the target slug exists locally.
    Returns count of replacements made.
    """
    slug_dirs = {
        d.name for d in base_dir.iterdir()
        if d.is_dir() and d.name != "images" and (d / "content.md").exists()
    }

    link_re = re.compile(
        r"\[([^\]]*)\]\(https?://(?:www\.)?investing\.com(/[^)]*)\)"
    )

    replacements = 0

    for slug_name in slug_dirs:
        content_file = base_dir / slug_name / "content.md"
        if not content_file.exists():
            continue

        text = content_file.read_text(encoding="utf-8")
        original = text

        def _replace_link(m: re.Match) -> str:
            nonlocal replacements
            link_text = m.group(1)
            url_path = m.group(2)
            url_path = url_path.split("?")[0].split("#")[0]
            segments = [s for s in url_path.strip("/").split("/") if s]
            if not segments:
                return m.group(0)

            for seg in reversed(segments):
                candidate = seg.lower()
                candidate = re.sub(r"[^a-z0-9\-]", "-", candidate)
                candidate = re.sub(r"-{2,}", "-", candidate).strip("-")
                if candidate in slug_dirs:
                    replacements += 1
                    return f"[{link_text}](../{candidate}/content.md)"
            return m.group(0)

        text = link_re.sub(_replace_link, text)

        if text != original:
            content_file.write_text(text, encoding="utf-8")

    return replacements


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl & scrape investing.com news articles via homepage BFS"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-fetch everything, ignoring incremental state",
    )
    parser.add_argument(
        "--slug", type=str, default=None,
        help="Fetch only a specific slug",
    )
    parser.add_argument(
        "--recrawl", action="store_true",
        help="Re-discover URLs from homepage (ignore cached .discovered-urls.json)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=DEFAULT_MAX_PAGES,
        help=f"Maximum pages to crawl (default: {DEFAULT_MAX_PAGES})",
    )
    parser.add_argument(
        "--list-urls", action="store_true",
        help="Crawl only, print discovered URLs, don't scrape",
    )
    args = parser.parse_args()

    # Ensure output dirs exist
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    state = load_state()

    # -------------------------------------------------------------------
    # Phase 1: Discover URLs (crawl or load from cache)
    # -------------------------------------------------------------------
    discovered: dict[str, Any] = {}

    if not args.recrawl and DISCOVERED_URLS_FILE.exists() and not args.slug:
        print("[info] Loading cached discovered URLs from .discovered-urls.json")
        discovered = load_discovered_urls()
        content_count = sum(
            1 for v in discovered.values() if v.get("type") == "content"
        )
        print(
            f"[info] Loaded {len(discovered)} URLs, "
            f"{content_count} content pages"
        )
    elif not args.slug:
        print("[info] Starting homepage crawl...")
        discovered = crawl(START_URL, max_pages=args.max_pages)
        save_discovered_urls(discovered)
        print("[info] Saved discovered URLs to .discovered-urls.json")

    # Handle --list-urls
    if args.list_urls:
        content_urls = sorted(
            url for url, meta in discovered.items()
            if meta.get("type") == "content"
        )
        listing_urls = sorted(
            url for url, meta in discovered.items()
            if meta.get("type") == "listing"
        )
        skip_urls = sorted(
            url for url, meta in discovered.items()
            if meta.get("type") in ("skip", "error")
        )

        print(f"\n{'=' * 60}")
        print(f"Content pages ({len(content_urls)}):")
        for u in content_urls:
            print(f"  {u}")
        print(f"\nListing pages ({len(listing_urls)}):")
        for u in listing_urls:
            print(f"  {u}")
        print(f"\nSkipped ({len(skip_urls)}):")
        for u in skip_urls:
            print(f"  {u}")
        print(f"{'=' * 60}")
        return

    # -------------------------------------------------------------------
    # Phase 2: Scrape content pages
    # -------------------------------------------------------------------
    # Build list of URLs to scrape
    if args.slug:
        # Find URL matching slug in state or discovered
        urls_to_scrape: list[tuple[str, int]] = []
        for url, meta in discovered.items():
            if args.slug in url:
                urls_to_scrape.append((url, meta.get("depth", 0)))
        if not urls_to_scrape:
            # Try to construct URL from slug
            candidate_url = f"https://{DOMAIN}/news/{args.slug}"
            urls_to_scrape.append((candidate_url, 0))
        print(f"[info] Filtered to {len(urls_to_scrape)} URLs matching --slug '{args.slug}'")
    else:
        urls_to_scrape = [
            (url, meta.get("depth", 0))
            for url, meta in discovered.items()
            if meta.get("type") == "content"
        ]
        print(f"[info] {len(urls_to_scrape)} content pages to scrape")

    if not urls_to_scrape:
        print("[warn] No content pages to scrape. Exiting.")
        return

    # Pre-populate existing slugs from filesystem
    existing_slugs: set[str] = set()
    if BASE_DIR.exists():
        for d in BASE_DIR.iterdir():
            if d.is_dir() and d.name != "images":
                existing_slugs.add(d.name)

    # Pre-populate content hashes from state
    content_hashes: dict[str, str] = {}
    for slug_key, slug_state in state.items():
        if isinstance(slug_state, dict) and "md_hash" in slug_state:
            content_hashes[slug_state["md_hash"]] = slug_key

    stats = {"done": 0, "skip": 0, "fail": 0, "dedup": 0}

    def _process_one(url_depth: tuple[str, int]) -> dict[str, Any] | None:
        url, depth = url_depth
        return process_page(url, depth, state, existing_slugs, content_hashes, args.force)

    # Process with thread pool
    with ThreadPoolExecutor(max_workers=SCRAPE_WORKERS) as pool:
        futures = {pool.submit(_process_one, ud): ud for ud in urls_to_scrape}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    slug = result["slug"]
                    state[slug] = {
                        "content_hash": result.get("content_hash"),
                        "md_hash": result.get("md_hash"),
                        "last_fetched": time.strftime("%Y-%m-%d"),
                    }
                    stats["done"] += 1
                else:
                    stats["skip"] += 1
            except Exception as exc:
                url_depth = futures[future]
                print(f"[fail] {url_depth[0]}: {exc}")
                stats["fail"] += 1

    # Save state
    save_state(state)

    # -------------------------------------------------------------------
    # Phase 3: Internal link replacement
    # -------------------------------------------------------------------
    print("[info] Replacing internal links...")
    link_replacements = replace_internal_links(BASE_DIR)
    print(f"[info] Replaced {link_replacements} internal links")

    # -------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Scrape complete")
    print(f"  Articles saved:    {stats['done']}")
    print(f"  Skipped unchanged: {stats['skip']}")
    print(f"  Duplicates:        {stats['dedup']}")
    print(f"  Failures:          {stats['fail']}")
    img_count = len(list(IMAGES_DIR.glob("*"))) if IMAGES_DIR.exists() else 0
    print(f"  Images on disk:    {img_count}")
    print(f"  Output directory:  {BASE_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
