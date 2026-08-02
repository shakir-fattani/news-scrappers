#!/usr/bin/env python3
from __future__ import annotations
"""
Al-Monitor sitemap scraper.

Incremental scraper for www.al-monitor.com.
Fetches all article URLs from the paginated sitemap index, downloads content
as markdown + YAML metadata + images, and supports incremental runs via
.fetch-state.json.

Usage:
    python3 scrape_almonitor.py              # incremental run
    python3 scrape_almonitor.py --force      # re-fetch everything
    python3 scrape_almonitor.py --slug X     # fetch only slug X
"""


import argparse
import hashlib
import json
import os
import re
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
_MISSING: list[str] = []
try:
    import requests
except ImportError:
    _MISSING.append("requests")
try:
    from bs4 import BeautifulSoup, NavigableString, Tag
except ImportError:
    _MISSING.append("beautifulsoup4")
try:
    import yaml
except ImportError:
    _MISSING.append("pyyaml")
try:
    import lxml  # noqa: F401
except ImportError:
    _MISSING.append("lxml")

if _MISSING:
    print(
        "Missing dependencies: " + ", ".join(_MISSING),
        file=sys.stderr,
    )
    print(
        "Install with:\n  pip3 install --user --break-system-packages "
        + " ".join(_MISSING),
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SITE_DOMAIN = "www.al-monitor.com"
SITE_ORIGIN = f"https://{SITE_DOMAIN}"
SITEMAP_INDEX_URL = f"{SITE_ORIGIN}/sitemap.xml"

BASE_DIR = Path(__file__).resolve().parent / "al_monitor"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

REQUEST_DELAY = 1.0  # seconds between requests
MAX_WORKERS = 5

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
    "/pulse/",
]

SKIP_URL_PATTERNS = re.compile(
    r"(/page/\d+|/search|/about|/contact|/privacy|/terms|/author/|"
    r"/tag/|/tags/|/category/|/archive|/subscribe|/newsletter|"
    r"/login|/register|/account|/cart|/checkout|/feed|/rss|"
    r"\.(pdf|jpg|jpeg|png|gif|svg|mp4|mp3|zip|doc|docx|xls|xlsx)$)",
    re.IGNORECASE,
)

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup", ".subscription-widget",
    ".comments", ".comment-section", ".author-bio", ".disclaimer", ".cookie-banner",
    ".breadcrumb", ".pagination", ".ad", ".advertisement",
    "[class*='promo']", "[class*='banner']", "[class*='popup']", "[class*='modal']",
    "[class*='social']", "[class*='newsletter']", "[class*='signup']",
    "[class*='related']", "[class*='recommend']",
    "script", "style", "noscript", "svg", "button", "form", "iframe",
    ".paywall", "[class*='paywall']", "[class*='subscribe']",
    "[class*='share-bar']", "[class*='toolbar']",
]

# Al-Monitor specific content selectors (priority order)
CONTENT_SELECTORS = [
    "div.field--name-body",            # Drupal field body
    "div.article-body",                # common news pattern
    "div.article__body",
    "div[class*='article-content']",
    "div[class*='ArticleBody']",
    "div.body-content",
    "div.story-body",
    "div.post-content",
    "article .content",
    "div.content-body",
    "div.entry-content",
    "article",
    "main",
    "div.content",
    "div#content",
]

WP_PROXY_RE = re.compile(r"^https?://i[0-3]\.wp\.com/(.+)$")

DATE_SEGMENT_RE = re.compile(r"/(\d{4})/(\d{1,2})/(\d{1,2})/")
DATE_YM_RE = re.compile(r"/(\d{4})-(\d{1,2})/")


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------
_session: Optional[requests.Session] = None
_last_request_time = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
    return _session


def throttled_get(url: str, **kwargs) -> requests.Response:
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    resp = get_session().get(url, timeout=30, **kwargs)
    _last_request_time = time.time()
    return resp


# ---------------------------------------------------------------------------
# Fetch state
# ---------------------------------------------------------------------------
def load_state() -> dict:
    if FETCH_STATE_FILE.exists():
        with open(FETCH_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state: dict) -> None:
    FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FETCH_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Sitemap parsing
# ---------------------------------------------------------------------------
def fetch_sitemap_index() -> list[str]:
    """Return list of child sitemap URLs from the sitemap index."""
    resp = throttled_get(SITEMAP_INDEX_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml-xml")

    child_urls: list[str] = []

    # Check if this is a sitemap index
    sitemapindex = soup.find("sitemapindex")
    if sitemapindex:
        for sitemap_tag in sitemapindex.find_all("sitemap"):
            loc = sitemap_tag.find("loc")
            if loc and loc.text.strip():
                child_urls.append(loc.text.strip())
    else:
        # It might be a direct urlset — treat the index URL itself as a child
        child_urls.append(SITEMAP_INDEX_URL)

    return child_urls


def fetch_child_sitemap(sitemap_url: str) -> list[dict]:
    """Fetch a single child sitemap and return list of {loc, lastmod}."""
    entries: list[dict] = []
    try:
        resp = throttled_get(sitemap_url)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[warn] Could not fetch sitemap {sitemap_url}: {exc}")
        return entries

    soup = BeautifulSoup(resp.content, "lxml-xml")

    # If this is itself a sitemap index, recurse
    sitemapindex = soup.find("sitemapindex")
    if sitemapindex:
        for sitemap_tag in sitemapindex.find_all("sitemap"):
            loc = sitemap_tag.find("loc")
            if loc and loc.text.strip():
                entries.extend(fetch_child_sitemap(loc.text.strip()))
        return entries

    urlset = soup.find("urlset")
    if not urlset:
        return entries

    for url_tag in urlset.find_all("url"):
        loc_tag = url_tag.find("loc")
        if not loc_tag:
            continue
        loc = loc_tag.text.strip()
        lastmod_tag = url_tag.find("lastmod")
        lastmod = lastmod_tag.text.strip() if lastmod_tag else None
        changefreq_tag = url_tag.find("changefreq")
        changefreq = changefreq_tag.text.strip() if changefreq_tag else None
        entries.append({
            "loc": loc,
            "lastmod": lastmod,
            "changefreq": changefreq,
        })

    return entries


def discover_all_sitemap_pages() -> list[str]:
    """
    Al-Monitor uses paginated child sitemaps (?page=1, ?page=2, ...).
    Discover all pages by following pagination until a 404 or empty result.
    """
    child_urls = fetch_sitemap_index()

    # If we got paginated sitemaps, try to discover additional pages
    paginated: list[str] = []
    non_paginated: list[str] = []

    for url in child_urls:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        if "page" in qs:
            paginated.append(url)
        else:
            non_paginated.append(url)

    # For paginated sitemaps, discover all pages
    all_urls: list[str] = list(non_paginated)

    if paginated:
        # We already have some paginated URLs from the index
        all_urls.extend(paginated)

        # Try to discover more pages beyond what the index listed
        max_page_found = 0
        base_url = None
        for url in paginated:
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            page_num = int(qs["page"][0])
            if page_num > max_page_found:
                max_page_found = page_num
                base_url = urlunparse((
                    parsed.scheme, parsed.netloc, parsed.path,
                    parsed.params, "", parsed.fragment,
                ))

        if base_url:
            page = max_page_found + 1
            while True:
                test_url = f"{base_url}?page={page}"
                try:
                    resp = throttled_get(test_url)
                    if resp.status_code != 200:
                        break
                    soup = BeautifulSoup(resp.content, "lxml-xml")
                    urlset = soup.find("urlset")
                    if not urlset or not urlset.find("url"):
                        break
                    all_urls.append(test_url)
                    page += 1
                except Exception:
                    break
    else:
        # No paginated URLs in index — try ?page=N on the index itself
        page = 1
        while True:
            test_url = f"{SITEMAP_INDEX_URL}?page={page}"
            try:
                resp = throttled_get(test_url)
                if resp.status_code != 200:
                    break
                soup = BeautifulSoup(resp.content, "lxml-xml")
                urlset = soup.find("urlset")
                if urlset and urlset.find("url"):
                    all_urls.append(test_url)
                    page += 1
                else:
                    # Maybe it's a sitemapindex with child sitemaps at each page
                    smi = soup.find("sitemapindex")
                    if smi and smi.find("sitemap"):
                        all_urls.append(test_url)
                        page += 1
                    else:
                        break
            except Exception:
                break

    return all_urls


def collect_all_entries() -> list[dict]:
    """Collect all URL entries from all sitemap pages."""
    print("[info] Discovering sitemap pages...")
    sitemap_urls = discover_all_sitemap_pages()
    print(f"[info] Found {len(sitemap_urls)} sitemap page(s)")

    all_entries: list[dict] = []
    seen_locs: set[str] = set()

    for sitemap_url in sitemap_urls:
        entries = fetch_child_sitemap(sitemap_url)
        for entry in entries:
            loc = entry["loc"]
            if loc not in seen_locs:
                seen_locs.add(loc)
                all_entries.append(entry)

    print(f"[info] Total unique URLs in sitemap: {len(all_entries)}")
    return all_entries


# ---------------------------------------------------------------------------
# URL / slug helpers
# ---------------------------------------------------------------------------
def is_article_url(url: str) -> bool:
    """Heuristic check: does the URL look like an article rather than listing."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Skip non-content URLs
    if SKIP_URL_PATTERNS.search(path):
        return False

    # Skip URLs with pagination query params
    qs = parse_qs(parsed.query)
    if "page" in qs or "p" in qs:
        return False

    # Must have a slug-like last segment (not just a category)
    segments = [s for s in path.split("/") if s]
    if not segments:
        return False

    last_seg = segments[-1]

    # Skip if the last segment is purely a content-type pattern name
    for pattern in CONTENT_PATH_PATTERNS:
        pattern_name = pattern.strip("/").split("/")[-1]
        if last_seg == pattern_name and len(segments) <= 2:
            return False

    # Al-Monitor URL patterns:
    # /originals/2026/07/article-slug.html
    # /pulse/originals/2026/07/article-slug
    # /originals/article-slug
    # Check that there's a meaningful slug (not just date segments)
    meaningful_segments = [
        s for s in segments
        if not re.match(r"^\d{4}$", s) and not re.match(r"^\d{1,2}$", s)
    ]
    if not meaningful_segments:
        return False

    return True


def detect_content_type(url_path: str) -> tuple[str, Optional[str]]:
    """
    Detect content-type and category from URL path.

    Returns (content_type, category).
    """
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    # Strip date segments
    segments = [
        s for s in segments
        if not re.match(r"^\d{4}$", s) and not re.match(r"^\d{1,2}$", s)
    ]

    if not segments:
        return ("article", None)

    # Check for nested patterns
    matched_patterns: list[str] = []
    for pattern in CONTENT_PATH_PATTERNS:
        pattern_name = pattern.strip("/")
        if pattern_name in segments[:-1]:  # don't match the slug itself
            matched_patterns.append(pattern_name)

    if len(matched_patterns) >= 2:
        return (matched_patterns[-1], matched_patterns[0])
    elif len(matched_patterns) == 1:
        return (matched_patterns[0], None)
    elif segments:
        # Use first segment as content type if it looks like a category
        if len(segments) >= 2:
            return (segments[0], None)
        return ("article", None)

    return ("article", None)


def generate_slug(url: str) -> str:
    """Generate a filesystem-safe slug from a URL."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Remove .html extension
    path = re.sub(r"\.html?$", "", path)

    segments = [s for s in path.split("/") if s]

    # Remove date segments
    non_date_segments = [
        s for s in segments
        if not re.match(r"^\d{4}$", s) and not re.match(r"^\d{1,2}$", s)
    ]

    if non_date_segments:
        slug = non_date_segments[-1]
    elif segments:
        slug = segments[-1]
    else:
        slug = hashlib.md5(url.encode()).hexdigest()[:10]

    # Sanitize
    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9-]", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    slug = slug.strip("-")

    return slug or "untitled"


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------
def parse_date_string(s: str) -> Optional[str]:
    """Try to parse a date string into YYYY-MM-DD."""
    if not s:
        return None
    s = s.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try ISO-ish partial match
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None


def extract_date(soup: BeautifulSoup, url: str, lastmod: Optional[str]) -> Optional[str]:
    """Extract publish date using the priority chain."""

    # 1. article:published_time
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        d = parse_date_string(meta["content"])
        if d:
            return d

    # 2. meta name="date" or name="publish-date"
    for name in ("date", "publish-date", "pubdate", "publishdate"):
        meta = soup.find("meta", attrs={"name": name})
        if meta and meta.get("content"):
            d = parse_date_string(meta["content"])
            if d:
                return d

    # 3. <time datetime="...">
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        d = parse_date_string(time_el["datetime"])
        if d:
            return d

    # 4. Date class elements
    for selector in [
        "[class*='date']", "[class*='timestamp']", "[class*='time']",
        "span.date", "div.date", "p.date",
    ]:
        el = soup.select_one(selector)
        if el and el.get_text(strip=True):
            d = parse_date_string(el.get_text(strip=True))
            if d:
                return d

    # 5. JSON-LD datePublished
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script_tag.string or "")
            if isinstance(ld, list):
                for item in ld:
                    if isinstance(item, dict) and "datePublished" in item:
                        d = parse_date_string(item["datePublished"])
                        if d:
                            return d
            elif isinstance(ld, dict):
                if "datePublished" in ld:
                    d = parse_date_string(ld["datePublished"])
                    if d:
                        return d
                # Check @graph
                for item in ld.get("@graph", []):
                    if isinstance(item, dict) and "datePublished" in item:
                        d = parse_date_string(item["datePublished"])
                        if d:
                            return d
        except (json.JSONDecodeError, TypeError):
            continue

    # 6. Date from URL path
    m = DATE_SEGMENT_RE.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = DATE_YM_RE.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-01"

    # 7. Sitemap lastmod fallback
    if lastmod:
        d = parse_date_string(lastmod)
        if d:
            return d

    # 8. (HTTP Last-Modified is handled at fetch time if needed)
    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------
def extract_tags(soup: BeautifulSoup) -> list[str]:
    """Extract and deduplicate tags from multiple sources."""
    raw_tags: list[str] = []

    # 1. meta keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        raw_tags.extend(t.strip() for t in meta_kw["content"].split(","))

    # 2. article:tag meta (may appear multiple times)
    for meta in soup.find_all("meta", property="article:tag"):
        if meta.get("content"):
            raw_tags.append(meta["content"].strip())

    # 3. JSON-LD keywords
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script_tag.string or "")
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if not isinstance(item, dict):
                    continue
                kw = item.get("keywords")
                if isinstance(kw, list):
                    raw_tags.extend(kw)
                elif isinstance(kw, str):
                    raw_tags.extend(t.strip() for t in kw.split(","))
                for graph_item in item.get("@graph", []):
                    if isinstance(graph_item, dict):
                        kw = graph_item.get("keywords")
                        if isinstance(kw, list):
                            raw_tags.extend(kw)
                        elif isinstance(kw, str):
                            raw_tags.extend(t.strip() for t in kw.split(","))
        except (json.JSONDecodeError, TypeError):
            continue

    # 4. Visible tag links
    for sel in ["a[rel='tag']", ".tags a", ".post-tags a", ".article-tags a",
                "[class*='tag-link']", ".cat-links a", ".entry-categories a",
                "[class*='topic'] a"]:
        for a in soup.select(sel):
            text = a.get_text(strip=True)
            if text and len(text) < 60:
                raw_tags.append(text)

    # Normalize and deduplicate
    seen: set[str] = set()
    tags: list[str] = []
    for t in raw_tags:
        normalized = t.strip().lower()
        if normalized and normalized not in seen and len(normalized) > 1:
            seen.add(normalized)
            tags.append(normalized)

    return tags


# ---------------------------------------------------------------------------
# Image handling
# ---------------------------------------------------------------------------
def resolve_wp_proxy(url: str) -> tuple[str, str]:
    """
    For WP proxy URLs, return (fetch_url, original_path).
    For normal URLs, return (url, url).
    """
    m = WP_PROXY_RE.match(url)
    if m:
        original_path = m.group(1)
        return (url, f"https://{original_path}")
    return (url, url)


def image_filename(slug: str, img_url: str) -> str:
    """Generate a deterministic image filename."""
    _, original_url = resolve_wp_proxy(img_url)

    # Strip query params for extension detection
    parsed = urlparse(original_url)
    path = parsed.path

    ext = os.path.splitext(path)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp"):
        ext = ".jpg"

    url_hash = hashlib.md5(img_url.encode()).hexdigest()[:10]
    return f"{slug}_{url_hash}{ext}"


def download_image(img_url: str, filename: str) -> bool:
    """Download an image if it doesn't already exist."""
    dest = IMAGES_DIR / filename
    if dest.exists():
        return True
    try:
        fetch_url, _ = resolve_wp_proxy(img_url)
        resp = throttled_get(fetch_url)
        if resp.status_code == 200 and len(resp.content) > 100:
            IMAGES_DIR.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                f.write(resp.content)
            return True
    except Exception as exc:
        print(f"[warn] Image download failed {img_url}: {exc}")
    return False


# ---------------------------------------------------------------------------
# HTML → Markdown converter
# ---------------------------------------------------------------------------
def html_to_markdown(element: Tag, slug: str, images: list[tuple[str, str]]) -> str:
    """Convert a BeautifulSoup Tag to markdown text."""
    if element is None:
        return ""

    parts: list[str] = []

    for child in element.children:
        if isinstance(child, NavigableString):
            text = str(child)
            if text.strip():
                parts.append(text)
            elif parts and not parts[-1].endswith(" "):
                parts.append(" ")
            continue

        if not isinstance(child, Tag):
            continue

        tag_name = child.name.lower() if child.name else ""

        # Skip noise elements
        if tag_name in ("script", "style", "noscript", "svg", "button", "form",
                        "iframe", "nav", "input", "select", "textarea"):
            continue

        # Check for noise classes
        classes = " ".join(child.get("class", []))
        if any(kw in classes.lower() for kw in (
            "social", "share", "newsletter", "subscribe", "related",
            "recommend", "promo", "banner", "popup", "modal",
            "sidebar", "comment", "advertisement", "ad-", "paywall",
        )):
            continue

        if tag_name in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag_name[1])
            text = child.get_text(strip=True)
            if text:
                parts.append(f"\n\n{'#' * level} {text}\n\n")

        elif tag_name == "p":
            inner = html_to_markdown(child, slug, images)
            if inner.strip():
                parts.append(f"\n\n{inner.strip()}\n\n")

        elif tag_name in ("strong", "b"):
            text = child.get_text(strip=True)
            if text:
                parts.append(f"**{text}**")

        elif tag_name in ("em", "i"):
            text = child.get_text(strip=True)
            if text:
                parts.append(f"*{text}*")

        elif tag_name == "a":
            href = child.get("href", "")
            # Check if it wraps an image
            inner_img = child.find("img")
            if inner_img:
                parts.append(_convert_img(inner_img, slug, images))
            else:
                text = child.get_text(strip=True)
                if text and href:
                    parts.append(f"[{text}]({href})")
                elif text:
                    parts.append(text)

        elif tag_name == "img":
            parts.append(_convert_img(child, slug, images))

        elif tag_name == "picture":
            inner_img = child.find("img")
            if inner_img:
                parts.append(_convert_img(inner_img, slug, images))
            else:
                source = child.find("source")
                if source and source.get("srcset"):
                    src = source["srcset"].split(",")[0].strip().split(" ")[0]
                    fname = image_filename(slug, src)
                    images.append((src, fname))
                    parts.append(f"![image](../images/{fname})")

        elif tag_name == "figure":
            inner = html_to_markdown(child, slug, images)
            figcaption = child.find("figcaption")
            if figcaption:
                caption = figcaption.get_text(strip=True)
                # Remove caption from inner since it was already processed
                inner = inner.replace(caption, "").strip()
                if inner:
                    parts.append(f"\n\n{inner}\n*{caption}*\n\n")
                else:
                    parts.append(f"\n\n*{caption}*\n\n")
            elif inner.strip():
                parts.append(f"\n\n{inner.strip()}\n\n")

        elif tag_name == "blockquote":
            inner = html_to_markdown(child, slug, images)
            if inner.strip():
                quoted = "\n".join(f"> {line}" for line in inner.strip().split("\n"))
                parts.append(f"\n\n{quoted}\n\n")

        elif tag_name == "pre":
            code_el = child.find("code")
            if code_el:
                lang = ""
                code_classes = code_el.get("class", [])
                for cls in code_classes:
                    if cls.startswith("language-"):
                        lang = cls.replace("language-", "")
                        break
                code_text = code_el.get_text()
                parts.append(f"\n\n```{lang}\n{code_text}\n```\n\n")
            else:
                parts.append(f"\n\n```\n{child.get_text()}\n```\n\n")

        elif tag_name == "code" and child.parent and child.parent.name != "pre":
            parts.append(f"`{child.get_text()}`")

        elif tag_name in ("ul", "ol"):
            items = child.find_all("li", recursive=False)
            list_parts: list[str] = []
            for idx, li in enumerate(items):
                inner = html_to_markdown(li, slug, images).strip()
                if inner:
                    prefix = "- " if tag_name == "ul" else f"{idx + 1}. "
                    list_parts.append(f"{prefix}{inner}")
            if list_parts:
                parts.append("\n\n" + "\n".join(list_parts) + "\n\n")

        elif tag_name == "li":
            inner = html_to_markdown(child, slug, images)
            parts.append(inner)

        elif tag_name == "br":
            parts.append("\n")

        elif tag_name == "hr":
            parts.append("\n\n---\n\n")

        elif tag_name == "table":
            parts.append(_convert_table(child))

        elif tag_name in ("div", "span", "section", "article", "main", "figcaption"):
            inner = html_to_markdown(child, slug, images)
            if inner.strip():
                parts.append(inner)

        else:
            inner = html_to_markdown(child, slug, images)
            if inner.strip():
                parts.append(inner)

    return "".join(parts)


def _convert_img(img: Tag, slug: str, images: list[tuple[str, str]]) -> str:
    """Convert an img tag to markdown and queue download."""
    src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
    if not src or src.startswith("data:"):
        return ""

    # Make absolute
    if src.startswith("//"):
        src = "https:" + src
    elif src.startswith("/"):
        src = SITE_ORIGIN + src

    alt = img.get("alt", "image") or "image"
    fname = image_filename(slug, src)
    images.append((src, fname))
    return f"![{alt}](../images/{fname})"


def _convert_table(table: Tag) -> str:
    """Convert an HTML table to a markdown table."""
    rows: list[list[str]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        row = [c.get_text(strip=True).replace("|", "\\|") for c in cells]
        if row:
            rows.append(row)

    if not rows:
        return ""

    # Normalize column count
    max_cols = max(len(r) for r in rows)
    for r in rows:
        while len(r) < max_cols:
            r.append("")

    lines: list[str] = []
    header = rows[0]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join("---" for _ in header) + " |")
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n\n" + "\n".join(lines) + "\n\n"


def clean_markdown(md: str) -> str:
    """Clean up markdown text."""
    # Collapse excessive newlines
    md = re.sub(r"\n{3,}", "\n\n", md)
    # Strip leading/trailing whitespace
    md = md.strip()
    return md


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------
def extract_title(soup: BeautifulSoup) -> str:
    """Extract the article title."""
    # Try h1 first
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text

    # og:title
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()

    # <title>
    title = soup.find("title")
    if title:
        text = title.get_text(strip=True)
        # Strip site suffix
        for sep in (" | ", " - ", " :: ", " — "):
            if sep in text:
                text = text.split(sep)[0].strip()
        return text

    return "Untitled"


def extract_brief(soup: BeautifulSoup) -> str:
    """Extract a short brief/description."""
    for prop in ("og:description", "description"):
        meta = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if meta and meta.get("content"):
            return meta["content"].strip()[:300]
    return ""


def find_content_container(soup: BeautifulSoup) -> Optional[Tag]:
    """Find the main content container using site-specific then generic selectors."""
    for selector in CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            # Verify it has meaningful text
            text = el.get_text(strip=True)
            if len(text) > 100:
                return el
    return None


def is_listing_page(soup: BeautifulSoup, url: str) -> bool:
    """Detect if a page is a listing/index rather than an article."""
    # Check title for listing indicators
    title = extract_title(soup).lower()
    for kw in ("archive", "all posts", "page 2", "page 3", "category:", "tag:"):
        if kw in title:
            return True

    # Check og:type
    og_type = soup.find("meta", property="og:type")
    if og_type and og_type.get("content", "").lower() in ("website", "blog"):
        # Not definitive, but a signal
        pass

    # Count article cards vs prose
    container = find_content_container(soup)
    if container:
        text = container.get_text(strip=True)
        word_count = len(text.split())
        links = container.find_all("a")

        # High link-to-text ratio = listing
        if word_count < 200 and len(links) > 10:
            return True

    return False


def detect_paywall(soup: BeautifulSoup) -> bool:
    """Check if content appears truncated by a paywall."""
    paywall_indicators = [
        "[class*='paywall']", "[class*='subscribe']",
        "[class*='premium']", "[class*='locked']",
        "[id*='paywall']",
    ]
    for sel in paywall_indicators:
        if soup.select_one(sel):
            return True

    # Check for "subscribe to continue" text
    body_text = soup.get_text().lower()
    for phrase in ("subscribe to continue", "sign up to read", "members only",
                   "premium content", "subscribe for full access"):
        if phrase in body_text:
            return True

    return False


# ---------------------------------------------------------------------------
# Main scrape logic
# ---------------------------------------------------------------------------
def scrape_article(
    entry: dict,
    state: dict,
    force: bool,
    content_hashes: dict,
    slug_counts: dict,
) -> Optional[dict]:
    """
    Scrape a single article. Returns updated state entry or None.
    """
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq")

    # Generate slug
    slug = generate_slug(url)

    # Handle slug collisions
    if slug in slug_counts:
        slug_counts[slug] += 1
        slug = f"{slug}-{slug_counts[slug]}"
    else:
        slug_counts[slug] = 1

    slug_dir = BASE_DIR / slug
    content_file = slug_dir / "content.md"
    meta_file = slug_dir / "meta.yaml"

    # Check incremental state
    if not force:
        stored = state.get(slug)
        if stored and stored.get("lastmod") == lastmod and content_file.exists():
            print(f"[skip] {slug} — unchanged")
            return None

    # Fetch page
    try:
        resp = throttled_get(url)
        if resp.status_code != 200:
            print(f"[fail] {slug} — HTTP {resp.status_code}")
            return None
    except Exception as exc:
        print(f"[fail] {slug} — {exc}")
        return None

    http_last_modified = resp.headers.get("Last-Modified")

    soup = BeautifulSoup(resp.content, "lxml")

    # Remove noise
    for sel in NOISE_SELECTORS:
        for el in soup.select(sel):
            el.decompose()

    # Check if listing page
    if is_listing_page(soup, url):
        print(f"[skip] {slug} — listing page")
        return None

    # Find content
    container = find_content_container(soup)
    if not container:
        print(f"[skip] {slug} — no content container found")
        return None

    # Check word count
    raw_text = container.get_text(strip=True)
    word_count = len(raw_text.split())
    if word_count < 50:
        print(f"[skip] {slug} — too short ({word_count} words)")
        return None

    # Convert to markdown
    images_to_download: list[tuple[str, str]] = []
    markdown = html_to_markdown(container, slug, images_to_download)
    markdown = clean_markdown(markdown)

    if not markdown.strip():
        print(f"[skip] {slug} — empty content after conversion")
        return None

    # Content dedup
    content_hash = hashlib.md5(markdown.encode()).hexdigest()
    if content_hash in content_hashes:
        original = content_hashes[content_hash]
        print(f"[dedup] {slug} — duplicate of {original}")
        return None
    content_hashes[content_hash] = slug

    # Extract metadata
    title = extract_title(soup)
    brief = extract_brief(soup)
    publish_date = extract_date(soup, url, lastmod)

    # HTTP Last-Modified as last resort
    if not publish_date and http_last_modified:
        publish_date = parse_date_string(http_last_modified)

    content_type, category = detect_content_type(urlparse(url).path)
    tags = extract_tags(soup)
    truncated = detect_paywall(soup)

    # Download images
    for img_url, img_fname in images_to_download:
        download_image(img_url, img_fname)

    # Write output
    slug_dir.mkdir(parents=True, exist_ok=True)

    # meta.yaml
    meta: dict = {
        "title": title,
        "publish-date": publish_date,
        "short-brief": brief,
        "source-url": url,
        "content-type": content_type,
    }
    if category:
        meta["category"] = category
    if changefreq:
        meta["change-frequency"] = changefreq
    if tags:
        meta["tags"] = tags
    if truncated:
        meta["truncated"] = True

    with open(meta_file, "w", encoding="utf-8") as f:
        yaml.dump(meta, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    # content.md
    with open(content_file, "w", encoding="utf-8") as f:
        f.write(markdown)

    img_count = len(images_to_download)
    print(f"[done] {slug} — {word_count} words, {img_count} images")

    return {
        "slug": slug,
        "lastmod": lastmod,
        "content_hash": content_hash,
    }


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------
def replace_internal_links() -> int:
    """Replace internal links in all content.md files with local relative paths."""
    replaced_count = 0
    existing_slugs: set[str] = set()

    for child in BASE_DIR.iterdir():
        if child.is_dir() and child.name != "images" and not child.name.startswith("."):
            existing_slugs.add(child.name)

    link_re = re.compile(
        rf"$$([^]]+)$$\(https?://{re.escape(SITE_DOMAIN)}(/[^)]*)\)"
    )

    for slug_name in existing_slugs:
        content_file = BASE_DIR / slug_name / "content.md"
        if not content_file.exists():
            continue

        content = content_file.read_text(encoding="utf-8")
        original = content

        def _replace_link(m: re.Match) -> str:
            link_text = m.group(1)
            path = m.group(2)
            # Strip query params and fragments
            path = path.split("?")[0].split("#")[0]
            target_slug = generate_slug(SITE_ORIGIN + path)
            if target_slug in existing_slugs:
                return f"[{link_text}](../{target_slug}/content.md)"
            return m.group(0)

        content = link_re.sub(_replace_link, content)

        if content != original:
            content_file.write_text(content, encoding="utf-8")
            replaced_count += 1

    return replaced_count


# ---------------------------------------------------------------------------
# CLI & main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Al-Monitor sitemap scraper")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything ignoring state")
    parser.add_argument("--slug", type=str, default=None, help="Fetch only this slug")
    args = parser.parse_args()

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    state = load_state()

    # Collect sitemap entries
    all_entries = collect_all_entries()

    # Filter to article URLs
    article_entries = [e for e in all_entries if is_article_url(e["loc"])]
    print(f"[info] Article URLs after filtering: {len(article_entries)}")

    # If --slug, filter further
    if args.slug:
        article_entries = [
            e for e in article_entries
            if generate_slug(e["loc"]) == args.slug
        ]
        if not article_entries:
            print(f"[error] No sitemap entry matches slug '{args.slug}'")
            sys.exit(1)
        print(f"[info] Fetching single slug: {args.slug}")

    # Scrape
    content_hashes: dict[str, str] = {}
    slug_counts: dict[str, int] = {}
    stats = {"done": 0, "skip": 0, "fail": 0, "dedup": 0}

    # Load existing content hashes from state for dedup
    for slug_key, entry_data in state.items():
        if isinstance(entry_data, dict) and "content_hash" in entry_data:
            content_hashes[entry_data["content_hash"]] = slug_key

    def _process(entry: dict) -> Optional[dict]:
        try:
            return scrape_article(entry, state, args.force, content_hashes, slug_counts)
        except Exception:
            slug = generate_slug(entry["loc"])
            print(f"[fail] {slug} — unhandled error")
            traceback.print_exc()
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_process, e): e for e in article_entries}
        for future in as_completed(futures):
            result = future.result()
            if result:
                state[result["slug"]] = {
                    "lastmod": result["lastmod"],
                    "content_hash": result["content_hash"],
                }
                stats["done"] += 1
            else:
                stats["skip"] += 1

    # Save state
    save_state(state)

    # Internal link replacement
    print("[info] Replacing internal links...")
    link_count = replace_internal_links()

    # Report
    print(f"\n{'=' * 50}")
    print(f"Al-Monitor scrape complete")
    print(f"  Fetched:    {stats['done']}")
    print(f"  Skipped:    {stats['skip']}")
    print(f"  Links fixed: {link_count}")
    print(f"  Output:     {BASE_DIR}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
