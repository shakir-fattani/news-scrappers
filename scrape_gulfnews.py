#!/usr/bin/env python3
"""
Gulf News sitemap scraper — archives articles from gulfnews.com into
structured markdown + YAML metadata with downloaded images.

Usage:
    python3 scrape_gulfnews.py              # incremental run
    python3 scrape_gulfnews.py --force      # re-fetch everything
    python3 scrape_gulfnews.py --slug X     # fetch only slug X

Output: gulf_news/<slug>/meta.yaml + content.md, gulf_news/images/
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
from __future__ import annotations

import argparse
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
DOMAIN = "gulfnews.com"
SITEMAP_URL = "https://gulfnews.com/sitemap.xml"
BASE_DIR = Path("/Users/shakirfattani/kaam/news-scrappers/gulf_news")
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

MAX_WORKERS = 5
REQUEST_DELAY = 1.0
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

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
    "/entertainment/", "/sport/", "/world/", "/uae/", "/gulf/",
    "/technology/", "/photos/", "/videos/", "/food/", "/travel/",
    "/health/", "/going-out/", "/culture/", "/auto/", "/how-to/",
]

# Gulf News specific content selectors — tried in order
CONTENT_SELECTORS = [
    "div.article-body",
    "div.article-body-viewer",
    "div[class*='article-body']",
    "div[class*='ArticleBody']",
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
    ".tags-wrapper", ".article-tags-social",
    ".gallery-overlay", ".article-gallery-overlay",
    "script", "style", "noscript", "svg", "button", "iframe",
    "[class*='ad-']", "[class*='Ad-']", "[id*='google_ads']",
    ".login-wall", ".subscribe-wall", ".paywall",
]

SKIP_URL_PATTERNS = re.compile(
    r"(/page/\d+|/search\b|/login|/signup|/register|/account|"
    r"/settings|/cart|/checkout|/api/|/graphql|/rss|/feed|"
    r"/author/|/authors/|/tag/[^/]*/?$|/tags/?$|/about-us|"
    r"/privacy|/terms|/contact|/advertise|/subscribe/?$|"
    r"\.(css|js|json|xml|rss|atom|woff2?|ttf|eot|pdf|mp4|mp3)$)",
    re.IGNORECASE,
)

DATE_SEGMENT_RE = re.compile(r"^(19|20)\d{2}$")
NUMERIC_ONLY_RE = re.compile(r"^\d+$")

# WordPress image proxy pattern
WP_PROXY_RE = re.compile(r"^https?://i[0-3]\.wp\.com/(.+)")

# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

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


# ---------------------------------------------------------------------------
# Sitemap fetching
# ---------------------------------------------------------------------------

def fetch_xml(url: str) -> BeautifulSoup | None:
    """Fetch a URL and parse as XML."""
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.content, "lxml-xml")
    except Exception as exc:
        print(f"[warn] Failed to fetch {url}: {exc}")
        return None


def fetch_sitemap_urls(sitemap_url: str) -> list[dict[str, str]]:
    """
    Fetch sitemap — handle sitemapindex with child sitemaps.
    Returns list of dicts with keys: loc, lastmod, changefreq.
    """
    soup = fetch_xml(sitemap_url)
    if soup is None:
        return []

    entries: list[dict[str, str]] = []

    # Check for sitemapindex
    sitemaps = soup.find_all("sitemap")
    if sitemaps:
        print(f"[info] Sitemap index found with {len(sitemaps)} child sitemaps")
        for sm in sitemaps:
            loc_tag = sm.find("loc")
            if loc_tag is None:
                continue
            child_url = loc_tag.get_text(strip=True)
            print(f"[info]   Fetching child sitemap: {child_url}")
            child_entries = _parse_urlset(child_url)
            entries.extend(child_entries)
            time.sleep(0.5)
    else:
        # Direct urlset
        entries = _parse_urlset_soup(soup)

    return entries


def _parse_urlset(url: str) -> list[dict[str, str]]:
    soup = fetch_xml(url)
    if soup is None:
        return []
    return _parse_urlset_soup(soup)


def _parse_urlset_soup(soup: BeautifulSoup) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        if loc is None:
            continue
        entry: dict[str, str] = {"loc": loc.get_text(strip=True)}
        lastmod = url_tag.find("lastmod")
        if lastmod:
            entry["lastmod"] = lastmod.get_text(strip=True)
        changefreq = url_tag.find("changefreq")
        if changefreq:
            entry["changefreq"] = changefreq.get_text(strip=True)
        entries.append(entry)
    return entries


# ---------------------------------------------------------------------------
# URL classification
# ---------------------------------------------------------------------------

def is_article_url(url: str) -> bool:
    """Return True if the URL looks like an article, not a listing."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    if SKIP_URL_PATTERNS.search(path):
        return False

    # Must have a slug segment after the section
    segments = [s for s in path.split("/") if s]
    if len(segments) < 2:
        return False

    # Gulf News article URLs typically end with a slug like "headline-1.1234567"
    # or just a descriptive slug
    last_seg = segments[-1]

    # Pure section pages — no article slug
    section_only = "/" + "/".join(segments) + "/"
    for pat in CONTENT_PATH_PATTERNS:
        if section_only == pat:
            return False

    # Must have some alphabetic content in the last segment
    if not re.search(r"[a-zA-Z]", last_seg):
        return False

    return True


def detect_content_type(url_path: str) -> tuple[str, str | None]:
    """
    Extract content-type and category from URL path.

    /business/markets/slug     -> ('markets', 'business')
    /world/gulf/saudi/slug     -> ('saudi', 'gulf')
    /opinion/slug              -> ('opinion', None)
    """
    segments = [s for s in url_path.strip("/").split("/") if s]
    # Remove the last segment (the slug)
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
    Handles date segments, numeric parents, and collisions.
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
    sitemap_lastmod: str | None,
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

    # 7. Sitemap lastmod
    if sitemap_lastmod:
        return _normalize_date(sitemap_lastmod)

    # 8. HTTP Last-Modified
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
    # Fallback: return first 10 chars if they look date-ish
    if len(raw) >= 10 and raw[4] == "-":
        return raw[:10]
    return raw[:10] if len(raw) >= 10 else raw


def _try_parse_date_text(text: str) -> str | None:
    """Try to parse a human-readable date string."""
    import re as _re
    # "July 15, 2026" or "15 July 2026"
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
    m = _re.search(r"([a-z]+)\s+(\d{1,2}),?\s+(\d{4})", text_lower)
    if m and m.group(1) in months:
        return f"{m.group(3)}-{months[m.group(1)]}-{int(m.group(2)):02d}"
    # DD Month YYYY
    m = _re.search(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", text_lower)
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
                # about / mentions
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
    ]:
        for a_tag in soup.select(selector):
            text = a_tag.get_text(strip=True).lower()
            if text and len(text) < 80:
                tags.add(text)

    # 5. Gulf News specific: section/topic links
    for a_tag in soup.select(".article-tags a, .tags-list a, .story-tags a"):
        text = a_tag.get_text(strip=True).lower()
        if text and len(text) < 80:
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
        "paywall", "subscribe-wall",
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
        # If wrapping an image, recurse without flattening
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
        # Make absolute
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = f"https://{DOMAIN}{src}"
        filename = download_image(src, slug)
        if filename:
            return f"![{alt}](../images/{filename})"
        return f"![{alt}]({src})"

    # Picture — extract inner img or first source
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

    # Normalize column count
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
    """Find the main article content container using Gulf News selectors."""
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
    # Check for article indicators
    og_type = soup.find("meta", property="og:type")
    if og_type and og_type.get("content", "").lower() == "article":
        return False

    pub_time = soup.find("meta", property="article:published_time")
    if pub_time:
        return False

    # Count words
    word_count = len(content_text.split())
    if word_count < 200:
        # Few words — check if link-heavy
        links = soup.select("article a, main a")
        if len(links) > 10:
            return True

    # Check title for listing indicators
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
    entry: dict[str, str],
    state: dict[str, Any],
    existing_slugs: set[str],
    content_hashes: dict[str, str],
    force: bool = False,
) -> dict[str, Any] | None:
    """
    Fetch and process a single article page.
    Returns updated state entry or None on skip/failure.
    """
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq", "unknown")
    parsed_url = urlparse(url)

    # Generate slug
    slug = generate_slug(parsed_url.path, existing_slugs)
    existing_slugs.add(slug)

    slug_dir = BASE_DIR / slug
    content_file = slug_dir / "content.md"
    meta_file = slug_dir / "meta.yaml"

    # Incremental check
    if not force:
        stored = state.get(slug)
        if (
            stored
            and stored.get("lastmod") == lastmod
            and content_file.exists()
        ):
            print(f"[skip] {slug} — unchanged")
            return None

    # Rate limiting
    time.sleep(REQUEST_DELAY)

    # Fetch page
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[fail] {slug} — {exc}")
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
        # Check if it's a listing page
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
        "[class*='premium-content']",
    ]
    for sel in paywall_indicators:
        if soup.select_one(sel):
            truncated = True
            break
    if word_count < 100 and not truncated:
        # Suspiciously short — might be truncated
        if soup.find(string=re.compile(r"subscribe|sign.?in|log.?in.*to.*read", re.I)):
            truncated = True

    # Extract metadata
    title = _extract_title(soup)
    brief = _extract_brief(soup)
    pub_date = extract_date(soup, url, lastmod, dict(resp.headers))
    tags = extract_tags(soup)
    content_type, category = detect_content_type(parsed_url.path)

    # Build meta.yaml
    meta: dict[str, Any] = {
        "title": title,
        "publish-date": pub_date,
        "change-frequency": changefreq,
        "short-brief": brief,
        "source-url": url,
        "content-type": content_type,
        "category": category,
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
        "lastmod": lastmod,
        "content_hash": content_hash,
    }


def _extract_title(soup: BeautifulSoup) -> str:
    """Extract article title."""
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
    title_el = soup.find("title")
    if title_el:
        text = title_el.get_text(strip=True)
        # Strip site name suffix like " | Gulf News"
        text = re.sub(r"\s*\|.*$", "", text)
        return text

    return "Untitled"


def _extract_brief(soup: BeautifulSoup) -> str:
    """Extract short brief / description."""
    # og:description
    og = soup.find("meta", property="og:description")
    if og and og.get("content"):
        return og["content"].strip()

    # meta description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()

    return ""


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------

def replace_internal_links(base_dir: Path) -> int:
    """
    Scan all content.md files and replace internal gulfnews.com links
    with local relative paths where the target slug exists locally.
    Returns count of replacements made.
    """
    # Build map: URL path -> slug dir
    slug_dirs = {
        d.name for d in base_dir.iterdir()
        if d.is_dir() and d.name != "images" and (d / "content.md").exists()
    }

    link_re = re.compile(
        r"\[([^\]]*)\]\(https?://(?:www\.)?gulfnews\.com(/[^)]*)\)"
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
            # Strip query params and fragments
            url_path = url_path.split("?")[0].split("#")[0]
            # Get the last meaningful segment as potential slug
            segments = [s for s in url_path.strip("/").split("/") if s]
            if not segments:
                return m.group(0)

            # Try to find matching slug
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
        description="Scrape gulfnews.com articles via sitemap"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-fetch everything, ignoring incremental state",
    )
    parser.add_argument(
        "--slug", type=str, default=None,
        help="Fetch only a specific slug",
    )
    args = parser.parse_args()

    # Ensure output dirs exist
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    state = load_state()

    # -----------------------------------------------------------------------
    # Phase 1: Fetch sitemap URLs
    # -----------------------------------------------------------------------
    print(f"[info] Fetching sitemap from {SITEMAP_URL}")
    entries = fetch_sitemap_urls(SITEMAP_URL)
    print(f"[info] Found {len(entries)} total sitemap URLs")

    # Filter to article URLs
    article_entries = [e for e in entries if is_article_url(e["loc"])]
    print(f"[info] {len(article_entries)} appear to be article URLs")

    if not article_entries:
        print("[warn] No article URLs found in sitemap. Exiting.")
        return

    # Filter to single slug if requested
    if args.slug:
        article_entries = [
            e for e in article_entries
            if args.slug in e["loc"]
        ]
        if not article_entries:
            print(f"[warn] No sitemap entry matching slug '{args.slug}'")
            return
        print(f"[info] Filtered to {len(article_entries)} entries matching --slug")

    # -----------------------------------------------------------------------
    # Phase 2: Scrape articles
    # -----------------------------------------------------------------------
    existing_slugs: set[str] = set()
    content_hashes: dict[str, str] = {}

    # Pre-populate existing slugs from filesystem
    if BASE_DIR.exists():
        for d in BASE_DIR.iterdir():
            if d.is_dir() and d.name != "images":
                existing_slugs.add(d.name)

    # Pre-populate content hashes from state
    for slug_key, slug_state in state.items():
        if isinstance(slug_state, dict) and "content_hash" in slug_state:
            content_hashes[slug_state["content_hash"]] = slug_key

    stats = {"done": 0, "skip": 0, "fail": 0, "dedup": 0}

    def _process_one(entry: dict[str, str]) -> dict[str, Any] | None:
        return process_page(entry, state, existing_slugs, content_hashes, args.force)

    # Process with thread pool
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_process_one, e): e for e in article_entries}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    slug = result["slug"]
                    state[slug] = {
                        "lastmod": result.get("lastmod"),
                        "content_hash": result.get("content_hash"),
                    }
                    stats["done"] += 1
                else:
                    stats["skip"] += 1
            except Exception as exc:
                entry = futures[future]
                print(f"[fail] {entry['loc']}: {exc}")
                stats["fail"] += 1

    # Save state
    save_state(state)

    # -----------------------------------------------------------------------
    # Phase 3: Internal link replacement
    # -----------------------------------------------------------------------
    print("[info] Replacing internal links...")
    link_replacements = replace_internal_links(BASE_DIR)
    print(f"[info] Replaced {link_replacements} internal links")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
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
