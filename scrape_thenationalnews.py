#!/usr/bin/env python3
"""
Scraper for www.thenationalnews.com via sitemap.xml.

Fetches the sitemap, extracts article URLs, downloads and converts each article
to structured markdown + YAML metadata + images.

Usage:
    python3 scrape_thenationalnews.py              # incremental run
    python3 scrape_thenationalnews.py --force       # re-fetch everything
    python3 scrape_thenationalnews.py --slug X      # fetch only slug X
"""

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
_MISSING = []
for _pkg, _imp in [
    ("requests", "requests"),
    ("beautifulsoup4", "bs4"),
    ("pyyaml", "yaml"),
    ("lxml", "lxml"),
]:
    try:
        __import__(_imp)
    except ImportError:
        _MISSING.append(_pkg)
if _MISSING:
    print(
        "Missing dependencies. Install with:\n"
        "  pip3 install --user --break-system-packages "
        + " ".join(_MISSING)
    )
    raise SystemExit(1)

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import argparse
import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

import requests
import yaml
from bs4 import BeautifulSoup, NavigableString, Tag

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "www.thenationalnews.com"
SITEMAP_URL = "https://www.thenationalnews.com/sitemap.xml"
BASE_DIR = Path(__file__).resolve().parent / "the_national"
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
    "/sport/", "/arts-culture/", "/health/", "/climate/",
    "/uae/", "/gulf-news/", "/mena/", "/weekend/",
    "/travel/", "/food/", "/luxury/", "/technology/",
    "/environment/", "/podcasts/", "/weekend/", "/comment/",
]

SKIP_PATH_PATTERNS = {
    "/page/", "/search", "/login", "/signup", "/register",
    "/account", "/settings", "/cart", "/checkout",
    "/api/", "/graphql", "/feed", "/rss",
    "/author/", "/authors/",
}

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".sharing",
    ".newsletter-signup", ".subscription-widget", ".subscribe",
    ".comments", ".comment-section",
    ".author-bio", ".author-card",
    ".disclaimer", ".cookie-banner",
    ".breadcrumb", ".breadcrumbs",
    ".pagination",
    ".ad", ".advertisement", "[class*='promo']",
    "[class*='banner']", "[class*='popup']", "[class*='modal']",
    "[class*='paywall']",
    ".related-stories", ".more-stories", ".also-read",
    ".tags-section",
    ".article-share", ".share-bar",
    ".read-more-wrapper",
    "[data-testid='ad-slot']",
    ".sticky-ad", ".inline-ad",
]

# The National uses a custom CMS. Content selectors in priority order.
CONTENT_SELECTORS = [
    "div.article__content",
    "div.article-body",
    "div[class*='ArticleBody']",
    "div[class*='article-content']",
    "div[class*='story-body']",
    "div.body-content",
    "div.post-content",
    "article .content",
    "article",
    "main",
    "div.content",
    "div#content",
]

# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------
def load_state():
    if FETCH_STATE_FILE.exists():
        with open(FETCH_STATE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_state(state):
    FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FETCH_STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Sitemap parsing
# ---------------------------------------------------------------------------
def fetch_sitemap():
    """Fetch sitemap.xml and return list of dicts with loc/lastmod/changefreq."""
    resp = SESSION.get(SITEMAP_URL, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    ns = ""
    tag = root.tag
    if tag.startswith("{"):
        ns = tag.split("}")[0] + "}"

    entries = []
    for url_el in root.findall(f"{ns}url"):
        loc_el = url_el.find(f"{ns}loc")
        lastmod_el = url_el.find(f"{ns}lastmod")
        changefreq_el = url_el.find(f"{ns}changefreq")

        if loc_el is None or not loc_el.text:
            continue

        entries.append({
            "loc": loc_el.text.strip(),
            "lastmod": lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else None,
            "changefreq": changefreq_el.text.strip() if changefreq_el is not None and changefreq_el.text else None,
        })

    return entries


# ---------------------------------------------------------------------------
# URL classification
# ---------------------------------------------------------------------------
def is_listing_url(url):
    """Return True if the URL looks like a listing/index page rather than an article."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Skip utility pages
    for skip in SKIP_PATH_PATTERNS:
        if skip in path:
            return True

    # Pagination
    if re.search(r"/page/\d+", path):
        return True

    # Pure category/section index: path ends with a known pattern and nothing after
    # e.g. /news/ or /business/ with no slug
    segments = [s for s in path.split("/") if s]
    if not segments:
        return True  # homepage

    # If path is just the domain root section with 0-1 segments
    if len(segments) <= 1:
        return True

    # Check if it ends with only a known content-type dir (no slug after)
    path_with_slash = "/" + "/".join(segments) + "/"
    for pattern in CONTENT_PATH_PATTERNS:
        pat_clean = pattern.strip("/")
        if path_with_slash.rstrip("/") == "/" + pat_clean:
            return True

    return False


def is_article_url(url):
    """Return True if the URL looks like an article page."""
    if is_listing_url(url):
        return False

    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    if len(segments) < 2:
        return False

    # Must have a non-numeric, non-date final slug
    last_seg = segments[-1]

    # Skip pure file extensions
    if re.search(r"\.\w{2,4}$", last_seg) and not last_seg.endswith(".html"):
        return False

    return True


# ---------------------------------------------------------------------------
# Slug generation
# ---------------------------------------------------------------------------
_DATE_SEGMENT_RE = re.compile(r"^\d{4}$|^\d{2}$|^\d{4}-\d{2}$")


def generate_slug(url_path):
    """Extract the last meaningful path segment as slug."""
    path = urlparse(url_path).path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    if not segments:
        return "index"

    # Walk backwards, skip date segments and pure-numeric parents
    for seg in reversed(segments):
        if _DATE_SEGMENT_RE.match(seg):
            continue
        # Keep numeric-only if it's the only option
        slug = seg.lower()
        slug = re.sub(r"[^a-z0-9-]", "-", slug)
        slug = re.sub(r"-+", "-", slug).strip("-")
        if slug:
            return slug

    return segments[-1].lower()


# ---------------------------------------------------------------------------
# Content type detection
# ---------------------------------------------------------------------------
def detect_content_type(url_path):
    """
    Detect content-type and category from URL path.

    /news/some-slug             -> content_type='news', category=None
    /news/forex-news/slug       -> content_type='forex-news', category='news'
    /business/economy/slug      -> content_type='economy', category='business'
    """
    path = urlparse(url_path).path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    # Filter out date segments and the final slug
    content_segments = []
    for seg in segments[:-1]:  # exclude slug
        if _DATE_SEGMENT_RE.match(seg):
            continue
        if re.match(r"^\d+$", seg):
            continue
        content_segments.append(seg)

    if not content_segments:
        return "general", None

    if len(content_segments) == 1:
        return content_segments[0], None

    # Deepest matching pattern = content_type, parent = category
    return content_segments[-1], content_segments[0] if len(content_segments) >= 2 else None


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------
def extract_date(soup, entry_lastmod, url, response_headers=None):
    """
    8-level priority chain for date extraction.
    Returns ISO date string (YYYY-MM-DD) or None.
    """
    # 1. article:published_time
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        return _normalize_date(meta["content"])

    # 2. meta name=date / publish-date
    for name in ("date", "publish-date", "publish_date", "pubdate"):
        meta = soup.find("meta", attrs={"name": name})
        if meta and meta.get("content"):
            return _normalize_date(meta["content"])

    # 3. <time datetime>
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        return _normalize_date(time_el["datetime"])

    # 4. date/timestamp class elements
    for selector in [
        "[class*='date']", "[class*='timestamp']",
        "[class*='Date']", "[class*='publish']",
        "span.date", "span.timestamp",
    ]:
        el = soup.select_one(selector)
        if el and el.get_text(strip=True):
            parsed = _try_parse_date_text(el.get_text(strip=True))
            if parsed:
                return parsed

    # 5. JSON-LD datePublished
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict):
                dp = data.get("datePublished")
                if dp:
                    return _normalize_date(dp)
        except (json.JSONDecodeError, TypeError, IndexError):
            pass

    # 6. URL path date segments
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"/(\d{4})/(\d{2})/", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"

    # 7. Sitemap lastmod
    if entry_lastmod:
        return _normalize_date(entry_lastmod)

    # 8. HTTP Last-Modified
    if response_headers:
        lm = response_headers.get("Last-Modified")
        if lm:
            return _normalize_date(lm)

    return None


def _normalize_date(raw):
    """Normalize various date formats to YYYY-MM-DD."""
    if not raw:
        return None
    raw = raw.strip()

    # ISO-ish: 2026-07-15T10:30:00...
    m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)

    # RFC 2822 style: Thu, 15 Jul 2026 ...
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    return None


def _try_parse_date_text(text):
    """Attempt to parse human-readable date text."""
    import re as _re

    # "July 15, 2026" / "15 July 2026"
    months = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12",
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    }

    text_lower = text.lower().strip()

    # Month DD, YYYY
    m = _re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", text_lower)
    if m and m.group(1) in months:
        return f"{m.group(3)}-{months[m.group(1)]}-{int(m.group(2)):02d}"

    # DD Month YYYY
    m = _re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text_lower)
    if m and m.group(2) in months:
        return f"{m.group(3)}-{months[m.group(2)]}-{int(m.group(1)):02d}"

    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------
def extract_tags(soup):
    """Extract tags from multiple sources, deduplicate, normalize."""
    tags = set()

    # 1. meta keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        for t in meta_kw["content"].split(","):
            t = t.strip().lower()
            if t and len(t) < 80:
                tags.add(t)

    # 2. article:tag meta (multiple)
    for meta in soup.find_all("meta", property="article:tag"):
        if meta.get("content"):
            tags.add(meta["content"].strip().lower())

    # 3. JSON-LD keywords
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict):
                kw = data.get("keywords")
                if isinstance(kw, list):
                    for k in kw:
                        if isinstance(k, str):
                            tags.add(k.strip().lower())
                elif isinstance(kw, str):
                    for k in kw.split(","):
                        k = k.strip().lower()
                        if k:
                            tags.add(k)
                # about / mentions
                for field in ("about", "mentions"):
                    items = data.get(field, [])
                    if isinstance(items, dict):
                        items = [items]
                    for item in items:
                        if isinstance(item, dict) and item.get("name"):
                            tags.add(item["name"].strip().lower())
        except (json.JSONDecodeError, TypeError, IndexError):
            pass

    # 4. Visible tag links
    for selector in [
        'a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a",
        "[class*='tag-link']", "[class*='topic-link']",
        ".article__tags a", ".story-tags a",
        ".tag-list a", "[class*='Tag'] a",
    ]:
        for a in soup.select(selector):
            text = a.get_text(strip=True).lower()
            if text and len(text) < 80:
                tags.add(text)

    # 5. Category links
    for selector in [".cat-links a", ".entry-categories a", ".category a"]:
        for a in soup.select(selector):
            text = a.get_text(strip=True).lower()
            if text and len(text) < 80:
                tags.add(text)

    return sorted(tags)


# ---------------------------------------------------------------------------
# Image downloading
# ---------------------------------------------------------------------------
def download_image(img_url, slug, session=None):
    """
    Download an image and return the local filename.
    Handles WP proxies. Returns None on failure.
    """
    if not img_url or img_url.startswith("data:"):
        return None

    sess = session or SESSION

    # Resolve WP proxy URLs
    original_path = img_url
    parsed = urlparse(img_url)
    is_wp_proxy = re.match(r"i[0-3]\.wp\.com", parsed.netloc or "")
    if is_wp_proxy:
        # The real image path follows the proxy domain
        original_path = parsed.path
    else:
        original_path = parsed.path

    # Strip query params for extension detection
    clean_path = original_path.split("?")[0]
    ext = os.path.splitext(clean_path)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp", ".tiff"):
        ext = ".jpg"  # fallback

    # Generate filename
    url_hash = hashlib.md5(img_url.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    filepath = IMAGES_DIR / filename

    if filepath.exists():
        return filename

    try:
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        resp = sess.get(img_url, timeout=20, stream=True)
        resp.raise_for_status()
        with open(filepath, "wb") as fh:
            for chunk in resp.iter_content(8192):
                fh.write(chunk)
        return filename
    except Exception as exc:
        print(f"  [img-err] {img_url}: {exc}")
        return None


# ---------------------------------------------------------------------------
# HTML -> Markdown converter
# ---------------------------------------------------------------------------
SKIP_TAGS = {
    "script", "style", "noscript", "svg", "button",
    "nav", "footer", "header", "iframe", "form", "input",
    "select", "textarea",
}


def html_to_markdown(element, slug, depth=0):
    """Recursively convert a BS4 element to markdown."""
    if isinstance(element, NavigableString):
        text = str(element)
        if not text.strip():
            return ""
        return text

    if not isinstance(element, Tag):
        return ""

    tag_name = element.name.lower() if element.name else ""

    if tag_name in SKIP_TAGS:
        return ""

    # Skip noise by class/id
    el_classes = " ".join(element.get("class", []))
    el_id = element.get("id", "")
    noise_indicators = [
        "share", "social", "related", "recommended", "newsletter",
        "subscribe", "comment", "sidebar", "ad-slot", "promo",
        "banner", "popup", "modal", "paywall", "cookie",
        "breadcrumb", "pagination",
    ]
    for indicator in noise_indicators:
        if indicator in el_classes.lower() or indicator in el_id.lower():
            return ""

    # Process by tag
    if tag_name in ("h1", "h2", "h3", "h4", "h5", "h6"):
        level = int(tag_name[1])
        inner = _children_to_md(element, slug, depth)
        if inner.strip():
            return f"\n\n{'#' * level} {inner.strip()}\n\n"
        return ""

    if tag_name == "p":
        inner = _children_to_md(element, slug, depth)
        if inner.strip():
            return f"\n\n{inner.strip()}\n\n"
        return ""

    if tag_name in ("strong", "b"):
        inner = _children_to_md(element, slug, depth)
        if inner.strip():
            return f"**{inner.strip()}**"
        return ""

    if tag_name in ("em", "i"):
        inner = _children_to_md(element, slug, depth)
        if inner.strip():
            return f"*{inner.strip()}*"
        return ""

    if tag_name == "a":
        href = element.get("href", "")
        # If wrapping an image, recurse without flattening
        if element.find("img"):
            return _children_to_md(element, slug, depth)
        inner = _children_to_md(element, slug, depth)
        if inner.strip() and href:
            return f"[{inner.strip()}]({href})"
        return inner

    if tag_name == "img":
        src = element.get("src") or element.get("data-src") or ""
        alt = element.get("alt", "").strip()
        if src:
            filename = download_image(src, slug, SESSION)
            if filename:
                return f"![{alt}](../images/{filename})"
        return ""

    if tag_name == "picture":
        img = element.find("img")
        if img:
            return html_to_markdown(img, slug, depth)
        source = element.find("source")
        if source and source.get("srcset"):
            srcset = source["srcset"].split(",")[0].strip().split(" ")[0]
            filename = download_image(srcset, slug, SESSION)
            if filename:
                return f"![](../images/{filename})"
        return ""

    if tag_name == "figure":
        parts = []
        for child in element.children:
            if isinstance(child, Tag) and child.name == "figcaption":
                cap = child.get_text(strip=True)
                if cap:
                    parts.append(f"\n*{cap}*\n")
            else:
                parts.append(html_to_markdown(child, slug, depth))
        return "\n".join(parts)

    if tag_name == "blockquote":
        inner = _children_to_md(element, slug, depth)
        lines = inner.strip().split("\n")
        quoted = "\n".join(f"> {line}" for line in lines)
        return f"\n\n{quoted}\n\n"

    if tag_name == "pre":
        code_el = element.find("code")
        if code_el:
            lang = ""
            code_classes = code_el.get("class", [])
            for c in code_classes:
                if c.startswith("language-"):
                    lang = c.replace("language-", "")
                    break
            code_text = code_el.get_text()
            return f"\n\n```{lang}\n{code_text}\n```\n\n"
        return f"\n\n```\n{element.get_text()}\n```\n\n"

    if tag_name == "code" and not (element.parent and element.parent.name == "pre"):
        return f"`{element.get_text()}`"

    if tag_name in ("ul", "ol"):
        items = []
        for i, li in enumerate(element.find_all("li", recursive=False)):
            inner = _children_to_md(li, slug, depth + 1)
            prefix = "- " if tag_name == "ul" else f"{i + 1}. "
            items.append(f"{prefix}{inner.strip()}")
        return "\n\n" + "\n".join(items) + "\n\n"

    if tag_name == "li":
        return _children_to_md(element, slug, depth)

    if tag_name == "br":
        return "\n"

    if tag_name == "hr":
        return "\n\n---\n\n"

    if tag_name == "table":
        return _convert_table(element, slug, depth)

    # Default: recurse children
    return _children_to_md(element, slug, depth)


def _children_to_md(element, slug, depth):
    parts = []
    for child in element.children:
        parts.append(html_to_markdown(child, slug, depth))
    return "".join(parts)


def _convert_table(table, slug, depth):
    """Convert an HTML table to markdown table."""
    rows = []
    for tr in table.find_all("tr"):
        cells = []
        for cell in tr.find_all(["th", "td"]):
            cells.append(_children_to_md(cell, slug, depth).strip().replace("|", "\\|"))
        rows.append(cells)

    if not rows:
        return ""

    max_cols = max(len(r) for r in rows)
    for r in rows:
        while len(r) < max_cols:
            r.append("")

    lines = []
    lines.append("| " + " | ".join(rows[0]) + " |")
    lines.append("| " + " | ".join(["---"] * max_cols) + " |")
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n\n" + "\n".join(lines) + "\n\n"


def clean_markdown(text):
    """Collapse excessive blank lines."""
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() + "\n"


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------
def find_content_container(soup):
    """Find the best content container element."""
    for selector in CONTENT_SELECTORS:
        container = soup.select_one(selector)
        if container:
            return container
    return soup.find("body")


def remove_noise(soup):
    """Remove noise elements before content extraction."""
    for selector in NOISE_SELECTORS:
        for el in soup.select(selector):
            el.decompose()


def is_article_page(soup, word_count):
    """Determine if a page is a real article vs a listing."""
    # Check for article indicators
    has_og_article = False
    og_type = soup.find("meta", property="og:type")
    if og_type and og_type.get("content", "").lower() == "article":
        has_og_article = True

    has_publish_time = soup.find("meta", property="article:published_time") is not None

    # Count internal links vs content
    if word_count < 200:
        return False

    if has_og_article or has_publish_time:
        return True

    # If it has a single h1, likely an article
    h1_tags = soup.find_all("h1")
    if len(h1_tags) == 1 and word_count >= 200:
        return True

    return word_count >= 200


def check_paywall(soup):
    """Check if content appears to be behind a paywall."""
    paywall_indicators = [
        "[class*='paywall']", "[class*='subscribe-to-read']",
        "[class*='premium-content']", "[class*='locked']",
        "[data-testid='paywall']",
    ]
    for sel in paywall_indicators:
        if soup.select_one(sel):
            return True

    # Check for truncation text
    for text in ["Subscribe to continue reading", "This content is for subscribers",
                  "Sign in to read", "Premium content"]:
        if soup.find(string=re.compile(re.escape(text), re.I)):
            return True

    return False


# ---------------------------------------------------------------------------
# Extract metadata
# ---------------------------------------------------------------------------
def extract_title(soup):
    """Extract article title."""
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)

    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        return og_title["content"].strip()

    title_el = soup.find("title")
    if title_el:
        title_text = title_el.get_text(strip=True)
        # Strip site name suffix
        for sep in [" | ", " - ", " — ", " – "]:
            if sep in title_text:
                return title_text.split(sep)[0].strip()
        return title_text

    return "Untitled"


def extract_brief(soup):
    """Extract short description/brief."""
    og_desc = soup.find("meta", property="og:description")
    if og_desc and og_desc.get("content"):
        return og_desc["content"].strip()

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()

    # Look for subtitle element
    for sel in [".article__subtitle", ".subtitle", ".standfirst", ".deck",
                "[class*='subtitle']", "[class*='standfirst']"]:
        el = soup.select_one(sel)
        if el:
            return el.get_text(strip=True)

    return None


# ---------------------------------------------------------------------------
# Process a single article
# ---------------------------------------------------------------------------
def process_article(entry, state, force=False, content_hashes=None):
    """
    Fetch and process a single article.
    Returns (slug, success, skipped, dedup) tuple.
    """
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq")

    slug = generate_slug(url)

    # Incremental check
    if not force:
        stored = state.get(slug)
        if stored:
            stored_lastmod = stored if isinstance(stored, str) else stored.get("lastmod")
            slug_dir = BASE_DIR / slug
            if stored_lastmod == lastmod and (slug_dir / "content.md").exists():
                return slug, False, True, False

    time.sleep(REQUEST_DELAY)

    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        print(f"  [err] {slug}: {exc}")
        return slug, False, False, False

    soup = BeautifulSoup(resp.text, "lxml")

    # Remove noise elements
    remove_noise(soup)

    # Find content
    container = find_content_container(soup)
    if not container:
        print(f"  [skip] {slug} -- no content container found")
        return slug, False, True, False

    # Convert to markdown
    md_content = html_to_markdown(container, slug)
    md_content = clean_markdown(md_content)

    # Word count check
    word_count = len(md_content.split())
    if not is_article_page(soup, word_count):
        print(f"  [skip] {slug} -- listing page ({word_count} words)")
        return slug, False, True, False

    # Content dedup
    content_hash = hashlib.md5(md_content.encode()).hexdigest()
    if content_hashes is not None:
        if content_hash in content_hashes:
            original = content_hashes[content_hash]
            print(f"  [dedup] {slug} -- duplicate of {original}")
            return slug, False, False, True
        content_hashes[content_hash] = slug

    # Check paywall
    truncated = check_paywall(soup)

    # Extract metadata
    title = extract_title(soup)
    brief = extract_brief(soup)
    publish_date = extract_date(soup, lastmod, url, dict(resp.headers))
    tags = extract_tags(soup)
    content_type, category = detect_content_type(url)

    # Build meta dict
    meta = {
        "title": title,
        "publish-date": publish_date,
        "change-frequency": changefreq or "unknown",
        "short-brief": brief,
        "source-url": url,
        "content-type": content_type,
        "category": category,
        "tags": tags if tags else [],
    }
    if truncated:
        meta["truncated"] = True

    # Write files
    slug_dir = BASE_DIR / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    with open(slug_dir / "meta.yaml", "w", encoding="utf-8") as fh:
        yaml.dump(meta, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)

    with open(slug_dir / "content.md", "w", encoding="utf-8") as fh:
        fh.write(md_content)

    # Update state
    state[slug] = {
        "lastmod": lastmod,
        "content_hash": content_hash,
    }

    print(f"  [ok] {slug} ({word_count} words, {len(tags)} tags)")
    return slug, True, False, False


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------
def replace_internal_links(base_dir, domain):
    """
    After scraping, replace internal links in all content.md files
    with local relative paths where the target slug exists locally.
    """
    slug_dirs = set()
    for d in base_dir.iterdir():
        if d.is_dir() and d.name != "images" and not d.name.startswith("."):
            slug_dirs.add(d.name)

    if not slug_dirs:
        return 0

    replacements = 0
    domain_pattern = re.compile(
        r"\[([^\]]+)\]\(https?://(?:www\.)?"
        + re.escape(domain.replace("www.", ""))
        + r"/[^)]*?/([a-z0-9-]+?)/?(?:\?[^)]*)?\)"
    )

    for slug_name in slug_dirs:
        content_path = base_dir / slug_name / "content.md"
        if not content_path.exists():
            continue

        with open(content_path, "r", encoding="utf-8") as fh:
            content = fh.read()

        def _replace_link(m):
            nonlocal replacements
            link_text = m.group(1)
            target_slug = m.group(2)
            if target_slug in slug_dirs and target_slug != slug_name:
                replacements += 1
                return f"[{link_text}](../{target_slug}/content.md)"
            return m.group(0)

        new_content = domain_pattern.sub(_replace_link, content)

        if new_content != content:
            with open(content_path, "w", encoding="utf-8") as fh:
                fh.write(new_content)

    return replacements


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Scrape thenationalnews.com via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything ignoring state")
    parser.add_argument("--slug", type=str, default=None, help="Fetch only this slug")
    args = parser.parse_args()

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    state = load_state()
    content_hashes = {}

    # Populate content_hashes from existing state
    for s, val in state.items():
        if isinstance(val, dict) and val.get("content_hash"):
            content_hashes[val["content_hash"]] = s

    # Fetch sitemap
    print(f"Fetching sitemap: {SITEMAP_URL}")
    try:
        entries = fetch_sitemap()
    except Exception as exc:
        print(f"Failed to fetch sitemap: {exc}")
        raise SystemExit(1)

    print(f"Found {len(entries)} URLs in sitemap")

    # Filter to article URLs
    article_entries = [e for e in entries if is_article_url(e["loc"])]
    listing_count = len(entries) - len(article_entries)
    print(f"Identified {len(article_entries)} article URLs ({listing_count} listings/non-article skipped)")

    # Filter by slug if requested
    if args.slug:
        article_entries = [e for e in article_entries if generate_slug(e["loc"]) == args.slug]
        if not article_entries:
            print(f"No entry found matching slug: {args.slug}")
            raise SystemExit(1)
        print(f"Filtered to {len(article_entries)} entry for slug: {args.slug}")

    # Process articles
    fetched = 0
    skipped = 0
    deduped = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for entry in article_entries:
            future = executor.submit(
                process_article, entry, state, args.force, content_hashes
            )
            futures[future] = entry

        for future in as_completed(futures):
            try:
                slug, success, was_skipped, was_dedup = future.result()
                if success:
                    fetched += 1
                elif was_skipped:
                    skipped += 1
                elif was_dedup:
                    deduped += 1
                else:
                    failed += 1
            except Exception as exc:
                entry = futures[future]
                print(f"  [err] {entry['loc']}: {exc}")
                failed += 1

    # Save state
    save_state(state)

    # Post-scrape: internal link replacement
    print("\nReplacing internal links...")
    link_replacements = replace_internal_links(BASE_DIR, DOMAIN)
    print(f"Replaced {link_replacements} internal links")

    # Summary
    print(f"\n{'='*50}")
    print(f"Scraping complete:")
    print(f"  Fetched:    {fetched}")
    print(f"  Skipped:    {skipped}")
    print(f"  Deduped:    {deduped}")
    print(f"  Failed:     {failed}")
    print(f"  Links fixed:{link_replacements}")
    print(f"  Output:     {BASE_DIR}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
