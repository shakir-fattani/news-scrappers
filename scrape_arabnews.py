#!/usr/bin/env python3
"""
Sitemap-based scraper for www.arabnews.com

Fetches the sitemap index at https://www.arabnews.com/sitemap.xml,
discovers paginated child sitemaps (?page=0, ?page=1, ...),
merges all URL entries, and scrapes each article page into
structured markdown + YAML metadata + images.

Usage:
    python3 scrape_arabnews.py              # incremental run
    python3 scrape_arabnews.py --force      # re-fetch everything
    python3 scrape_arabnews.py --slug X     # fetch only slug X

Output: arab_news/<slug>/meta.yaml + content.md + images/
"""

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
try:
    import requests
    from bs4 import BeautifulSoup
    import yaml
    import lxml  # noqa: F401
except ImportError:
    print(
        "Missing dependencies. Install with:\n"
        "  pip3 install --user --break-system-packages "
        "requests beautifulsoup4 pyyaml lxml"
    )
    raise SystemExit(1)

import argparse
import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock
from urllib.parse import urljoin, urlparse, unquote

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "www.arabnews.com"
SITEMAP_INDEX_URL = f"https://{DOMAIN}/sitemap.xml"
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR / "arab_news"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

REQUEST_DELAY = 1.0
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
    "/sport/", "/sports/", "/entertainment/", "/culture/",
    "/saudi-arabia/", "/middle-east/", "/offbeat/", "/features/",
    "/node/",
]

SKIP_URL_PATTERNS = re.compile(
    r"(/page/\d+|/archive|/search|/tags?/|/author/|/writers?/|"
    r"/about|/contact|/privacy|/terms|/login|/signup|/register|"
    r"/rss|/feed|/print/|\?page=\d+$|/taxonomy/)",
    re.IGNORECASE,
)

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup",
    ".subscription-widget", ".comments", ".comment-section",
    ".author-bio", ".disclaimer", ".cookie-banner",
    ".breadcrumb", ".pagination", ".ad", ".advertisement",
    "[class*='promo']", "[class*='banner']", "[class*='popup']",
    "[class*='modal']", ".block-views", ".field--name-field-tags",
    ".article-social", ".article-tools", ".article-footer",
    ".related-news", ".more-stories", ".trending",
    ".region-sidebar", ".pane-node-field-image",
    "#block-system-main .contextual-links-wrapper",
    ".addtoany_share_save_container", ".article-share",
]

# Arab News (Drupal) content selectors in priority order
CONTENT_SELECTORS = [
    "div.field--name-body",
    "div.field-name-body",
    "article .node__content .field--name-body",
    "div.article-body",
    "div.article__content",
    "div.body-content",
    "div.content-body",
    ".field--name-field-ar-news-body",
    "article .content",
    "article",
    "main",
    "div.content",
    "div#content",
]

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------
_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT})
_request_lock = Lock()
_last_request_time = 0.0


def throttled_get(url, **kwargs):
    """GET with per-thread rate limiting (1 s between requests)."""
    global _last_request_time
    with _request_lock:
        elapsed = time.time() - _last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        _last_request_time = time.time()
    kwargs.setdefault("timeout", 30)
    return _session.get(url, **kwargs)


# ---------------------------------------------------------------------------
# Sitemap fetching
# ---------------------------------------------------------------------------
def fetch_sitemap_index(url):
    """Return list of child sitemap URLs from the sitemap index."""
    resp = throttled_get(url)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    child_urls = []
    # Sitemap index format
    for sitemap_el in root.findall("sm:sitemap", NS):
        loc = sitemap_el.findtext("sm:loc", namespaces=NS)
        if loc:
            child_urls.append(loc.strip())

    # Also check for paginated pattern: ?page=0, ?page=1, ...
    # Arab News uses ?page=N on its child sitemaps
    if not child_urls:
        # Maybe it's already a urlset — treat the index itself as a child
        if root.findall("sm:url", NS):
            child_urls.append(url)

    return child_urls


def discover_paginated_sitemaps(base_urls):
    """Given a list of child sitemap URLs, discover additional ?page=N pages."""
    all_urls = set()
    for base in base_urls:
        all_urls.add(base)
        # If the URL already has ?page=, don't paginate further
        if "?page=" in base:
            continue
        # Try paginating: ?page=0, ?page=1, ... until 404 or empty
        page = 0
        while True:
            paged = f"{base}?page={page}"
            try:
                resp = throttled_get(paged)
                if resp.status_code != 200:
                    break
                root = ET.fromstring(resp.content)
                urls_in_page = root.findall("sm:url", NS)
                if not urls_in_page:
                    break
                all_urls.add(paged)
                page += 1
            except Exception:
                break
    return sorted(all_urls)


def parse_sitemap_urls(sitemap_url):
    """Parse a single sitemap XML and return list of entry dicts."""
    entries = []
    try:
        resp = throttled_get(sitemap_url)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for url_el in root.findall("sm:url", NS):
            loc = url_el.findtext("sm:loc", namespaces=NS)
            lastmod = url_el.findtext("sm:lastmod", namespaces=NS)
            changefreq = url_el.findtext("sm:changefreq", namespaces=NS)
            if loc:
                entries.append({
                    "loc": loc.strip(),
                    "lastmod": (lastmod.strip() if lastmod else None),
                    "changefreq": (changefreq.strip() if changefreq else None),
                })
    except Exception as exc:
        print(f"[error] Failed to parse sitemap {sitemap_url}: {exc}")
    return entries


def fetch_all_sitemap_entries():
    """Fetch sitemap index, discover paginated children, merge all entries."""
    print(f"[sitemap] Fetching index: {SITEMAP_INDEX_URL}")
    child_urls = fetch_sitemap_index(SITEMAP_INDEX_URL)
    print(f"[sitemap] Found {len(child_urls)} child sitemap(s)")

    print("[sitemap] Discovering paginated child sitemaps...")
    all_sitemap_urls = discover_paginated_sitemaps(child_urls)
    print(f"[sitemap] Total sitemap pages to parse: {len(all_sitemap_urls)}")

    all_entries = []
    seen_locs = set()
    for smap_url in all_sitemap_urls:
        entries = parse_sitemap_urls(smap_url)
        for entry in entries:
            if entry["loc"] not in seen_locs:
                seen_locs.add(entry["loc"])
                all_entries.append(entry)

    print(f"[sitemap] Total unique URL entries: {len(all_entries)}")
    return all_entries


# ---------------------------------------------------------------------------
# URL classification helpers
# ---------------------------------------------------------------------------
def is_listing_page_url(url):
    """Return True if the URL looks like a listing / pagination page."""
    path = urlparse(url).path.rstrip("/")
    if SKIP_URL_PATTERNS.search(url):
        return True
    # Pure category landing pages (path is just a content pattern with nothing after)
    for pat in CONTENT_PATH_PATTERNS:
        stripped = pat.rstrip("/")
        if path == stripped:
            return True
    return False


def detect_content_type(url_path):
    """Extract content-type and category from URL path segments.

    Returns (content_type, category) tuple.
    """
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    matched_patterns = []
    for pat in CONTENT_PATH_PATTERNS:
        pat_clean = pat.strip("/")
        if pat_clean in segments:
            matched_patterns.append(pat_clean)

    if not matched_patterns:
        return ("article", None)

    if len(matched_patterns) >= 2:
        # Deepest match is content_type, parent is category
        # Find which appears later in the URL
        best_type = matched_patterns[-1]
        best_cat = matched_patterns[-2]
        return (best_type, best_cat)

    return (matched_patterns[0], None)


def generate_slug(url):
    """Derive a filesystem-safe slug from the URL path.

    Uses the last meaningful path segment, stripping date and
    numeric-only parent segments.
    """
    path = urlparse(url).path.rstrip("/")
    segments = [s for s in path.split("/") if s]
    if not segments:
        return "index"

    # Strip date-like segments (4-digit year, 1-2 digit month/day)
    date_re = re.compile(r"^\d{1,4}$")
    meaningful = []
    for seg in segments:
        if date_re.match(seg) and len(seg) <= 4:
            continue
        meaningful.append(seg)

    # Strip known content-type path segments to find the actual slug
    content_type_segs = {p.strip("/") for p in CONTENT_PATH_PATTERNS}
    slug_candidates = [s for s in meaningful if s not in content_type_segs]

    if slug_candidates:
        slug = slug_candidates[-1]
    elif meaningful:
        slug = meaningful[-1]
    else:
        slug = segments[-1]

    # Normalise
    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    slug = slug.strip("-")
    return slug or "untitled"


# ---------------------------------------------------------------------------
# Fetch state (incremental)
# ---------------------------------------------------------------------------
def load_fetch_state():
    if FETCH_STATE_FILE.exists():
        with open(FETCH_STATE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_fetch_state(state):
    FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FETCH_STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------
def extract_publish_date(soup, entry_lastmod=None):
    """Try multiple date sources in priority order, return ISO date string."""

    # 1. article:published_time
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        return _normalise_date(meta["content"])

    # 2. meta name="date" / "publish-date"
    for name in ("date", "publish-date", "pubdate", "publication_date"):
        meta = soup.find("meta", attrs={"name": name})
        if meta and meta.get("content"):
            return _normalise_date(meta["content"])

    # 3. <time datetime>
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        return _normalise_date(time_el["datetime"])

    # 4. class-based date spans
    for sel in [
        "[class*='date']", "[class*='timestamp']",
        ".article-date", ".post-date", ".article-publish-date",
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            parsed = _try_parse_date_text(text)
            if parsed:
                return parsed

    # 5. JSON-LD datePublished
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            dp = data.get("datePublished")
            if dp:
                return _normalise_date(dp)
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass

    # 6. URL path date segments — handled externally if needed

    # 7. Sitemap lastmod fallback
    if entry_lastmod:
        return _normalise_date(entry_lastmod)

    # 8. No date found
    return None


def _normalise_date(raw):
    """Best-effort normalise a date string to YYYY-MM-DD."""
    if not raw:
        return None
    raw = raw.strip()
    # Already ISO-ish
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if match:
        return match.group(0)
    # Try common formats
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(raw[:30], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw[:10] if len(raw) >= 10 else raw


def _try_parse_date_text(text):
    """Try to parse visible date text."""
    if not text:
        return None
    text = text.strip()
    for fmt in (
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%B %d %Y",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------
def extract_tags(soup):
    """Merge tags from meta, JSON-LD, and visible elements."""
    tags = set()

    # 1. meta keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        for kw in meta_kw["content"].split(","):
            kw = kw.strip().lower()
            if kw:
                tags.add(kw)

    # 2. article:tag (multiple)
    for meta in soup.find_all("meta", property="article:tag"):
        val = (meta.get("content") or "").strip().lower()
        if val:
            tags.add(val)

    # 3. JSON-LD keywords
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            kws = data.get("keywords")
            if isinstance(kws, list):
                for kw in kws:
                    tags.add(str(kw).strip().lower())
            elif isinstance(kws, str):
                for kw in kws.split(","):
                    kw = kw.strip().lower()
                    if kw:
                        tags.add(kw)
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass

    # 4. Visible tag links
    for sel in [
        'a[rel="tag"]', ".tags a", ".post-tags a",
        ".article-tags a", "[class*='tag-link']",
        ".field--name-field-tags a", ".field-name-field-tags a",
    ]:
        for el in soup.select(sel):
            val = el.get_text(strip=True).lower()
            if val and len(val) < 100:
                tags.add(val)

    # 5. Category links
    for sel in [".cat-links a", ".entry-categories a", ".article-category a"]:
        for el in soup.select(sel):
            val = el.get_text(strip=True).lower()
            if val:
                tags.add(val)

    return sorted(tags)


# ---------------------------------------------------------------------------
# Image handling
# ---------------------------------------------------------------------------
def _wp_proxy_clean(url):
    """Handle WordPress i0-i3.wp.com proxy URLs — return (fetch_url, filename_source)."""
    parsed = urlparse(url)
    if re.match(r"i[0-3]\.wp\.com", parsed.netloc):
        # Path after the proxy host is the original URL path
        original_path = parsed.path
        clean_path = original_path.split("?")[0]
        return (url.split("?")[0], clean_path)
    return (url.split("?")[0], parsed.path)


def _image_ext(path):
    """Extract extension from a URL path."""
    path = path.split("?")[0]
    _, ext = os.path.splitext(path)
    ext = ext.lower()
    if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp"):
        return ext
    return ".jpg"


def download_image(img_url, slug):
    """Download image, return local filename or None."""
    if not img_url or img_url.startswith("data:"):
        return None

    # Resolve relative URLs
    if img_url.startswith("//"):
        img_url = "https:" + img_url
    elif img_url.startswith("/"):
        img_url = f"https://{DOMAIN}{img_url}"

    fetch_url, name_source = _wp_proxy_clean(img_url)
    ext = _image_ext(name_source)
    url_hash = hashlib.md5(img_url.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    dest = IMAGES_DIR / filename

    if dest.exists():
        return filename

    try:
        resp = throttled_get(fetch_url)
        if resp.status_code == 200 and len(resp.content) > 100:
            IMAGES_DIR.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as fh:
                fh.write(resp.content)
            return filename
    except Exception as exc:
        print(f"[warn] Image download failed {img_url}: {exc}")
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
    """Recursively convert a BeautifulSoup element to Markdown."""
    if element is None:
        return ""

    from bs4 import NavigableString, Tag

    if isinstance(element, NavigableString):
        text = str(element)
        if not text.strip():
            return ""
        return text.replace("\n", " ")

    if not isinstance(element, Tag):
        return ""

    tag = element.name.lower() if element.name else ""

    if tag in SKIP_TAGS:
        return ""

    # Skip noise by class
    el_class = " ".join(element.get("class", []))
    if any(noise in el_class for noise in [
        "share", "social", "comment", "subscribe", "newsletter",
        "related", "recommended", "sidebar", "promo", "banner",
        "popup", "modal", "breadcrumb", "pagination", "ad-",
        "advertisement", "cookie",
    ]):
        return ""

    children_md = ""
    for child in element.children:
        children_md += html_to_markdown(child, slug, depth + 1)

    # Headings
    if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
        level = int(tag[1])
        text = element.get_text(strip=True)
        if text:
            return f"\n\n{'#' * level} {text}\n\n"
        return ""

    # Paragraph
    if tag == "p":
        text = children_md.strip()
        if text:
            return f"\n\n{text}\n\n"
        return ""

    # Bold
    if tag in ("strong", "b"):
        text = children_md.strip()
        return f"**{text}**" if text else ""

    # Italic
    if tag in ("em", "i"):
        text = children_md.strip()
        return f"*{text}*" if text else ""

    # Links
    if tag == "a":
        href = element.get("href", "")
        # If wrapping an image, recurse into children
        if element.find("img"):
            return children_md
        text = element.get_text(strip=True)
        if text and href:
            return f"[{text}]({href})"
        return text or ""

    # Images
    if tag == "img":
        src = element.get("src") or element.get("data-src") or ""
        alt = element.get("alt", "").strip()
        if src:
            local_name = download_image(src, slug)
            if local_name:
                return f"![{alt}](../images/{local_name})"
            return f"![{alt}]({src})"
        return ""

    # Picture — find inner img
    if tag == "picture":
        img = element.find("img")
        if img:
            return html_to_markdown(img, slug, depth + 1)
        source = element.find("source")
        if source and source.get("srcset"):
            src = source["srcset"].split(",")[0].strip().split(" ")[0]
            local_name = download_image(src, slug)
            if local_name:
                return f"![](../images/{local_name})"
        return ""

    # Figure
    if tag == "figure":
        parts = []
        for child in element.children:
            if isinstance(child, Tag):
                if child.name == "figcaption":
                    caption = child.get_text(strip=True)
                    if caption:
                        parts.append(f"*{caption}*")
                else:
                    parts.append(html_to_markdown(child, slug, depth + 1))
        return "\n\n" + "\n".join(parts) + "\n\n"

    # Blockquote
    if tag == "blockquote":
        text = children_md.strip()
        if text:
            lines = text.split("\n")
            quoted = "\n".join(f"> {line}" for line in lines)
            return f"\n\n{quoted}\n\n"
        return ""

    # Code blocks
    if tag == "pre":
        code_el = element.find("code")
        text = code_el.get_text() if code_el else element.get_text()
        lang = ""
        if code_el:
            cls = " ".join(code_el.get("class", []))
            lang_match = re.search(r"language-(\w+)", cls)
            if lang_match:
                lang = lang_match.group(1)
        return f"\n\n```{lang}\n{text}\n```\n\n"

    if tag == "code" and element.parent and element.parent.name != "pre":
        text = element.get_text()
        return f"`{text}`"

    # Lists
    if tag in ("ul", "ol"):
        items = []
        for i, li in enumerate(element.find_all("li", recursive=False)):
            li_text = html_to_markdown(li, slug, depth + 1).strip()
            if tag == "ol":
                items.append(f"{i + 1}. {li_text}")
            else:
                items.append(f"- {li_text}")
        return "\n\n" + "\n".join(items) + "\n\n"

    if tag == "li":
        return children_md.strip()

    # Line break
    if tag == "br":
        return "\n"

    # Horizontal rule
    if tag == "hr":
        return "\n\n---\n\n"

    # Tables
    if tag == "table":
        return _table_to_markdown(element, slug)

    # Default: recurse
    return children_md


def _table_to_markdown(table, slug):
    """Convert an HTML table to markdown."""
    rows = []
    for tr in table.find_all("tr"):
        cells = []
        for td in tr.find_all(["th", "td"]):
            cells.append(td.get_text(strip=True).replace("|", "\\|"))
        if cells:
            rows.append(cells)

    if not rows:
        return ""

    # Normalise column count
    max_cols = max(len(r) for r in rows)
    for row in rows:
        while len(row) < max_cols:
            row.append("")

    lines = []
    header = rows[0]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join("---" for _ in header) + " |")
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n\n" + "\n".join(lines) + "\n\n"


def _clean_markdown(md):
    """Collapse excessive blank lines."""
    md = re.sub(r"\n{3,}", "\n\n", md)
    return md.strip() + "\n"


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------
def find_content_container(soup):
    """Return the best content container element using priority selectors."""
    for sel in CONTENT_SELECTORS:
        el = soup.select_one(sel)
        if el:
            return el
    return soup.find("body")


def remove_noise(soup):
    """Remove navigation, ads, social widgets, etc."""
    for sel in NOISE_SELECTORS:
        for el in soup.select(sel):
            el.decompose()
    return soup


def is_article_page(soup, url):
    """Return True if this looks like an article (not a listing page)."""
    # Check og:type
    og_type = soup.find("meta", property="og:type")
    if og_type and og_type.get("content", "").lower() == "article":
        return True

    # Check for article:published_time
    if soup.find("meta", property="article:published_time"):
        return True

    # Check JSON-LD for article type
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            t = data.get("@type", "")
            if isinstance(t, str) and t.lower() in (
                "newsarticle", "article", "blogposting", "reportagenewsarticle",
            ):
                return True
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass

    # Check word count in content area
    container = find_content_container(soup)
    if container:
        text = container.get_text(separator=" ", strip=True)
        word_count = len(text.split())
        link_count = len(container.find_all("a"))
        if word_count > 200 and link_count < word_count / 5:
            return True

    # Check title patterns suggesting listing
    title_el = soup.find("title")
    if title_el:
        title_text = title_el.get_text(strip=True)
        if any(kw in title_text.lower() for kw in [
            "archive", "all posts", "page 2", "category:",
        ]):
            return False

    return False


def extract_title(soup):
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
    title = soup.find("title")
    if title:
        text = title.get_text(strip=True)
        # Strip " | Arab News" suffix
        text = re.sub(r"\s*\|\s*Arab News.*$", "", text)
        return text

    return "Untitled"


def extract_brief(soup):
    """Extract short description / subtitle."""
    og = soup.find("meta", property="og:description")
    if og and og.get("content"):
        return og["content"].strip()

    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return meta["content"].strip()

    return ""


# ---------------------------------------------------------------------------
# Page scraper
# ---------------------------------------------------------------------------
_slug_counter = {}
_slug_lock = Lock()
_content_hashes = {}
_content_hash_lock = Lock()


def _unique_slug(slug):
    """Ensure slug uniqueness by appending -2, -3, etc."""
    with _slug_lock:
        if slug not in _slug_counter:
            _slug_counter[slug] = 0
            return slug
        _slug_counter[slug] += 1
        return f"{slug}-{_slug_counter[slug] + 1}"


def scrape_single(entry, state, force=False):
    """Scrape a single sitemap entry. Returns (slug, status_msg) or None."""
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq")

    # URL-level skip
    if is_listing_page_url(url):
        return None

    slug = generate_slug(url)

    # Incremental check
    if not force:
        stored = state.get(slug)
        if stored:
            stored_lastmod = stored.get("lastmod")
            slug_dir = BASE_DIR / slug
            if (
                stored_lastmod
                and lastmod
                and stored_lastmod == lastmod
                and (slug_dir / "content.md").exists()
            ):
                return (slug, f"[skip] {slug} -- unchanged")

    # Fetch page
    try:
        resp = throttled_get(url)
        if resp.status_code != 200:
            return (slug, f"[error] {slug} -- HTTP {resp.status_code}")
    except Exception as exc:
        return (slug, f"[error] {slug} -- {exc}")

    soup = BeautifulSoup(resp.text, "lxml")

    # Article vs listing detection
    if not is_article_page(soup, url):
        return (slug, f"[skip] {slug} -- listing page")

    # Remove noise
    remove_noise(soup)

    # Content
    container = find_content_container(soup)
    if not container:
        return (slug, f"[skip] {slug} -- no content container")

    md = html_to_markdown(container, slug)
    md = _clean_markdown(md)

    # Content deduplication
    content_hash = hashlib.md5(md.encode()).hexdigest()
    with _content_hash_lock:
        if content_hash in _content_hashes:
            original = _content_hashes[content_hash]
            return (slug, f"[dedup] {slug} -- duplicate of {original}")
        _content_hashes[content_hash] = slug

    # Check word count
    word_count = len(md.split())
    if word_count < 30:
        return (slug, f"[skip] {slug} -- too short ({word_count} words)")

    # Ensure unique slug
    slug = _unique_slug(slug)

    # Extract metadata
    title = extract_title(soup)
    brief = extract_brief(soup)
    publish_date = extract_publish_date(soup, entry_lastmod=lastmod)
    tags = extract_tags(soup)
    content_type, category = detect_content_type(urlparse(url).path)

    # Paywall / truncation detection
    truncated = False
    paywall_hints = soup.select(
        "[class*='paywall'], [class*='subscribe'], [class*='premium-content']"
    )
    if paywall_hints and word_count < 100:
        truncated = True

    # Write files
    slug_dir = BASE_DIR / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    # meta.yaml
    meta = {
        "title": title,
        "publish-date": publish_date,
        "change-frequency": changefreq or "unknown",
        "short-brief": brief,
        "source-url": url,
        "content-type": content_type,
        "category": category,
        "tags": tags,
    }
    if truncated:
        meta["truncated"] = True

    with open(slug_dir / "meta.yaml", "w", encoding="utf-8") as fh:
        yaml.dump(
            meta, fh,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

    # content.md
    with open(slug_dir / "content.md", "w", encoding="utf-8") as fh:
        fh.write(md)

    # Update state
    state[slug] = {
        "lastmod": lastmod,
        "content_hash": content_hash,
    }

    return (slug, f"[ok] {slug} -- {word_count} words")


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------
def replace_internal_links(base_dir, domain):
    """Replace internal links in all content.md files with relative paths."""
    known_slugs = set()
    for d in base_dir.iterdir():
        if d.is_dir() and (d / "content.md").exists() and d.name != "images":
            known_slugs.add(d.name)

    if not known_slugs:
        return 0

    domain_pattern = re.compile(
        r"\[([^\]]+)\]\(https?://" + re.escape(domain) + r"/([^)\s#?]+)[^)]*\)"
    )
    count = 0

    for slug_name in known_slugs:
        md_path = base_dir / slug_name / "content.md"
        content = md_path.read_text(encoding="utf-8")
        original = content

        def _replace_link(match):
            nonlocal count
            link_text = match.group(1)
            url_path = match.group(2).rstrip("/")
            target_slug = url_path.split("/")[-1]
            if target_slug in known_slugs:
                count += 1
                return f"[{link_text}](../{target_slug}/content.md)"
            return match.group(0)

        content = domain_pattern.sub(_replace_link, content)
        if content != original:
            md_path.write_text(content, encoding="utf-8")

    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Scrape www.arabnews.com via sitemap"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-fetch all pages ignoring incremental state",
    )
    parser.add_argument(
        "--slug", type=str, default=None,
        help="Fetch only the specified slug",
    )
    args = parser.parse_args()

    # Ensure output dirs
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    state = {} if args.force else load_fetch_state()

    # Fetch sitemap entries
    entries = fetch_all_sitemap_entries()
    if not entries:
        print("[error] No sitemap entries found. Exiting.")
        return

    # Filter for single slug if requested
    if args.slug:
        entries = [
            e for e in entries
            if generate_slug(e["loc"]) == args.slug
        ]
        if not entries:
            print(f"[error] No sitemap entry matches slug '{args.slug}'")
            return
        print(f"[info] Fetching single slug: {args.slug}")

    # Scrape
    stats = {"ok": 0, "skip": 0, "dedup": 0, "error": 0}
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(scrape_single, entry, state, args.force): entry
            for entry in entries
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is None:
                    stats["skip"] += 1
                    continue
                slug, msg = result
                print(msg)
                if msg.startswith("[ok]"):
                    stats["ok"] += 1
                elif msg.startswith("[skip]"):
                    stats["skip"] += 1
                elif msg.startswith("[dedup]"):
                    stats["dedup"] += 1
                elif msg.startswith("[error]"):
                    stats["error"] += 1
            except Exception as exc:
                print(f"[error] Unexpected: {exc}")
                stats["error"] += 1

    # Save state
    save_fetch_state(state)

    # Post-scrape: internal link replacement
    link_count = replace_internal_links(BASE_DIR, DOMAIN)

    # Summary
    print("\n--- Summary ---")
    print(f"  Sitemap entries: {len(entries)}")
    print(f"  Scraped:         {stats['ok']}")
    print(f"  Skipped:         {stats['skip']}")
    print(f"  Duplicates:      {stats['dedup']}")
    print(f"  Errors:          {stats['error']}")
    print(f"  Links replaced:  {link_count}")
    print(f"  Output:          {BASE_DIR}")


if __name__ == "__main__":
    main()
