#!/usr/bin/env python3
"""
Sitemap-based scraper for www.khaleejtimes.com

Fetches articles from sitemap.xml and news_sitemap.xml, extracts metadata
and content, converts HTML to markdown, downloads images, and stores
everything in structured per-slug directories.

Usage:
    python3 scrape_khaleejtimes.py              # incremental run
    python3 scrape_khaleejtimes.py --force      # re-fetch everything
    python3 scrape_khaleejtimes.py --slug X     # fetch only slug X
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse, unquote

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
_MISSING = []
for _mod in ("requests", "bs4", "yaml", "lxml"):
    try:
        __import__(_mod)
    except ImportError:
        _MISSING.append(_mod)

if _MISSING:
    print("Missing dependencies. Install with:")
    print("  pip3 install --user --break-system-packages requests beautifulsoup4 pyyaml lxml")
    sys.exit(1)

import requests
import yaml
from bs4 import BeautifulSoup, NavigableString, Tag

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "www.khaleejtimes.com"
BASE_URL = f"https://{DOMAIN}"
SITEMAP_URLS = [
    f"{BASE_URL}/sitemap.xml",
    f"{BASE_URL}/news_sitemap.xml",
]
BASE_DIR = Path(__file__).resolve().parent / "khaleej_times"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

REQUEST_DELAY = 1.0
MAX_WORKERS = 5
MIN_ARTICLE_WORDS = 200

# Content path patterns recognised as article sections
CONTENT_PATH_PATTERNS = [
    "/news/", "/articles/", "/press-release/", "/blogs/",
    "/insights/", "/market-insights/", "/latest-insights/", "/wealth-insights/",
    "/posts/", "/newsroom/", "/announcements/",
    "/opinion/", "/future/", "/business/", "/lifestyle/",
    "/life-and-living/", "/your-money/", "/awareness/", "/research/",
    "/reports/", "/market/", "/mediacenter/",
    "/publications/", "/spotlight/", "/economy/", "/stock-market/",
    "/forex-news/", "/commodities-news/", "/cryptocurrency-news/", "/world-news/",
    "/economic-indicators/", "/earnings/", "/analysis/", "/topic/",
    "/speeches/", "/review/", "/originals/", "/news-release/",
    # Khaleej Times specific sections
    "/uae/", "/world/", "/gold-forex/", "/sport/", "/sports/",
    "/entertainment/", "/technology/", "/auto/", "/travel/",
    "/food/", "/health/", "/legal/", "/education/",
    "/property/", "/jobs/", "/citytimes/", "/wknd/",
    "/energy/", "/aviation/", "/banking/", "/crime/",
    "/government/", "/transport/", "/weather/", "/courts/",
    "/gcc/", "/mena/", "/americas/", "/europe/", "/asia/",
    "/africa/", "/cricket/", "/football/", "/tennis/",
    "/motorsport/", "/horse-racing/", "/bollywood/", "/hollywood/",
    "/culture/", "/books/", "/uae-today/",
]

# URL patterns to skip (listings, pagination, utility pages)
SKIP_PATTERNS = [
    re.compile(r"/page/\d+/?$"),
    re.compile(r"[?&]page=\d+"),
    re.compile(r"[?&]p=\d+"),
    re.compile(r"/tag/[^/]+/?$"),
    re.compile(r"/category/[^/]+/?$"),
    re.compile(r"/author/[^/]+/?$"),
    re.compile(r"/search/?"),
    re.compile(r"/about/?$"),
    re.compile(r"/contact/?$"),
    re.compile(r"/privacy/?$"),
    re.compile(r"/terms/?$"),
    re.compile(r"/archive/?$"),
    re.compile(r"/advertise/?$"),
    re.compile(r"/sitemap"),
    re.compile(r"/feed/?$"),
    re.compile(r"/rss/?$"),
    re.compile(r"/amp/?$"),
    re.compile(r"\.(pdf|xml|json|rss)$"),
]

# Noise selectors to remove before content extraction
NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup", ".subscription-widget",
    ".comments", ".comment-section", ".author-bio",
    ".cookie-banner", ".breadcrumb", ".pagination",
    ".ad", ".advertisement", "[class*='promo']",
    "[class*='banner']", "[class*='popup']", "[class*='modal']",
    ".story-share", ".share-story", ".article-share",
    ".also-read", ".read-more-articles", ".related-stories",
    ".social-icons", ".follow-us", ".app-download",
    "[class*='newsletter']", "[class*='subscribe']",
    "[class*='widget']", "[class*='advert']",
    "script", "style", "noscript", "svg", "button", "form",
    "iframe", "[role='complementary']",
]

# Khaleej Times content selectors in priority order
CONTENT_SELECTORS = [
    "div.article-body",
    "div.articleBody",
    "div.article-content-body",
    "div.story-body",
    "div.article__body",
    "div.article__content",
    "div.entry-content",
    "div.post-content",
    "div.field--name-body",
    "article .content",
    "article",
    "main",
    "div.content",
    "div#content",
]

# WordPress image proxy hosts
WP_PROXY_HOSTS = {"i0.wp.com", "i1.wp.com", "i2.wp.com", "i3.wp.com"}


# ---------------------------------------------------------------------------
# Session setup
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    return session


SESSION = _make_session()


# ---------------------------------------------------------------------------
# Fetch state persistence
# ---------------------------------------------------------------------------
def load_fetch_state() -> dict:
    if FETCH_STATE_FILE.exists():
        with open(FETCH_STATE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_fetch_state(state: dict) -> None:
    FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = FETCH_STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)
    tmp.replace(FETCH_STATE_FILE)


# ---------------------------------------------------------------------------
# Sitemap fetching
# ---------------------------------------------------------------------------
def fetch_sitemap(url: str) -> list[dict]:
    """Fetch a sitemap URL and return list of entry dicts."""
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[warn] Failed to fetch sitemap {url}: {exc}")
        return []

    soup = BeautifulSoup(resp.content, "lxml-xml")

    # Check if sitemap index
    sitemap_tags = soup.find_all("sitemap")
    if sitemap_tags:
        entries = []
        for sm in sitemap_tags:
            loc = sm.find("loc")
            if loc and loc.text.strip():
                child_url = loc.text.strip()
                print(f"[sitemap-index] Fetching child: {child_url}")
                time.sleep(REQUEST_DELAY)
                entries.extend(fetch_sitemap(child_url))
        return entries

    # Regular sitemap — extract <url> entries
    entries = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        if not loc:
            continue
        entry = {"loc": loc.text.strip()}

        lastmod = url_tag.find("lastmod")
        if lastmod and lastmod.text.strip():
            entry["lastmod"] = lastmod.text.strip()

        changefreq = url_tag.find("changefreq")
        if changefreq and changefreq.text.strip():
            entry["changefreq"] = changefreq.text.strip()

        # news:title
        news_title = url_tag.find("news:title")
        if news_title and news_title.text.strip():
            entry["news_title"] = news_title.text.strip()

        # news:publication_date
        pub_date = url_tag.find("news:publication_date")
        if pub_date and pub_date.text.strip():
            entry["news_publication_date"] = pub_date.text.strip()

        # news:keywords
        keywords = url_tag.find("news:keywords")
        if keywords and keywords.text.strip():
            entry["news_keywords"] = keywords.text.strip()

        # image:loc
        image_locs = url_tag.find_all("image:loc")
        if image_locs:
            entry["image_locs"] = [il.text.strip() for il in image_locs if il.text.strip()]

        entries.append(entry)

    return entries


def merge_sitemap_entries(entries: list[dict]) -> dict:
    """Deduplicate by URL, preferring the entry with more fields."""
    merged = {}
    for entry in entries:
        url = entry["loc"]
        if url not in merged:
            merged[url] = entry
        else:
            existing = merged[url]
            # Keep the one with more metadata
            if len(entry) > len(existing):
                existing.update(entry)
            else:
                entry_copy = dict(entry)
                entry_copy.update(existing)
                merged[url] = entry_copy
    return merged


# ---------------------------------------------------------------------------
# URL analysis helpers
# ---------------------------------------------------------------------------
def should_skip_url(url: str) -> bool:
    """Return True if URL looks like a listing/utility page."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    for pattern in SKIP_PATTERNS:
        if pattern.search(url):
            return True

    # Skip bare domain
    if not path or path == "/":
        return True

    # Skip if path is just a section with no slug
    # e.g. /business/ or /uae/ with no further segment
    segments = [s for s in path.split("/") if s]
    if len(segments) <= 1:
        # Single segment like /business — likely a listing
        # Unless it looks like a specific slug
        if segments and any(segments[0] == p.strip("/") for p in CONTENT_PATH_PATTERNS):
            return True

    return False


def detect_content_type(url_path: str) -> tuple:
    """
    Returns (content_type, category) from URL path.

    /business/economy/slug  -> ('economy', 'business')
    /uae/crime/slug         -> ('crime', 'uae')
    /world/slug             -> ('world', None)
    /sport/cricket/slug     -> ('cricket', 'sport')
    """
    segments = [s for s in url_path.strip("/").split("/") if s]

    if not segments:
        return ("general", None)

    # Filter out date segments and the slug itself
    date_pattern = re.compile(r"^\d{4}$|^\d{2}$|^\d{1,2}$")
    non_date = [s for s in segments if not date_pattern.match(s)]

    if not non_date:
        return ("general", None)

    # Last segment is the slug, preceding ones are type/category
    if len(non_date) == 1:
        return (non_date[0], None)
    elif len(non_date) == 2:
        return (non_date[0], None)
    else:
        # e.g. /business/economy/slug -> type=economy, cat=business
        return (non_date[-2], non_date[0] if non_date[0] != non_date[-2] else None)


def generate_slug(url_path: str) -> str:
    """Extract the last meaningful path segment as slug."""
    segments = [s for s in url_path.strip("/").split("/") if s]

    if not segments:
        return "index"

    # Take last segment
    slug = segments[-1]

    # Strip common file extensions
    slug = re.sub(r"\.(html?|php|aspx?)$", "", slug, flags=re.IGNORECASE)

    # Normalise
    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9-]", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    slug = slug.strip("-")

    return slug or "index"


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------
def parse_date_string(date_str: str) -> str | None:
    """Attempt to parse a date string into YYYY-MM-DD format."""
    if not date_str:
        return None

    date_str = date_str.strip()

    # ISO 8601 variants
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(date_str[:26].replace("Z", "+00:00").rstrip("+00:00") if "Z" in date_str else date_str[:26], fmt).strftime("%Y-%m-%d")
        except (ValueError, IndexError):
            continue

    # Try common written formats
    for fmt in (
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
    ):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Regex fallback for YYYY-MM-DD anywhere in string
    m = re.search(r"(\d{4}-\d{2}-\d{2})", date_str)
    if m:
        return m.group(1)

    return None


def extract_publish_date(soup: BeautifulSoup, url: str, sitemap_entry: dict) -> str | None:
    """Extract publish date using priority chain."""

    # 1. article:published_time
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        d = parse_date_string(meta["content"])
        if d:
            return d

    # 2. meta name=date / publish-date
    for name in ("date", "publish-date", "publishdate", "publication_date"):
        meta = soup.find("meta", attrs={"name": name})
        if meta and meta.get("content"):
            d = parse_date_string(meta["content"])
            if d:
                return d

    # 3. <time datetime>
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        d = parse_date_string(time_el["datetime"])
        if d:
            return d

    # 4. Date-classed elements
    for selector in [
        "[class*='date']", "[class*='timestamp']", "[class*='publish']",
        "[class*='Date']", "[class*='time']",
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
                ld = ld[0] if ld else {}
            if isinstance(ld, dict):
                for key in ("datePublished", "dateCreated"):
                    if ld.get(key):
                        d = parse_date_string(ld[key])
                        if d:
                            return d
        except (json.JSONDecodeError, TypeError, IndexError):
            continue

    # 6. URL path date segments
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"/(\d{4})-(\d{2})-(\d{2})/", url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # 7. Sitemap lastmod
    lastmod = sitemap_entry.get("lastmod") or sitemap_entry.get("news_publication_date")
    if lastmod:
        d = parse_date_string(lastmod)
        if d:
            return d

    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------
def extract_tags(soup: BeautifulSoup, sitemap_entry: dict) -> list[str]:
    """Extract and deduplicate tags from multiple sources."""
    tags = set()

    # 1. meta keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        for kw in meta_kw["content"].split(","):
            t = kw.strip().lower()
            if t and len(t) < 60:
                tags.add(t)

    # 2. article:tag meta (multiple)
    for meta in soup.find_all("meta", property="article:tag"):
        if meta.get("content"):
            t = meta["content"].strip().lower()
            if t:
                tags.add(t)

    # 3. JSON-LD keywords
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script_tag.string or "")
            if isinstance(ld, list):
                ld = ld[0] if ld else {}
            if isinstance(ld, dict):
                kw = ld.get("keywords")
                if isinstance(kw, str):
                    for k in kw.split(","):
                        t = k.strip().lower()
                        if t:
                            tags.add(t)
                elif isinstance(kw, list):
                    for k in kw:
                        if isinstance(k, str):
                            tags.add(k.strip().lower())
        except (json.JSONDecodeError, TypeError):
            continue

    # 4. Visible tag links
    for sel in ('a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a",
                "[class*='tag-link']", ".story-tags a", ".article__tags a"):
        for a in soup.select(sel):
            t = a.get_text(strip=True).lower()
            if t and len(t) < 60:
                tags.add(t)

    # 5. News sitemap keywords
    kw_str = sitemap_entry.get("news_keywords", "")
    if kw_str:
        for k in kw_str.split(","):
            t = k.strip().lower()
            if t:
                tags.add(t)

    # Clean up
    tags = {t for t in tags if t and t not in ("", "news", "article")}
    return sorted(tags)


# ---------------------------------------------------------------------------
# Image handling
# ---------------------------------------------------------------------------
def resolve_image_url(src: str, page_url: str) -> str:
    """Resolve relative image URLs and normalise."""
    if not src:
        return ""
    src = src.strip()
    if src.startswith("data:"):
        return ""
    if src.startswith("//"):
        src = "https:" + src
    elif not src.startswith("http"):
        src = urljoin(page_url, src)
    return src


def derive_image_filename(src: str, slug: str) -> str:
    """Derive a stable filename from the image URL."""
    parsed = urlparse(src)
    host = parsed.hostname or ""

    # WordPress proxy handling
    path = parsed.path
    if host in WP_PROXY_HOSTS:
        # Path is like /example.com/wp-content/uploads/image.jpg
        # Strip the domain prefix to get the real path
        parts = path.split("/", 2)
        if len(parts) > 2:
            path = "/" + parts[2]

    # Get base name and extension
    basename = os.path.basename(unquote(path))
    _, ext = os.path.splitext(basename)
    if not ext or len(ext) > 6:
        ext = ".jpg"

    # Clean extension — strip query params that may have leaked
    ext = ext.split("?")[0].split("&")[0]

    # Hash for uniqueness
    url_hash = hashlib.md5(src.encode()).hexdigest()[:10]

    clean_slug = re.sub(r"[^a-z0-9-]", "", slug[:30])
    return f"{clean_slug}_{url_hash}{ext}"


def download_image(src: str, slug: str) -> str | None:
    """Download image and return the local filename, or None on failure."""
    if not src:
        return None

    filename = derive_image_filename(src, slug)
    filepath = IMAGES_DIR / filename

    if filepath.exists():
        return filename

    try:
        resp = SESSION.get(src, timeout=20, stream=True)
        resp.raise_for_status()
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb") as fh:
            for chunk in resp.iter_content(8192):
                fh.write(chunk)
        return filename
    except requests.RequestException:
        return None


# ---------------------------------------------------------------------------
# HTML -> Markdown converter
# ---------------------------------------------------------------------------
def html_to_markdown(element: Tag, page_url: str, slug: str) -> str:
    """Recursively convert a BeautifulSoup element tree to markdown."""
    parts = []

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

        tag = child.name.lower() if child.name else ""

        # Skip noise tags
        if tag in ("script", "style", "noscript", "svg", "button", "form",
                   "iframe", "nav", "aside", "input", "select", "textarea"):
            continue

        # Headings
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag[1])
            text = child.get_text(strip=True)
            if text:
                parts.append(f"\n\n{'#' * level} {text}\n\n")
            continue

        # Paragraph
        if tag == "p":
            inner = html_to_markdown(child, page_url, slug).strip()
            if inner:
                parts.append(f"\n\n{inner}\n\n")
            continue

        # Bold
        if tag in ("strong", "b"):
            text = child.get_text(strip=True)
            if text:
                parts.append(f"**{text}**")
            continue

        # Italic
        if tag in ("em", "i"):
            text = child.get_text(strip=True)
            if text:
                parts.append(f"*{text}*")
            continue

        # Links
        if tag == "a":
            href = child.get("href", "")
            # Check if link wraps an image
            inner_img = child.find("img")
            if inner_img:
                parts.append(html_to_markdown(child, page_url, slug))
                continue
            text = child.get_text(strip=True)
            if text and href:
                href = resolve_image_url(href, page_url)  # resolve relative
                parts.append(f"[{text}]({href})")
            elif text:
                parts.append(text)
            continue

        # Images
        if tag == "img":
            src = child.get("src") or child.get("data-src") or child.get("data-lazy-src") or ""
            alt = child.get("alt", "").strip()
            src = resolve_image_url(src, page_url)
            if src:
                img_file = download_image(src, slug)
                if img_file:
                    parts.append(f"\n\n![{alt}](../images/{img_file})\n\n")
            continue

        # Picture — find inner img or first source
        if tag == "picture":
            inner_img = child.find("img")
            if inner_img:
                src = inner_img.get("src") or inner_img.get("data-src") or ""
                alt = inner_img.get("alt", "").strip()
                src = resolve_image_url(src, page_url)
                if src:
                    img_file = download_image(src, slug)
                    if img_file:
                        parts.append(f"\n\n![{alt}](../images/{img_file})\n\n")
            continue

        # Figure
        if tag == "figure":
            inner = html_to_markdown(child, page_url, slug).strip()
            if inner:
                parts.append(f"\n\n{inner}")
            caption = child.find("figcaption")
            if caption:
                cap_text = caption.get_text(strip=True)
                if cap_text:
                    parts.append(f"\n*{cap_text}*")
            parts.append("\n\n")
            continue

        # Blockquote
        if tag == "blockquote":
            inner = html_to_markdown(child, page_url, slug).strip()
            if inner:
                quoted = "\n".join(f"> {line}" for line in inner.split("\n"))
                parts.append(f"\n\n{quoted}\n\n")
            continue

        # Code blocks
        if tag == "pre":
            code = child.find("code")
            text = (code or child).get_text()
            lang_class = ""
            if code and code.get("class"):
                for cls in code["class"]:
                    if cls.startswith("language-"):
                        lang_class = cls.replace("language-", "")
                        break
            parts.append(f"\n\n```{lang_class}\n{text}\n```\n\n")
            continue

        if tag == "code":
            text = child.get_text()
            parts.append(f"`{text}`")
            continue

        # Lists
        if tag in ("ul", "ol"):
            items = child.find_all("li", recursive=False)
            parts.append("\n\n")
            for idx, li in enumerate(items, 1):
                inner = html_to_markdown(li, page_url, slug).strip()
                prefix = f"{idx}. " if tag == "ol" else "- "
                parts.append(f"{prefix}{inner}\n")
            parts.append("\n")
            continue

        # Line break
        if tag == "br":
            parts.append("\n")
            continue

        # Horizontal rule
        if tag == "hr":
            parts.append("\n\n---\n\n")
            continue

        # Table
        if tag == "table":
            rows = child.find_all("tr")
            if rows:
                table_md = _convert_table(rows)
                if table_md:
                    parts.append(f"\n\n{table_md}\n\n")
            continue

        # Div / section / span — recurse
        if tag in ("div", "section", "span", "main", "article", "header",
                   "li", "dd", "dt", "dl", "small", "mark", "ins", "del",
                   "sub", "sup", "abbr", "cite", "time", "address"):
            inner = html_to_markdown(child, page_url, slug)
            parts.append(inner)
            continue

    result = "".join(parts)
    # Collapse excessive newlines
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


def _convert_table(rows: list) -> str:
    """Convert HTML table rows to markdown table."""
    if not rows:
        return ""

    table_data = []
    for row in rows:
        cells = row.find_all(["th", "td"])
        table_data.append([c.get_text(strip=True) for c in cells])

    if not table_data:
        return ""

    col_count = max(len(r) for r in table_data)
    # Pad rows
    for r in table_data:
        while len(r) < col_count:
            r.append("")

    lines = []
    # Header
    lines.append("| " + " | ".join(table_data[0]) + " |")
    lines.append("| " + " | ".join(["---"] * col_count) + " |")
    for row in table_data[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


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


def strip_noise(soup: BeautifulSoup) -> None:
    """Remove noisy elements from the soup in-place."""
    for selector in NOISE_SELECTORS:
        try:
            for el in soup.select(selector):
                el.decompose()
        except Exception:
            continue


def is_article_page(soup: BeautifulSoup, content_container: Tag | None, url: str) -> bool:
    """Determine if page is an article vs a listing."""
    # Check og:type
    og_type = soup.find("meta", property="og:type")
    if og_type and og_type.get("content", "").lower() == "article":
        return True

    # Check for article:published_time
    if soup.find("meta", property="article:published_time"):
        return True

    # Check JSON-LD for article type
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script_tag.string or "")
            if isinstance(ld, list):
                ld = ld[0] if ld else {}
            if isinstance(ld, dict):
                ld_type = ld.get("@type", "")
                if isinstance(ld_type, str) and "article" in ld_type.lower():
                    return True
                if isinstance(ld_type, list) and any("article" in t.lower() for t in ld_type if isinstance(t, str)):
                    return True
        except (json.JSONDecodeError, TypeError):
            continue

    # Word count check
    if content_container:
        text = content_container.get_text(separator=" ", strip=True)
        word_count = len(text.split())
        if word_count >= MIN_ARTICLE_WORDS:
            return True

    # Title check for listing indicators
    title_el = soup.find("title")
    if title_el:
        title_text = title_el.get_text(strip=True).lower()
        listing_words = ["archive", "all posts", "category:", "page 2", "page 3", "tag:"]
        if any(w in title_text for w in listing_words):
            return False

    return False


def extract_title(soup: BeautifulSoup, sitemap_entry: dict) -> str:
    """Extract article title with fallbacks."""
    # H1
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text and len(text) > 5:
            return text

    # og:title
    og = soup.find("meta", property="og:title")
    if og and og.get("content", "").strip():
        return og["content"].strip()

    # <title>
    title_el = soup.find("title")
    if title_el:
        text = title_el.get_text(strip=True)
        # Often "Title - Khaleej Times"
        text = re.sub(r"\s*[-|]\s*Khaleej\s*Times.*$", "", text, flags=re.IGNORECASE)
        if text:
            return text

    # Sitemap news:title
    if sitemap_entry.get("news_title"):
        return sitemap_entry["news_title"]

    return "Untitled"


def extract_brief(soup: BeautifulSoup) -> str:
    """Extract short description / subtitle."""
    # og:description
    og = soup.find("meta", property="og:description")
    if og and og.get("content", "").strip():
        return og["content"].strip()

    # meta description
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content", "").strip():
        return meta["content"].strip()

    # twitter:description
    tw = soup.find("meta", attrs={"name": "twitter:description"})
    if tw and tw.get("content", "").strip():
        return tw["content"].strip()

    return ""


# ---------------------------------------------------------------------------
# Article processing
# ---------------------------------------------------------------------------
def process_article(url: str, sitemap_entry: dict, force: bool, state: dict,
                    content_hashes: dict) -> dict | None:
    """
    Fetch and process a single article.
    Returns a result dict or None on skip/error.
    """
    slug = generate_slug(urlparse(url).path)
    slug_dir = BASE_DIR / slug
    content_file = slug_dir / "content.md"
    meta_file = slug_dir / "meta.yaml"

    lastmod = sitemap_entry.get("lastmod", "")

    # Incremental skip check
    if not force:
        stored_lastmod = state.get(slug, {}).get("lastmod", "")
        if stored_lastmod and stored_lastmod == lastmod and content_file.exists():
            return {"status": "skip", "slug": slug, "reason": "unchanged"}

    # Fetch page
    try:
        time.sleep(REQUEST_DELAY)
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        return {"status": "error", "slug": slug, "reason": str(exc)}

    soup = BeautifulSoup(resp.content, "lxml")
    strip_noise(soup)

    content_container = find_content_container(soup)

    # Article vs listing check
    if not is_article_page(soup, content_container, url):
        return {"status": "skip", "slug": slug, "reason": "listing page"}

    if not content_container:
        return {"status": "skip", "slug": slug, "reason": "no content container"}

    # Convert to markdown
    markdown = html_to_markdown(content_container, url, slug).strip()

    # Word count check
    word_count = len(re.sub(r"[#*\[\]()!>|`\-]", " ", markdown).split())
    if word_count < MIN_ARTICLE_WORDS:
        # Check for paywall/truncation
        paywall_indicators = [
            "subscribe to continue", "sign in to read", "premium content",
            "subscribe now", "login to continue", "register to read",
        ]
        page_text = soup.get_text(separator=" ", strip=True).lower()
        truncated = any(ind in page_text for ind in paywall_indicators)
        if not truncated and word_count < 50:
            return {"status": "skip", "slug": slug, "reason": f"too short ({word_count} words)"}

    # Content dedup
    content_hash = hashlib.md5(markdown.encode("utf-8")).hexdigest()
    if content_hash in content_hashes:
        original_slug = content_hashes[content_hash]
        return {"status": "dedup", "slug": slug, "reason": f"duplicate of {original_slug}"}
    content_hashes[content_hash] = slug

    # Extract metadata
    title = extract_title(soup, sitemap_entry)
    publish_date = extract_publish_date(soup, url, sitemap_entry)
    brief = extract_brief(soup)
    tags = extract_tags(soup, sitemap_entry)
    url_path = urlparse(url).path
    content_type, category = detect_content_type(url_path)
    changefreq = sitemap_entry.get("changefreq", "")

    # Check truncation
    paywall_indicators = [
        "subscribe to continue", "sign in to read", "premium content",
        "subscribe now", "login to continue",
    ]
    page_text = soup.get_text(separator=" ", strip=True).lower()
    truncated = any(ind in page_text for ind in paywall_indicators) and word_count < 150

    # Build meta
    meta = {
        "title": title,
        "publish-date": publish_date or "",
        "change-frequency": changefreq or "daily",
        "short-brief": brief,
        "source-url": url,
        "content-type": content_type,
    }
    if category:
        meta["category"] = category
    if tags:
        meta["tags"] = tags
    if truncated:
        meta["truncated"] = True

    # Write files
    slug_dir.mkdir(parents=True, exist_ok=True)

    with open(meta_file, "w", encoding="utf-8") as fh:
        yaml.dump(meta, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)

    with open(content_file, "w", encoding="utf-8") as fh:
        fh.write(markdown)

    # Update state
    state[slug] = {
        "lastmod": lastmod,
        "content_hash": content_hash,
        "fetched_at": datetime.utcnow().isoformat(),
    }

    return {"status": "ok", "slug": slug, "words": word_count}


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------
def replace_internal_links(base_dir: Path) -> int:
    """Replace internal Khaleej Times links with local relative paths."""
    replaced_count = 0
    slug_dirs = {d.name for d in base_dir.iterdir() if d.is_dir() and d.name != "images"}

    link_pattern = re.compile(
        r'\[([^\]]*)\]\(https?://(?:www\.)?khaleejtimes\.com/([^)]*)\)'
    )

    for slug_name in slug_dirs:
        content_file = base_dir / slug_name / "content.md"
        if not content_file.exists():
            continue

        text = content_file.read_text(encoding="utf-8")
        original = text

        def _replace_link(m):
            nonlocal replaced_count
            link_text = m.group(1)
            path_and_params = m.group(2)
            # Strip query params and fragments
            clean_path = path_and_params.split("?")[0].split("#")[0].rstrip("/")
            target_slug = clean_path.split("/")[-1] if clean_path else ""
            target_slug = re.sub(r"\.(html?|php)$", "", target_slug, flags=re.IGNORECASE)

            if target_slug in slug_dirs:
                replaced_count += 1
                return f"[{link_text}](../{target_slug}/content.md)"
            return m.group(0)

        text = link_pattern.sub(_replace_link, text)

        if text != original:
            content_file.write_text(text, encoding="utf-8")

    return replaced_count


# ---------------------------------------------------------------------------
# Slug collision handling
# ---------------------------------------------------------------------------
def resolve_slug_collisions(entries: list[tuple[str, dict]]) -> list[tuple[str, dict, str]]:
    """
    Given (url, sitemap_entry) pairs, compute unique slugs.
    Returns (url, sitemap_entry, unique_slug) triples.
    """
    slug_counts: dict[str, int] = {}
    result = []

    for url, entry in entries:
        slug = generate_slug(urlparse(url).path)
        count = slug_counts.get(slug, 0)
        slug_counts[slug] = count + 1
        unique_slug = slug if count == 0 else f"{slug}-{count + 1}"
        result.append((url, entry, unique_slug))

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Scrape Khaleej Times via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything ignoring state")
    parser.add_argument("--slug", type=str, default=None, help="Fetch only this slug")
    args = parser.parse_args()

    print(f"=== Khaleej Times Scraper ===")
    print(f"Output: {BASE_DIR}")

    # Create directories
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    state = {} if args.force else load_fetch_state()
    content_hashes: dict[str, str] = {}

    # Rebuild content hash map from state
    for slug_key, slug_state in state.items():
        ch = slug_state.get("content_hash")
        if ch:
            content_hashes[ch] = slug_key

    # Fetch sitemaps
    print("\n[1/4] Fetching sitemaps...")
    all_entries = []
    for sitemap_url in SITEMAP_URLS:
        print(f"  Fetching: {sitemap_url}")
        entries = fetch_sitemap(sitemap_url)
        print(f"  Found {len(entries)} entries")
        all_entries.extend(entries)

    # Merge and deduplicate
    merged = merge_sitemap_entries(all_entries)
    print(f"\n  Total unique URLs after merge: {len(merged)}")

    # Filter to content URLs
    filtered = []
    for url, entry in merged.items():
        if should_skip_url(url):
            continue
        filtered.append((url, entry))

    print(f"  Content URLs after filtering: {len(filtered)}")

    # Handle --slug filter
    if args.slug:
        filtered = [
            (url, entry) for url, entry in filtered
            if generate_slug(urlparse(url).path) == args.slug
        ]
        if not filtered:
            print(f"\n[error] No URL found matching slug '{args.slug}'")
            sys.exit(1)
        print(f"  Filtered to slug '{args.slug}': {len(filtered)} URL(s)")

    if not filtered:
        print("\nNo URLs to process.")
        return

    # Process articles
    print(f"\n[2/4] Processing {len(filtered)} articles...")
    stats = {"ok": 0, "skip": 0, "error": 0, "dedup": 0}

    def _process(item):
        url, entry = item
        return process_article(url, entry, args.force, state, content_hashes)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_process, item): item for item in filtered}
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                stats["error"] += 1
                continue

            status = result["status"]
            stats[status] = stats.get(status, 0) + 1
            slug = result.get("slug", "?")

            if status == "ok":
                words = result.get("words", 0)
                print(f"  [ok]   {slug} ({words} words)")
            elif status == "skip":
                reason = result.get("reason", "")
                print(f"  [skip] {slug} — {reason}")
            elif status == "dedup":
                reason = result.get("reason", "")
                print(f"  [dedup] {slug} — {reason}")
            elif status == "error":
                reason = result.get("reason", "")
                print(f"  [error] {slug} — {reason}")

    # Save state
    print("\n[3/4] Saving state...")
    save_fetch_state(state)

    # Internal link replacement
    print("\n[4/4] Replacing internal links...")
    replaced = replace_internal_links(BASE_DIR)
    print(f"  Replaced {replaced} internal links")

    # Summary
    print(f"\n=== Summary ===")
    print(f"  Fetched:    {stats['ok']}")
    print(f"  Skipped:    {stats['skip']}")
    print(f"  Duplicates: {stats['dedup']}")
    print(f"  Errors:     {stats['error']}")
    print(f"  State file: {FETCH_STATE_FILE}")
    print(f"  Output dir: {BASE_DIR}")


if __name__ == "__main__":
    main()
