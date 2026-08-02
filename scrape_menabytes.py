#!/usr/bin/env python3
"""
Sitemap-based scraper for www.menabytes.com (WordPress / Yoast SEO).

Usage:
    python3 scrape_menabytes.py              # incremental run
    python3 scrape_menabytes.py --force      # re-fetch everything
    python3 scrape_menabytes.py --slug X     # fetch only slug X
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
_MISSING = []
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
        "Missing dependencies. Install with:\n"
        f"  pip3 install --user --break-system-packages {' '.join(_MISSING)}"
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "www.menabytes.com"
SITE_URL = f"https://{DOMAIN}"
SITEMAP_INDEX_URL = f"{SITE_URL}/sitemap_index.xml"
POST_SITEMAP_URL = f"{SITE_URL}/post-sitemap.xml"

SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR / "menabytes"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

MAX_WORKERS = 5
REQUEST_DELAY = 1.0
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

# WordPress content selectors (tried in order)
CONTENT_SELECTORS = [
    "div.entry-content",
    "article .post-content",
    "div.td-post-content",
    "div.single-content",
    "article",
    "main",
    "div.content",
    "div#content",
]

# URL path segments that indicate content pages
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

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup", ".subscription-widget",
    ".comments", ".comment-section", ".author-bio", ".disclaimer", ".cookie-banner",
    ".breadcrumb", ".pagination", ".ad", ".advertisement",
    "[class*='promo']", "[class*='banner']", "[class*='popup']", "[class*='modal']",
    ".sharedaddy", ".jp-relatedposts", ".post-navigation", ".yarpp-related",
    ".wp-block-embed", ".addtoany_share_save_container",
]

SKIP_TAGS = {"script", "style", "noscript", "svg", "button", "iframe", "form"}

SKIP_URL_PATTERNS = [
    re.compile(r"/page/\d+/?$"),
    re.compile(r"[?&]page=\d+"),
    re.compile(r"[?&]p=\d+"),
    re.compile(r"/tag/[^/]+/?$"),
    re.compile(r"/category/[^/]+/?$"),
    re.compile(r"/author/[^/]+/?$"),
    re.compile(r"/archive/?$"),
    re.compile(r"/search/?$"),
    re.compile(r"/about/?$"),
    re.compile(r"/contact/?$"),
    re.compile(r"/feed/?$"),
    re.compile(r"/wp-json/"),
    re.compile(r"/wp-admin/"),
    re.compile(r"/wp-login"),
]

WP_IMAGE_PROXY_RE = re.compile(r"^https?://i[0-3]\.wp\.com/(.+)$")

DATE_SEGMENT_RE = re.compile(r"/(\d{4})/(\d{2})(?:/(\d{2}))?/")

# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------
_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT})
_last_request_time = 0.0


def _throttled_get(url: str, **kwargs) -> requests.Response:
    """GET with per-request delay."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    resp = _session.get(url, timeout=30, **kwargs)
    _last_request_time = time.time()
    return resp


# ---------------------------------------------------------------------------
# Sitemap parsing
# ---------------------------------------------------------------------------
def fetch_sitemap_urls() -> list[dict]:
    """Return list of {loc, lastmod, changefreq} from sitemap(s)."""
    entries: list[dict] = []

    # Try sitemap index first, fall back to direct post-sitemap
    index_xml = _try_fetch_xml(SITEMAP_INDEX_URL)
    sitemap_urls: list[str] = []

    if index_xml is not None:
        ns = _detect_ns(index_xml)
        for sm in index_xml.findall(f"{ns}sitemap"):
            loc_el = sm.find(f"{ns}loc")
            if loc_el is not None and loc_el.text:
                sitemap_urls.append(loc_el.text.strip())
    else:
        sitemap_urls.append(POST_SITEMAP_URL)

    # If the index had no post-sitemap, add it explicitly
    if not any("post-sitemap" in u for u in sitemap_urls):
        sitemap_urls.append(POST_SITEMAP_URL)

    for sm_url in sitemap_urls:
        # Only parse post sitemaps; skip page/category/author sitemaps
        basename = sm_url.rsplit("/", 1)[-1].lower()
        if any(
            skip in basename
            for skip in ("category", "author", "tag", "page-sitemap")
        ):
            continue

        root = _try_fetch_xml(sm_url)
        if root is None:
            continue

        ns = _detect_ns(root)
        for url_el in root.findall(f"{ns}url"):
            loc = _text(url_el, f"{ns}loc")
            if not loc:
                continue
            lastmod = _text(url_el, f"{ns}lastmod") or ""
            changefreq = _text(url_el, f"{ns}changefreq") or ""
            entries.append(
                {"loc": loc.strip(), "lastmod": lastmod.strip(), "changefreq": changefreq.strip()}
            )

    return entries


def _try_fetch_xml(url: str) -> ET.Element | None:
    try:
        resp = _throttled_get(url)
        resp.raise_for_status()
        return ET.fromstring(resp.content)
    except Exception as exc:
        print(f"[warn] Could not fetch {url}: {exc}")
        return None


def _detect_ns(root: ET.Element) -> str:
    tag = root.tag
    if tag.startswith("{"):
        return tag.split("}")[0] + "}"
    return ""


def _text(parent: ET.Element, tag: str) -> str | None:
    el = parent.find(tag)
    return el.text if el is not None else None


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------
def should_skip_url(url: str) -> bool:
    """Return True if this URL looks like a listing/non-article page."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Skip known non-article patterns
    for pat in SKIP_URL_PATTERNS:
        if pat.search(url):
            return True

    # Homepage
    if path in ("", "/"):
        return True

    # Bare content-type paths (listing pages like /news/ with nothing after)
    for cp in CONTENT_PATH_PATTERNS:
        cp_stripped = cp.rstrip("/")
        if path == cp_stripped or path == cp_stripped + "/":
            return True

    return False


def detect_content_type(url_path: str) -> tuple[str, str | None]:
    """Return (content_type, category) based on URL path segments."""
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    # Remove date segments
    segments = [s for s in segments if not re.match(r"^\d{4}$", s) and not re.match(r"^\d{2}$", s)]

    if not segments:
        return ("article", None)

    # Try to match against known content patterns
    best_match_depth = -1
    best_type = None
    best_category = None

    for pattern in CONTENT_PATH_PATTERNS:
        pat_segments = [s for s in pattern.strip("/").split("/") if s]
        if len(pat_segments) == 0:
            continue

        # Check if all pattern segments appear in URL segments in order
        match_idx = 0
        for seg in segments:
            if match_idx < len(pat_segments) and seg == pat_segments[match_idx]:
                match_idx += 1

        if match_idx == len(pat_segments) and match_idx > best_match_depth:
            best_match_depth = match_idx
            if len(pat_segments) >= 2:
                best_type = pat_segments[-1]
                best_category = pat_segments[0]
            else:
                best_type = pat_segments[0]
                best_category = None

    if best_type:
        return (best_type, best_category)

    # Fallback: use first segment as content-type
    if len(segments) >= 2:
        return (segments[0], None)

    return ("article", None)


def generate_slug(url: str) -> str:
    """Derive a filesystem-safe slug from the URL."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    if not segments:
        return "index"

    # Remove date segments
    cleaned = []
    i = 0
    while i < len(segments):
        if (
            re.match(r"^\d{4}$", segments[i])
            and i + 1 < len(segments)
            and re.match(r"^\d{2}$", segments[i + 1])
        ):
            # Skip year/month (and optional day)
            i += 2
            if i < len(segments) and re.match(r"^\d{2}$", segments[i]):
                i += 1
            continue
        cleaned.append(segments[i])
        i += 1

    if not cleaned:
        cleaned = segments  # fallback to originals if everything was stripped

    # Remove known content-type path segments to get the actual slug
    slug_candidates = list(cleaned)
    for pattern in CONTENT_PATH_PATTERNS:
        pat_segments = [s for s in pattern.strip("/").split("/") if s]
        for ps in pat_segments:
            if slug_candidates and slug_candidates[0] == ps:
                slug_candidates.pop(0)

    # Take last meaningful segment
    slug = slug_candidates[-1] if slug_candidates else cleaned[-1]

    # Normalise
    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")

    return slug or "untitled"


# ---------------------------------------------------------------------------
# State management
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
# Date extraction
# ---------------------------------------------------------------------------
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def extract_date(soup: BeautifulSoup, url: str, lastmod: str) -> str | None:
    """8-step date extraction priority chain."""

    # 1. article:published_time
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        d = _parse_date_str(meta["content"])
        if d:
            return d

    # 2. meta name="date" or name="publish-date"
    for name in ("date", "publish-date", "pubdate", "publishdate"):
        meta = soup.find("meta", attrs={"name": name})
        if meta and meta.get("content"):
            d = _parse_date_str(meta["content"])
            if d:
                return d

    # 3. <time datetime>
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        d = _parse_date_str(time_el["datetime"])
        if d:
            return d

    # 4. Span/div with date class
    for sel in [
        "[class*='date']", "[class*='timestamp']", "[class*='published']",
        "[class*='post-date']", "[class*='entry-date']",
    ]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            d = _parse_date_str(el.get_text(strip=True))
            if d:
                return d

    # 5. JSON-LD datePublished
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            dp = data.get("datePublished") or data.get("dateCreated")
            if dp:
                d = _parse_date_str(dp)
                if d:
                    return d
        except (json.JSONDecodeError, TypeError, IndexError):
            pass

    # 6. URL date segments
    m = DATE_SEGMENT_RE.search(urlparse(url).path)
    if m:
        year, month = m.group(1), m.group(2)
        day = m.group(3) or "01"
        return f"{year}-{month}-{day}"

    # 7. Sitemap lastmod
    if lastmod:
        d = _parse_date_str(lastmod)
        if d:
            return d

    # 8. HTTP Last-Modified is handled at caller level
    return None


def _parse_date_str(s: str) -> str | None:
    """Extract YYYY-MM-DD from various date formats."""
    if not s:
        return None
    s = s.strip()
    m = _DATE_RE.search(s)
    if m:
        return m.group(0)
    # Try common formats
    from datetime import datetime as _dt
    for fmt in (
        "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y",
        "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d",
    ):
        try:
            return _dt.strptime(s.split("T")[0].strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------
def extract_tags(soup: BeautifulSoup) -> list[str]:
    """Extract and deduplicate tags from multiple sources."""
    tags: set[str] = set()

    # 1. meta keywords
    meta = soup.find("meta", attrs={"name": "keywords"})
    if meta and meta.get("content"):
        for t in meta["content"].split(","):
            t = t.strip().lower()
            if t:
                tags.add(t)

    # 2. article:tag (multiple)
    for meta in soup.find_all("meta", property="article:tag"):
        if meta.get("content"):
            tags.add(meta["content"].strip().lower())

    # 3. JSON-LD keywords
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            kw = data.get("keywords")
            if isinstance(kw, list):
                for k in kw:
                    tags.add(str(k).strip().lower())
            elif isinstance(kw, str):
                for k in kw.split(","):
                    k = k.strip().lower()
                    if k:
                        tags.add(k)
        except (json.JSONDecodeError, TypeError, IndexError):
            pass

    # 4. Visible tag links
    for sel in [
        'a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a",
        "[class*='tag-link']", ".cat-links a", ".entry-categories a",
    ]:
        for a in soup.select(sel):
            t = a.get_text(strip=True).lower()
            if t and len(t) < 80:
                tags.add(t)

    return sorted(tags)


# ---------------------------------------------------------------------------
# Image downloading
# ---------------------------------------------------------------------------
def download_image(src: str, slug: str) -> str | None:
    """Download image, return relative path from slug dir (../images/filename) or None."""
    if not src or src.startswith("data:"):
        return None

    # Resolve WordPress proxy URLs
    original_path = src
    wp_match = WP_IMAGE_PROXY_RE.match(src)
    if wp_match:
        original_path = wp_match.group(1)

    # Derive extension from original path (strip query params)
    clean_path = urlparse(original_path).path
    ext = Path(clean_path).suffix.lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp", ".ico"):
        ext = ".jpg"

    # Create filename: slug_hash.ext
    url_hash = hashlib.md5(src.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    filepath = IMAGES_DIR / filename

    if filepath.exists():
        return f"../images/{filename}"

    try:
        resp = _throttled_get(src, stream=True)
        resp.raise_for_status()
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        return f"../images/{filename}"
    except Exception as exc:
        print(f"  [warn] Image download failed {src}: {exc}")
        return None


# ---------------------------------------------------------------------------
# HTML → Markdown converter
# ---------------------------------------------------------------------------
def html_to_markdown(element, slug: str) -> str:
    """Recursively convert a BeautifulSoup element tree to markdown."""
    if element is None:
        return ""
    lines = _convert_node(element, slug)
    md = "".join(lines)
    # Collapse 3+ consecutive newlines to 2
    md = re.sub(r"\n{3,}", "\n\n", md)
    return md.strip() + "\n"


def _convert_node(node, slug: str) -> list[str]:
    """Recursive node converter."""
    if isinstance(node, NavigableString):
        text = str(node)
        # Collapse whitespace within inline text (not pre)
        if not _inside_pre(node):
            text = re.sub(r"[ \t]+", " ", text)
        return [text]

    if not isinstance(node, Tag):
        return []

    tag = node.name.lower() if node.name else ""

    if tag in SKIP_TAGS:
        return []

    # Headings
    if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
        level = int(tag[1])
        inner = _children_text(node, slug).strip()
        if inner:
            return [f"\n\n{'#' * level} {inner}\n\n"]
        return []

    # Paragraph
    if tag == "p":
        inner = _children_text(node, slug).strip()
        if inner:
            return [f"\n\n{inner}\n\n"]
        return []

    # Bold
    if tag in ("strong", "b"):
        inner = _children_text(node, slug).strip()
        if inner:
            return [f"**{inner}**"]
        return []

    # Italic
    if tag in ("em", "i"):
        inner = _children_text(node, slug).strip()
        if inner:
            return [f"*{inner}*"]
        return []

    # Links
    if tag == "a":
        href = node.get("href", "")
        # If link wraps an image, recurse into children
        if node.find("img"):
            return _convert_children(node, slug)
        inner = _children_text(node, slug).strip()
        if inner and href:
            return [f"[{inner}]({href})"]
        if inner:
            return [inner]
        return []

    # Images
    if tag == "img":
        src = node.get("data-src") or node.get("src") or ""
        alt = node.get("alt", "").strip()
        if src:
            local = download_image(src, slug)
            if local:
                return [f"![{alt}]({local})"]
            return [f"![{alt}]({src})"]
        return []

    # Picture
    if tag == "picture":
        img = node.find("img")
        if img:
            return _convert_node(img, slug)
        source = node.find("source")
        if source and source.get("srcset"):
            src = source["srcset"].split(",")[0].strip().split(" ")[0]
            local = download_image(src, slug)
            if local:
                return [f"![]({local})"]
        return []

    # Figure
    if tag == "figure":
        parts = _convert_children(node, slug)
        caption = node.find("figcaption")
        if caption:
            cap_text = caption.get_text(strip=True)
            if cap_text:
                parts.append(f"\n*{cap_text}*\n")
        return parts

    # Figcaption (handled by figure parent)
    if tag == "figcaption":
        return []

    # Blockquote
    if tag == "blockquote":
        inner = _children_text(node, slug).strip()
        if inner:
            quoted = "\n".join(f"> {line}" for line in inner.split("\n"))
            return [f"\n\n{quoted}\n\n"]
        return []

    # Code blocks
    if tag == "pre":
        code = node.find("code")
        if code:
            lang = ""
            cls = code.get("class", [])
            if cls:
                for c in cls:
                    if c.startswith("language-"):
                        lang = c.replace("language-", "")
                        break
            return [f"\n\n```{lang}\n{code.get_text()}\n```\n\n"]
        return [f"\n\n```\n{node.get_text()}\n```\n\n"]

    if tag == "code" and not _inside_pre(node):
        return [f"`{node.get_text()}`"]

    # Lists
    if tag in ("ul", "ol"):
        items = []
        for i, li in enumerate(node.find_all("li", recursive=False)):
            prefix = "- " if tag == "ul" else f"{i + 1}. "
            inner = _children_text(li, slug).strip()
            if inner:
                items.append(f"{prefix}{inner}")
        if items:
            return ["\n\n" + "\n".join(items) + "\n\n"]
        return []

    if tag == "li":
        return _convert_children(node, slug)

    # Line break
    if tag == "br":
        return ["\n"]

    # Horizontal rule
    if tag == "hr":
        return ["\n\n---\n\n"]

    # Table
    if tag == "table":
        return _convert_table(node, slug)

    # Skip table sub-elements (handled by table converter)
    if tag in ("thead", "tbody", "tfoot", "tr", "th", "td"):
        return _convert_children(node, slug)

    # Div and other containers — recurse
    return _convert_children(node, slug)


def _convert_children(node, slug: str) -> list[str]:
    parts = []
    for child in node.children:
        parts.extend(_convert_node(child, slug))
    return parts


def _children_text(node, slug: str) -> str:
    return "".join(_convert_children(node, slug))


def _inside_pre(node) -> bool:
    parent = node.parent
    while parent:
        if isinstance(parent, Tag) and parent.name == "pre":
            return True
        parent = parent.parent
    return False


def _convert_table(table, slug: str) -> list[str]:
    """Convert an HTML table to markdown."""
    rows = table.find_all("tr")
    if not rows:
        return []

    md_rows = []
    for row in rows:
        cells = row.find_all(["th", "td"])
        md_cells = [_children_text(c, slug).strip().replace("|", "\\|") for c in cells]
        md_rows.append("| " + " | ".join(md_cells) + " |")

    if len(md_rows) < 1:
        return []

    # Insert separator after first row (header)
    first_row_cells = rows[0].find_all(["th", "td"])
    separator = "| " + " | ".join("---" for _ in first_row_cells) + " |"
    md_rows.insert(1, separator)

    return ["\n\n" + "\n".join(md_rows) + "\n\n"]


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------
def extract_content(soup: BeautifulSoup) -> Tag | None:
    """Find the main content container."""
    for sel in CONTENT_SELECTORS:
        container = soup.select_one(sel)
        if container:
            return container
    return None


def remove_noise(container: Tag) -> None:
    """Remove noisy elements in-place from the content container."""
    for sel in NOISE_SELECTORS:
        for el in container.select(sel):
            el.decompose()


def is_listing_page(soup: BeautifulSoup, container: Tag | None, url: str) -> bool:
    """Return True if this page looks like a listing rather than an article."""
    parsed_path = urlparse(url).path.rstrip("/")

    # URL-based listing indicators
    for cp in CONTENT_PATH_PATTERNS:
        cp_stripped = cp.rstrip("/")
        if parsed_path == cp_stripped:
            return True

    # Title-based indicators
    title = soup.title.string if soup.title else ""
    if title:
        listing_words = ["archive", "all posts", "page 2", "page 3", "category:"]
        if any(w in title.lower() for w in listing_words):
            return True

    if container is None:
        return True

    text = container.get_text(separator=" ", strip=True)
    word_count = len(text.split())

    # Too few words + many links = listing
    links = container.find_all("a")
    if word_count < 200 and len(links) > 10:
        return True

    # Many article cards
    articles = container.find_all("article")
    h2_links = container.select("h2 a")
    if len(articles) > 3 or len(h2_links) > 5:
        return True

    return False


def extract_title(soup: BeautifulSoup) -> str:
    """Extract article title."""
    # h1
    h1 = soup.find("h1")
    if h1:
        t = h1.get_text(strip=True)
        if t:
            return t

    # og:title
    meta = soup.find("meta", property="og:title")
    if meta and meta.get("content"):
        return meta["content"].strip()

    # <title>
    if soup.title and soup.title.string:
        # Strip site name suffix
        title = soup.title.string.strip()
        for sep in (" - ", " | ", " :: ", " » "):
            if sep in title:
                title = title.split(sep)[0].strip()
        return title

    return "Untitled"


def extract_brief(soup: BeautifulSoup) -> str:
    """Extract short description/subtitle."""
    meta = soup.find("meta", property="og:description")
    if meta and meta.get("content"):
        return meta["content"].strip()

    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return meta["content"].strip()

    return ""


# ---------------------------------------------------------------------------
# Per-page scraper
# ---------------------------------------------------------------------------
def scrape_page(
    entry: dict,
    state: dict,
    content_hashes: dict,
    force: bool,
) -> dict | None:
    """Scrape a single page. Returns updated state entry or None on skip/failure."""
    url = entry["loc"]
    lastmod = entry.get("lastmod", "")
    changefreq = entry.get("changefreq", "")
    slug = generate_slug(url)

    # Check incremental state
    slug_dir = BASE_DIR / slug
    content_file = slug_dir / "content.md"

    if not force:
        stored_lastmod = state.get(slug, {}).get("lastmod")
        if stored_lastmod and stored_lastmod == lastmod and content_file.exists():
            print(f"[skip] {slug} — unchanged")
            return None

    print(f"[fetch] {slug} ← {url}")

    try:
        resp = _throttled_get(url)
        resp.raise_for_status()
    except Exception as exc:
        print(f"  [error] {slug}: {exc}")
        return None

    soup = BeautifulSoup(resp.content, "lxml")
    container = extract_content(soup)

    # Listing detection
    if is_listing_page(soup, container, url):
        print(f"  [skip-listing] {slug}")
        return None

    if container is None:
        print(f"  [warn] No content container found for {slug}")
        return None

    # Remove noise
    remove_noise(container)

    # Convert to markdown
    md = html_to_markdown(container, slug)

    # Content dedup
    content_hash = hashlib.md5(md.encode()).hexdigest()
    if content_hash in content_hashes:
        print(f"  [dedup] {slug} — duplicate of {content_hashes[content_hash]}")
        return None
    content_hashes[content_hash] = slug

    # Check for truncation / paywall
    truncated = False
    paywall_indicators = [
        ".paywall", ".subscribe-cta", "[class*='subscribe']",
        "[class*='paywall']", "[class*='premium']",
    ]
    for sel in paywall_indicators:
        if soup.select_one(sel):
            truncated = True
            break
    if len(md.split()) < 100 and truncated:
        print(f"  [warn] Content appears truncated for {slug}")

    # Extract metadata
    title = extract_title(soup)
    brief = extract_brief(soup)
    pub_date = extract_date(soup, url, lastmod)

    # Try HTTP Last-Modified as final fallback
    if not pub_date:
        lm = resp.headers.get("Last-Modified")
        if lm:
            pub_date = _parse_date_str(lm)

    content_type, category = detect_content_type(urlparse(url).path)
    tags = extract_tags(soup)

    # Build meta.yaml
    meta: dict = {"title": title}
    if pub_date:
        meta["publish-date"] = pub_date
    if changefreq:
        meta["change-frequency"] = changefreq
    if brief:
        meta["short-brief"] = brief
    meta["source-url"] = url
    meta["content-type"] = content_type
    if category:
        meta["category"] = category
    if tags:
        meta["tags"] = tags
    if truncated:
        meta["truncated"] = True

    # Write files
    slug_dir.mkdir(parents=True, exist_ok=True)

    with open(slug_dir / "meta.yaml", "w", encoding="utf-8") as f:
        yaml.dump(meta, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    with open(content_file, "w", encoding="utf-8") as f:
        f.write(md)

    return {
        "slug": slug,
        "lastmod": lastmod,
        "content_hash": content_hash,
    }


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------
def replace_internal_links() -> int:
    """Scan all content.md files and replace internal links with local paths."""
    replaced_count = 0
    slugs_on_disk = {d.name for d in BASE_DIR.iterdir() if d.is_dir() and d.name != "images"}

    internal_link_re = re.compile(
        rf"\[([^\]]+)\]\(https?://{re.escape(DOMAIN)}(/[^)]*)\)"
    )

    for slug_name in slugs_on_disk:
        content_file = BASE_DIR / slug_name / "content.md"
        if not content_file.exists():
            continue

        text = content_file.read_text(encoding="utf-8")
        original = text

        def _replacer(m):
            nonlocal replaced_count
            link_text = m.group(1)
            path = m.group(2)
            # Strip query params and fragments
            clean = urlparse(path)
            clean_path = clean.path
            target_slug = generate_slug(f"https://{DOMAIN}{clean_path}")
            if target_slug in slugs_on_disk:
                replaced_count += 1
                return f"[{link_text}](../{target_slug}/content.md)"
            return m.group(0)

        text = internal_link_re.sub(_replacer, text)

        if text != original:
            content_file.write_text(text, encoding="utf-8")

    return replaced_count


# ---------------------------------------------------------------------------
# Slug collision resolution
# ---------------------------------------------------------------------------
def resolve_slug_collisions(entries: list[dict]) -> list[dict]:
    """Ensure unique slugs by appending -2, -3, etc."""
    seen: dict[str, int] = {}
    result = []

    for entry in entries:
        slug = generate_slug(entry["loc"])
        if slug in seen:
            seen[slug] += 1
            new_slug_suffix = f"-{seen[slug]}"
            # Store the collision info for later use
            entry = {**entry, "_slug_override": slug + new_slug_suffix}
        else:
            seen[slug] = 1
        result.append(entry)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape menabytes.com via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything, ignore state")
    parser.add_argument("--slug", type=str, help="Fetch only this slug")
    args = parser.parse_args()

    # Ensure output dirs exist
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching sitemap from {SITEMAP_INDEX_URL} ...")
    entries = fetch_sitemap_urls()
    print(f"Found {len(entries)} URLs in sitemap")

    if not entries:
        print("No URLs found. Check sitemap availability.")
        return

    # Filter out skip URLs
    entries = [e for e in entries if not should_skip_url(e["loc"])]
    print(f"{len(entries)} article URLs after filtering")

    # Resolve slug collisions
    entries = resolve_slug_collisions(entries)

    # Filter for specific slug if requested
    if args.slug:
        target = args.slug.lower().strip()
        entries = [
            e for e in entries
            if generate_slug(e["loc"]) == target
            or e.get("_slug_override", "").rstrip("-0123456789") == target
        ]
        if not entries:
            print(f"No sitemap entry matches slug '{args.slug}'")
            return
        print(f"Filtered to {len(entries)} entries matching slug '{args.slug}'")

    # Load state
    state = {} if args.force else load_state()
    content_hashes: dict[str, str] = {}

    # Pre-populate content hashes from existing state
    for slug_info in state.values():
        if isinstance(slug_info, dict) and "content_hash" in slug_info:
            content_hashes[slug_info["content_hash"]] = slug_info.get("slug", "")

    # Stats
    fetched = 0
    skipped = 0
    errors = 0
    deduped = 0

    # Process with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_entry = {
            executor.submit(scrape_page, entry, state, content_hashes, args.force): entry
            for entry in entries
        }

        for future in as_completed(future_to_entry):
            entry = future_to_entry[future]
            try:
                result = future.result()
                if result is None:
                    skipped += 1
                else:
                    fetched += 1
                    slug = result["slug"]
                    state[slug] = {
                        "lastmod": result["lastmod"],
                        "content_hash": result["content_hash"],
                        "slug": slug,
                    }
            except Exception as exc:
                errors += 1
                print(f"  [error] {entry['loc']}: {exc}")

    # Save state
    save_state(state)

    # Post-scrape: replace internal links
    print("\nReplacing internal links...")
    link_count = replace_internal_links()

    # Count images
    image_count = len(list(IMAGES_DIR.glob("*"))) if IMAGES_DIR.exists() else 0

    # Summary
    print(f"\n{'=' * 50}")
    print(f"Scrape complete for {DOMAIN}")
    print(f"  Fetched:   {fetched}")
    print(f"  Skipped:   {skipped}")
    print(f"  Errors:    {errors}")
    print(f"  Images:    {image_count}")
    print(f"  Links replaced: {link_count}")
    print(f"  Output:    {BASE_DIR}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
