#!/usr/bin/env python3
from __future__ import annotations
"""
Homepage-crawl scraper for www.centralbank.ae (Central Bank of UAE)

No sitemap available (Cloudflare blocks). Uses BFS crawl from homepage to
discover all content pages under /en/news/, /en/publications/, /en/press-release/, etc.

Usage:
    python3 scrape_centralbank.py                  # incremental run (reuse cached URLs)
    python3 scrape_centralbank.py --recrawl         # re-discover URLs from homepage
    python3 scrape_centralbank.py --force           # re-fetch all content ignoring state
    python3 scrape_centralbank.py --recrawl --force  # full fresh run
    python3 scrape_centralbank.py --slug X          # fetch only slug X
    python3 scrape_centralbank.py --max-pages 200   # limit crawl depth

Dependencies:
    pip3 install --user --break-system-packages requests beautifulsoup4 pyyaml lxml
"""


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
_MISSING: list[str] = []
try:
    import requests
except ImportError:
    _MISSING.append("requests")
try:
    from bs4 import BeautifulSoup, Tag, NavigableString
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
    print("[error] Missing dependencies: " + ", ".join(_MISSING))
    print("       Run:  pip3 install --user --break-system-packages " + " ".join(_MISSING))
    raise SystemExit(1)

import argparse
import collections
import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as date_type
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "www.centralbank.ae"
START_URL = "https://www.centralbank.ae/en/"
BASE_DIR = Path(__file__).resolve().parent / "cbuae"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"
DISCOVERED_URLS_FILE = BASE_DIR / ".discovered-urls.json"

MAX_WORKERS = 5
CRAWL_WORKERS = 3
REQUEST_DELAY = 1.0
DEFAULT_MAX_PAGES = 500
DEFAULT_DEPTH = 10

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Content sections on centralbank.ae
CONTENT_PATH_PATTERNS = [
    "/news/", "/publications/", "/press-release/", "/press-releases/",
    "/announcements/", "/speeches/", "/reports/", "/regulations/",
    "/circulars/", "/guidelines/", "/notices/", "/statistics/",
    "/articles/", "/insights/", "/media-centre/", "/media-center/",
    "/about-the-cbuae/", "/financial-stability/",
    "/consumer-protection/", "/licensing/", "/supervision/",
    "/monetary-policy/", "/payment-systems/",
]

# URL patterns to skip during crawl
SKIP_URL_PATTERNS = re.compile(
    r"(/page/\d+|/search|/login|/register"
    r"|/privacy|/terms|/disclaimer|/sitemap"
    r"|/ar/|/ar$"  # Skip Arabic pages
    r"|\.pdf$|\.zip$|\.doc$|\.docx$|\.xls$|\.xlsx$"
    r"|\.jpg$|\.jpeg$|\.png$|\.gif$|\.svg$|\.webp$"
    r"|\.css$|\.js$|\.json$|\.xml$|\.rss$|\.atom$"
    r"|\.woff$|\.woff2$|\.ttf$|\.eot$"
    r"|\.mp4$|\.mp3$|\.wav$"
    r"|mailto:|tel:|javascript:|#$)",
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
    "[class*='modal']", ".cookie-notice",
    ".mega-menu", ".sub-menu", ".mobile-menu",
]

# CBUAE content selectors
CONTENT_SELECTORS = [
    "div.article-body", "div[class*='article-body']",
    "div.content-area", "div.page-content",
    "div[class*='page-body']", "div[class*='news-detail']",
    "div.field--name-body", "article .node__content",
    "div.entry-content", "article .post-content",
    "article", "main", "div.content", "div#content",
    "div.region-content",
]

SKIP_TAGS = frozenset([
    "script", "style", "noscript", "svg", "button", "iframe",
    "form", "input", "select", "textarea",
])

WP_PROXY_PATTERN = re.compile(r"^https?://i[0-3]\.wp\.com/(.+)$")
DATE_SEGMENT_RE = re.compile(r"^\d{4}$|^\d{2}$|^\d{4}-\d{2}$")
URL_DATE_RE = re.compile(r"/(\d{4})/(\d{2})(?:/(\d{2}))?/")

PAYWALL_INDICATORS = [
    "subscribe to continue", "sign up to read", "premium content",
    "unlock this article", "membership required",
]

# File extensions to skip during crawl link discovery
SKIP_EXTENSIONS = frozenset([
    ".pdf", ".zip", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".avif",
    ".css", ".js", ".json", ".xml", ".rss", ".atom",
    ".woff", ".woff2", ".ttf", ".eot",
    ".mp4", ".mp3", ".wav", ".avi",
])


# ---------------------------------------------------------------------------
# Shared session
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    return sess


SESSION = _make_session()


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


def save_discovered_urls(data: dict[str, Any]) -> None:
    DISCOVERED_URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DISCOVERED_URLS_FILE, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# URL normalization & helpers
# ---------------------------------------------------------------------------
def normalize_url(url: str, base_url: str = "") -> str | None:
    """Normalize a URL: resolve relative, strip fragment/trailing slash."""
    if not url:
        return None

    # Skip non-HTTP schemes
    if url.startswith(("mailto:", "tel:", "javascript:", "data:", "#")):
        return None

    # Resolve relative URLs
    if not url.startswith("http"):
        if base_url:
            url = urljoin(base_url, url)
        else:
            return None

    parsed = urlparse(url)

    # Must be our domain
    if parsed.netloc and DOMAIN.replace("www.", "") not in parsed.netloc.lower():
        return None

    # Strip fragment
    cleaned = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))

    # Strip trailing slash for consistency (except root)
    if cleaned.endswith("/") and parsed.path != "/":
        cleaned = cleaned.rstrip("/")

    return cleaned


def should_skip_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()

    if SKIP_URL_PATTERNS.search(url):
        return True

    # Check file extension
    ext = os.path.splitext(path)[1].lower()
    if ext in SKIP_EXTENSIONS:
        return True

    return False


def is_english_page(url: str) -> bool:
    """Only process English pages (/en/ prefix)."""
    parsed = urlparse(url)
    return parsed.path.startswith("/en/") or parsed.path == "/en"


# ---------------------------------------------------------------------------
# BFS Crawler (Phase 1)
# ---------------------------------------------------------------------------
def crawl(start_url: str, max_pages: int = DEFAULT_MAX_PAGES,
          max_depth: int = DEFAULT_DEPTH) -> dict[str, Any]:
    """
    BFS crawl from start_url. Returns dict of discovered URLs with metadata.
    """
    queue: collections.deque[tuple[str, int]] = collections.deque()
    queue.append((start_url, 0))
    visited: set[str] = set()
    discovered: dict[str, Any] = {}

    print(f"[crawl] Starting BFS from {start_url} (max_pages={max_pages}, max_depth={max_depth})")

    pages_crawled = 0

    while queue and pages_crawled < max_pages:
        url, depth = queue.popleft()

        if url in visited:
            continue
        if depth > max_depth:
            continue

        visited.add(url)
        pages_crawled += 1

        time.sleep(REQUEST_DELAY)

        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"  [crawl-error] {url}: {exc}")
            continue

        if pages_crawled % 20 == 0:
            print(f"  [crawl] Visited {pages_crawled} pages, queue size: {len(queue)}")

        soup = BeautifulSoup(resp.text, "lxml")

        # Classify this URL
        url_type = _classify_url(soup, url)
        discovered[url] = {
            "type": url_type,
            "depth": depth,
        }

        # Extract links from the page
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            normalized = normalize_url(href, url)
            if normalized is None:
                continue
            if normalized in visited:
                continue
            if should_skip_url(normalized):
                continue
            if not is_english_page(normalized):
                continue
            queue.append((normalized, depth + 1))

    print(f"[crawl] Finished. Visited {pages_crawled} pages, discovered {len(discovered)} URLs")
    return discovered


def _classify_url(soup: BeautifulSoup, url: str) -> str:
    """Classify a URL as 'content', 'listing', or 'skip'."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Skip utility pages
    segments = [s for s in path.split("/") if s]
    if segments:
        last_seg = segments[-1].lower()
        if last_seg in ("about", "contact", "privacy", "terms", "disclaimer",
                        "accessibility", "sitemap", "search"):
            return "skip"

    # Check if it's a content section root (listing)
    content_slug_set = {cp.strip("/") for cp in CONTENT_PATH_PATTERNS}
    if segments and len(segments) <= 2:
        # e.g. /en/news or /en/publications
        non_lang = [s for s in segments if s.lower() != "en"]
        if non_lang and len(non_lang) == 1 and non_lang[0].lower() in content_slug_set:
            return "listing"

    # Check article indicators
    has_article_meta = (
        soup.find("meta", property="article:published_time") is not None
        or soup.find("time") is not None
    )

    content_el = None
    for selector in CONTENT_SELECTORS:
        content_el = soup.select_one(selector)
        if content_el:
            break
    if content_el is None:
        content_el = soup.find("body")

    if content_el:
        text = content_el.get_text(separator=" ", strip=True)
        word_count = len(text.split())
        links = content_el.find_all("a", href=True)

        # Listing: many links, little unique text
        if word_count < 200 and len(links) > 10:
            return "listing"

        # Content: enough words and has article indicators
        if word_count >= 200:
            return "content"

        if has_article_meta and word_count >= 100:
            return "content"

    return "skip"


# ---------------------------------------------------------------------------
# Slug generation
# ---------------------------------------------------------------------------
def generate_slug(url_path: str) -> str:
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]
    if not segments:
        return "index"
    # Filter out 'en' language prefix
    segments = [s for s in segments if s.lower() != "en"]
    if not segments:
        return "index"
    content_slugs = {cp.strip("/") for cp in CONTENT_PATH_PATTERNS}
    for seg in reversed(segments):
        if DATE_SEGMENT_RE.match(seg):
            continue
        if seg.lower() in content_slugs:
            continue
        if seg.isdigit() and len(segments) > 1:
            continue
        slug = seg.lower()
        slug = re.sub(r"[^a-z0-9\-]", "-", slug)
        slug = re.sub(r"-+", "-", slug).strip("-")
        if slug:
            return slug
    slug = segments[-1].lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or "page"


def resolve_slug_collision(slug: str, used_slugs: set[str]) -> str:
    if slug not in used_slugs:
        return slug
    counter = 2
    while f"{slug}-{counter}" in used_slugs:
        counter += 1
    return f"{slug}-{counter}"


# ---------------------------------------------------------------------------
# Content type detection
# ---------------------------------------------------------------------------
def detect_content_type(url_path: str) -> tuple[str, str | None]:
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]
    # Remove 'en' prefix
    segments = [s for s in segments if s.lower() != "en"]
    content_slug_set = {cp.strip("/") for cp in CONTENT_PATH_PATTERNS}
    matched: list[tuple[int, str]] = []
    for i, seg in enumerate(segments):
        if seg.lower() in content_slug_set:
            matched.append((i, seg.lower()))
    if not matched:
        return ("article", None)
    if len(matched) >= 2:
        return (matched[-1][1], matched[0][1])
    return (matched[0][1], None)


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------
def extract_date(soup: BeautifulSoup, url: str,
                 headers: dict | None = None) -> str | None:
    meta_pub = soup.find("meta", property="article:published_time")
    if meta_pub and meta_pub.get("content"):
        d = _normalize_date(meta_pub["content"])
        if d:
            return d
    for name in ("date", "publish-date", "publish_date", "publication_date"):
        meta_date = soup.find("meta", attrs={"name": name})
        if meta_date and meta_date.get("content"):
            d = _normalize_date(meta_date["content"])
            if d:
                return d
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        d = _normalize_date(time_el["datetime"])
        if d:
            return d
    for selector in ["[class*='date']", "[class*='timestamp']", "[class*='published']"]:
        date_el = soup.select_one(selector)
        if date_el and date_el.name not in SKIP_TAGS:
            text = date_el.get_text(strip=True)
            d = _parse_date_text(text)
            if d:
                return d
            if date_el.get("datetime"):
                d = _normalize_date(date_el["datetime"])
                if d:
                    return d
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script_tag.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict):
                dp = data.get("datePublished") or data.get("dateCreated")
                if dp:
                    d = _normalize_date(str(dp))
                    if d:
                        return d
        except (json.JSONDecodeError, TypeError, IndexError):
            continue
    m = URL_DATE_RE.search(url)
    if m:
        year, month = m.group(1), m.group(2)
        day = m.group(3) or "01"
        return f"{year}-{month}-{day}"
    if headers:
        lm = headers.get("Last-Modified")
        if lm:
            try:
                from datetime import datetime
                dt = datetime.strptime(lm, "%a, %d %b %Y %H:%M:%S %Z")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
    return None


def _normalize_date(raw: str) -> str | None:
    raw = raw.strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
    return None


def _parse_date_text(text: str) -> str | None:
    import calendar
    months = {m.lower(): str(i).zfill(2) for i, m in enumerate(calendar.month_name) if m}
    months_abbr = {m.lower(): str(i).zfill(2) for i, m in enumerate(calendar.month_abbr) if m}
    all_months = {**months, **months_abbr}
    m = re.search(
        r"(\b(?:" + "|".join(all_months.keys()) + r")\b)\s+(\d{1,2}),?\s+(\d{4})",
        text.lower(),
    )
    if m:
        return f"{m.group(3)}-{all_months[m.group(1)]}-{m.group(2).zfill(2)}"
    m = re.search(
        r"(\d{1,2})\s+(\b(?:" + "|".join(all_months.keys()) + r")\b)\s+(\d{4})",
        text.lower(),
    )
    if m:
        return f"{m.group(3)}-{all_months[m.group(2)]}-{m.group(1).zfill(2)}"
    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------
def extract_tags(soup: BeautifulSoup) -> list[str]:
    raw_tags: list[str] = []
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        raw_tags.extend(t.strip() for t in meta_kw["content"].split(","))
    for meta_tag in soup.find_all("meta", property="article:tag"):
        if meta_tag.get("content"):
            raw_tags.append(meta_tag["content"].strip())
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script_tag.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict):
                kw = data.get("keywords")
                if isinstance(kw, list):
                    raw_tags.extend(kw)
                elif isinstance(kw, str):
                    raw_tags.extend(t.strip() for t in kw.split(","))
                for field in ("about", "mentions"):
                    items = data.get(field, [])
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict) and item.get("name"):
                                raw_tags.append(item["name"])
        except (json.JSONDecodeError, TypeError, IndexError):
            continue
    for selector in ['a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a",
                     "[class*='tag-link']", "[class*='topic'] a"]:
        for el in soup.select(selector):
            text = el.get_text(strip=True)
            if text:
                raw_tags.append(text)
    seen: set[str] = set()
    result: list[str] = []
    for tag in raw_tags:
        normalized = tag.strip().lower()
        if normalized and normalized not in seen and len(normalized) < 100:
            seen.add(normalized)
            result.append(normalized)
    return result


# ---------------------------------------------------------------------------
# Content element finder
# ---------------------------------------------------------------------------
def _find_content_element(soup: BeautifulSoup) -> Tag | None:
    for selector in CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            return el
    return soup.find("body")


# ---------------------------------------------------------------------------
# Noise removal
# ---------------------------------------------------------------------------
def remove_noise(soup: BeautifulSoup) -> None:
    for selector in NOISE_SELECTORS:
        for el in soup.select(selector):
            el.decompose()


# ---------------------------------------------------------------------------
# Title & brief
# ---------------------------------------------------------------------------
def _extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        return og_title["content"].strip()
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        title = title_tag.string.strip()
        for sep in (" | ", " - ", " \u2014 ", " \u2013 "):
            if sep in title:
                title = title.split(sep)[0].strip()
        return title
    return "Untitled"


def _extract_brief(soup: BeautifulSoup) -> str:
    og_desc = soup.find("meta", property="og:description")
    if og_desc and og_desc.get("content"):
        return og_desc["content"].strip()
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()
    return ""


# ---------------------------------------------------------------------------
# Image handling
# ---------------------------------------------------------------------------
def _resolve_wp_proxy(src: str) -> tuple[str, str]:
    m = WP_PROXY_PATTERN.match(src)
    if m:
        original_path = m.group(1)
        clean_path = original_path.split("?")[0]
        return (src, clean_path)
    return (src, src.split("?")[0])


def _image_extension(path: str) -> str:
    path_clean = path.split("?")[0].split("#")[0]
    ext = os.path.splitext(path_clean)[1].lower()
    if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp", ".tiff"):
        return ext
    return ".jpg"


def download_image(src: str, slug: str) -> str | None:
    if not src or src.startswith("data:"):
        return None
    if src.startswith("//"):
        src = "https:" + src
    elif not src.startswith("http"):
        src = urljoin(f"https://{DOMAIN}/", src)
    fetch_url, name_source = _resolve_wp_proxy(src)
    ext = _image_extension(name_source)
    url_hash = hashlib.md5(src.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    filepath = IMAGES_DIR / filename
    if filepath.exists():
        return filename
    try:
        resp = SESSION.get(fetch_url, timeout=20, stream=True)
        resp.raise_for_status()
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb") as fh:
            for chunk in resp.iter_content(8192):
                fh.write(chunk)
        return filename
    except requests.RequestException as exc:
        print(f"    [warn] Image download failed: {src} -- {exc}")
        return None


# ---------------------------------------------------------------------------
# HTML -> Markdown converter
# ---------------------------------------------------------------------------
def html_to_markdown(element: Tag, slug: str, depth: int = 0) -> str:
    parts: list[str] = []
    for child in element.children:
        if isinstance(child, NavigableString):
            text = str(child)
            if text.strip():
                parts.append(text)
            elif text:
                parts.append(" ")
            continue
        if not isinstance(child, Tag):
            continue
        tag = child.name
        if tag in SKIP_TAGS:
            continue
        cls = " ".join(child.get("class", [])).lower()
        if any(kw in cls for kw in ["subscribe", "share", "social", "paywall",
                                     "newsletter", "signup", "comments", "cookie",
                                     "related", "recommended", "sidebar",
                                     "breadcrumb", "pagination", "promo",
                                     "banner", "popup", "modal", "ad-",
                                     "mega-menu", "sub-menu"]):
            continue
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag[1])
            heading_text = child.get_text(strip=True)
            if heading_text:
                parts.append(f"\n\n{'#' * level} {heading_text}\n\n")
        elif tag == "p":
            inner = html_to_markdown(child, slug, depth + 1).strip()
            if inner:
                parts.append(f"\n\n{inner}\n\n")
        elif tag in ("strong", "b"):
            inner = html_to_markdown(child, slug, depth + 1).strip()
            if inner:
                parts.append(f"**{inner}**")
        elif tag in ("em", "i"):
            inner = html_to_markdown(child, slug, depth + 1).strip()
            if inner:
                parts.append(f"*{inner}*")
        elif tag == "a":
            href = child.get("href", "")
            if child.find("img"):
                parts.append(html_to_markdown(child, slug, depth + 1))
            else:
                link_text = child.get_text(strip=True)
                if link_text and href:
                    parts.append(f"[{link_text}]({href})")
                elif link_text:
                    parts.append(link_text)
        elif tag == "img":
            src = child.get("src") or child.get("data-src") or ""
            alt = child.get("alt", "")
            if src:
                img_file = download_image(src, slug)
                if img_file:
                    parts.append(f"![{alt}](../images/{img_file})")
        elif tag == "picture":
            img = child.find("img")
            if img:
                src = img.get("src") or img.get("data-src") or ""
                alt = img.get("alt", "")
                if src:
                    img_file = download_image(src, slug)
                    if img_file:
                        parts.append(f"![{alt}](../images/{img_file})")
            else:
                source = child.find("source")
                if source and source.get("srcset"):
                    srcset = source["srcset"].split(",")[0].strip().split(" ")[0]
                    img_file = download_image(srcset, slug)
                    if img_file:
                        parts.append(f"![](../images/{img_file})")
        elif tag == "figure":
            inner = html_to_markdown(child, slug, depth + 1).strip()
            caption_el = child.find("figcaption")
            caption = caption_el.get_text(strip=True) if caption_el else ""
            if caption and caption in inner:
                inner = inner.replace(caption, "").strip()
            if inner:
                parts.append(f"\n\n{inner}")
                if caption:
                    parts.append(f"\n*{caption}*")
                parts.append("\n\n")
        elif tag == "blockquote":
            inner = html_to_markdown(child, slug, depth + 1).strip()
            if inner:
                quoted = "\n".join(f"> {line}" for line in inner.split("\n"))
                parts.append(f"\n\n{quoted}\n\n")
        elif tag == "pre":
            code_el = child.find("code")
            if code_el:
                lang = ""
                for c in code_el.get("class", []):
                    if c.startswith("language-"):
                        lang = c.replace("language-", "")
                        break
                parts.append(f"\n\n```{lang}\n{code_el.get_text()}\n```\n\n")
            else:
                parts.append(f"\n\n```\n{child.get_text()}\n```\n\n")
        elif tag == "code" and depth > 0:
            parts.append(f"`{child.get_text()}`")
        elif tag == "ul":
            items = child.find_all("li", recursive=False)
            list_md = [f"- {html_to_markdown(li, slug, depth + 1).strip()}"
                       for li in items if html_to_markdown(li, slug, depth + 1).strip()]
            if list_md:
                parts.append("\n\n" + "\n".join(list_md) + "\n\n")
        elif tag == "ol":
            items = child.find_all("li", recursive=False)
            list_md = []
            for idx, li in enumerate(items, 1):
                item_text = html_to_markdown(li, slug, depth + 1).strip()
                if item_text:
                    list_md.append(f"{idx}. {item_text}")
            if list_md:
                parts.append("\n\n" + "\n".join(list_md) + "\n\n")
        elif tag == "table":
            parts.append(_table_to_markdown(child))
        elif tag == "hr":
            parts.append("\n\n---\n\n")
        elif tag == "br":
            parts.append("\n")
        elif tag in ("div", "span", "section", "article", "main", "figcaption"):
            inner = html_to_markdown(child, slug, depth + 1)
            if inner.strip():
                parts.append(inner)
        else:
            inner = html_to_markdown(child, slug, depth + 1)
            if inner.strip():
                parts.append(inner)
    result = "".join(parts)
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


def _table_to_markdown(table: Tag) -> str:
    rows: list[list[str]] = []
    for tr in table.find_all("tr"):
        cells = [cell.get_text(strip=True).replace("|", "\\|")
                 for cell in tr.find_all(["th", "td"])]
        if cells:
            rows.append(cells)
    if not rows:
        return ""
    max_cols = max(len(r) for r in rows)
    for row in rows:
        while len(row) < max_cols:
            row.append("")
    lines = ["| " + " | ".join(rows[0]) + " |",
             "| " + " | ".join("---" for _ in rows[0]) + " |"]
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")
    return "\n\n" + "\n".join(lines) + "\n\n"


# ---------------------------------------------------------------------------
# Page processing (Phase 2)
# ---------------------------------------------------------------------------
def process_page(
    url: str,
    slug: str,
    crawl_depth: int,
    state: dict[str, Any],
    content_hashes: dict[str, str],
    force: bool = False,
) -> bool:
    slug_dir = BASE_DIR / slug

    # Incremental check by content hash (no lastmod available)
    if not force:
        stored = state.get("slugs", {}).get(slug)
        if stored and (slug_dir / "content.md").exists():
            print(f"  [skip] {slug} -- unchanged")
            return False

    time.sleep(REQUEST_DELAY)

    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  [error] {slug} -- fetch failed: {exc}")
        return False

    # Content hash for incremental state
    page_hash = hashlib.md5(resp.text.encode()).hexdigest()

    # Check if page content changed
    if not force:
        stored = state.get("slugs", {}).get(slug)
        if stored and stored.get("content_hash") == page_hash and (slug_dir / "content.md").exists():
            print(f"  [skip] {slug} -- unchanged (hash match)")
            return False

    soup = BeautifulSoup(resp.text, "lxml")
    remove_noise(soup)

    content_el = _find_content_element(soup)
    if not content_el:
        print(f"  [skip] {slug} -- no content element found")
        return False

    markdown = html_to_markdown(content_el, slug).strip()
    word_count = len(markdown.split())

    if word_count < 200:
        print(f"  [skip] {slug} -- only {word_count} words")
        return False

    # Content dedup by extracted markdown hash
    md_hash = hashlib.md5(markdown.encode()).hexdigest()
    if md_hash in content_hashes:
        print(f"  [dedup] {slug} -- duplicate of {content_hashes[md_hash]}")
        return False
    content_hashes[md_hash] = slug

    title = _extract_title(soup)
    publish_date = extract_date(soup, url, dict(resp.headers))
    short_brief = _extract_brief(soup)
    tags = extract_tags(soup)
    content_type, category = detect_content_type(urlparse(url).path)

    slug_dir.mkdir(parents=True, exist_ok=True)

    meta: dict[str, Any] = {
        "title": title,
        "publish-date": publish_date,
        "change-frequency": "unknown",
        "short-brief": short_brief,
        "source-url": url,
        "content-type": content_type,
    }
    if category:
        meta["category"] = category
    meta["crawl-depth"] = crawl_depth
    if tags:
        meta["tags"] = tags

    with open(slug_dir / "meta.yaml", "w", encoding="utf-8") as fh:
        yaml.dump(meta, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)
    with open(slug_dir / "content.md", "w", encoding="utf-8") as fh:
        fh.write(markdown)

    if "slugs" not in state:
        state["slugs"] = {}
    state["slugs"][slug] = {
        "content_hash": page_hash,
        "last_fetched": str(date_type.today()),
    }

    print(f"  [saved] {slug} ({word_count} words)")
    return True


# ---------------------------------------------------------------------------
# Internal link replacement
# ---------------------------------------------------------------------------
def replace_internal_links(base_dir: Path) -> int:
    slug_dirs = {d.name for d in base_dir.iterdir() if d.is_dir() and d.name != "images"}
    replaced_count = 0
    domain_pattern = re.compile(
        r"\[([^\]]*)\]\(https?://(?:www\.)?" + re.escape(DOMAIN.replace("www.", "")) + r"/([^)]*)\)"
    )
    for slug_name in slug_dirs:
        content_file = base_dir / slug_name / "content.md"
        if not content_file.exists():
            continue
        content = content_file.read_text(encoding="utf-8")
        original = content

        def _replace_link(match: re.Match) -> str:
            link_text = match.group(1)
            url_path = match.group(2)
            clean_path = url_path.split("?")[0].split("#")[0].rstrip("/")
            target_slug = generate_slug("/" + clean_path)
            if target_slug in slug_dirs:
                return f"[{link_text}](../{target_slug}/content.md)"
            return match.group(0)

        content = domain_pattern.sub(_replace_link, content)
        if content != original:
            content_file.write_text(content, encoding="utf-8")
            replaced_count += 1
    return replaced_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=f"Crawl & scrape {DOMAIN}")
    parser.add_argument("--recrawl", action="store_true",
                        help="Re-discover URLs from homepage (ignoring cached URL list)")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch all content ignoring state")
    parser.add_argument("--slug", type=str, help="Fetch only this slug")
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES,
                        help=f"Max pages to crawl (default: {DEFAULT_MAX_PAGES})")
    parser.add_argument("--depth", type=int, default=DEFAULT_DEPTH,
                        help=f"Max crawl depth (default: {DEFAULT_DEPTH})")
    args = parser.parse_args()

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Phase 1: Crawl (discover URLs)
    discovered: dict[str, Any] = {}
    if not args.recrawl and DISCOVERED_URLS_FILE.exists():
        print("[info] Using cached URL list. Pass --recrawl to re-discover.")
        discovered = load_discovered_urls()
    else:
        discovered = crawl(START_URL, max_pages=args.max_pages, max_depth=args.depth)
        save_discovered_urls(discovered)

    # Filter to content pages
    content_urls = {
        url: info for url, info in discovered.items()
        if info.get("type") == "content"
    }
    listing_count = sum(1 for info in discovered.values() if info.get("type") == "listing")
    skip_count = sum(1 for info in discovered.values() if info.get("type") == "skip")

    print(f"[info] Discovered: {len(discovered)} total URLs")
    print(f"  Content: {len(content_urls)}, Listings: {listing_count}, Skipped: {skip_count}")

    # Phase 2: Scrape content pages
    state = load_state()
    content_hashes: dict[str, str] = {}
    for s, info in state.get("slugs", {}).items():
        ch = info.get("content_hash")
        if ch:
            content_hashes[ch] = s

    # Assign slugs
    used_slugs: set[str] = set()
    items_to_process: list[tuple[str, str, int]] = []
    for url, info in content_urls.items():
        parsed = urlparse(url)
        raw_slug = generate_slug(parsed.path)
        slug = resolve_slug_collision(raw_slug, used_slugs)
        used_slugs.add(slug)
        items_to_process.append((url, slug, info.get("depth", 0)))

    # Filter to single slug if requested
    if args.slug:
        items_to_process = [(u, s, d) for u, s, d in items_to_process if s == args.slug]
        if not items_to_process:
            print(f"[error] Slug '{args.slug}' not found in discovered URLs")
            return

    saved_count = 0
    skipped_count = 0

    def _worker(item: tuple[str, str, int]) -> bool:
        url, slug, depth = item
        try:
            return process_page(url, slug, depth, state, content_hashes, force=args.force)
        except Exception as exc:
            print(f"  [error] {slug} -- {exc}")
            return False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_worker, item): item for item in items_to_process}
        for future in as_completed(futures):
            try:
                if future.result():
                    saved_count += 1
                else:
                    skipped_count += 1
            except Exception:
                skipped_count += 1

    save_state(state)

    print("[info] Replacing internal links...")
    link_count = replace_internal_links(BASE_DIR)

    print("\n--- Summary ---")
    print(f"  Discovered URLs     : {len(discovered)}")
    print(f"  Content pages       : {len(content_urls)}")
    print(f"  Saved               : {saved_count}")
    print(f"  Skipped             : {skipped_count}")
    print(f"  Internal links fixed: {link_count}")
    print(f"  Output directory    : {BASE_DIR}")


if __name__ == "__main__":
    main()
