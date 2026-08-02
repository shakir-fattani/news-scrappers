#!/usr/bin/env python3
"""
Sitemap-based scraper for www.zawya.com
Fetches sitemap index, discovers all articles, extracts content as markdown + YAML metadata.

Usage:
    python3 scrape_zawya.py              # incremental run
    python3 scrape_zawya.py --force      # re-fetch everything
    python3 scrape_zawya.py --slug X     # fetch only slug X
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
import hashlib
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse, unquote

import requests
import yaml
from bs4 import BeautifulSoup, NavigableString, Tag

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "www.zawya.com"
SITEMAP_INDEX_URL = "https://www.zawya.com/sitemap.xml"
BASE_DIR = Path(__file__).resolve().parent / "zawya"
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
]

SKIP_PATTERNS = [
    re.compile(r"/page/\d+/?$"),
    re.compile(r"[?&]page=\d+"),
    re.compile(r"[?&]p=\d+"),
    re.compile(r"/search/?$"),
    re.compile(r"/about/?$"),
    re.compile(r"/contact/?$"),
    re.compile(r"/privacy/?$"),
    re.compile(r"/terms/?$"),
    re.compile(r"/sitemap\.xml$"),
    re.compile(r"/feed/?$"),
    re.compile(r"/rss/?$"),
    re.compile(r"/author/[^/]+/?$"),
    re.compile(r"/tag/[^/]+/?$"),
    re.compile(r"/category/[^/]+/?$"),
    re.compile(r"/archive/?$"),
]

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup", ".subscription-widget",
    ".comments", ".comment-section", ".author-bio", ".disclaimer", ".cookie-banner",
    ".breadcrumb", ".pagination", ".ad", ".advertisement",
    "[class*='promo']", "[class*='banner']", "[class*='popup']", "[class*='modal']",
]

CONTENT_SELECTORS = [
    "div.article-body", "div[class*='ArticleBody']", "div[class*='article-body']",
    "div.article__content-body", "div.body-content", "article.story-body",
    "div.article-detail", "div.content-area", "div.page-content",
    "div.field--name-body", "article .node__content",
    "div.entry-content", "article .post-content",
    "div.report-content", "div.publication-body",
    "article", "main", "div.content", "div.post", "div#content",
]

PAYWALL_INDICATORS = [
    "subscribe to continue", "sign up to read", "premium content",
    "unlock this article", "membership required", "paywall",
    "register to read", "subscribe now to read",
]

SKIP_TAGS = {"script", "style", "noscript", "svg", "button", "input", "form", "iframe"}

WP_PROXY_RE = re.compile(r"^https?://i[0-3]\.wp\.com/(.+)$")

DATE_SEGMENT_RE = re.compile(r"^(\d{4})$")
DATE_PATH_RE = re.compile(r"/(\d{4})/(\d{1,2})/(\d{1,2})/")
DATE_PATH_YM_RE = re.compile(r"/(\d{4})-(\d{2})/")

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------
def make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return sess


SESSION = make_session()


# ---------------------------------------------------------------------------
# Fetch state
# ---------------------------------------------------------------------------
def load_state() -> dict:
    if FETCH_STATE_FILE.exists():
        with open(FETCH_STATE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_state(state: dict) -> None:
    FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FETCH_STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Sitemap fetching
# ---------------------------------------------------------------------------
def fetch_xml(url: str) -> Optional[ET.Element]:
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        content = resp.text
        # Strip any XML namespace prefix issues
        content = re.sub(r'xmlns="[^"]+"', "", content, count=1)
        return ET.fromstring(content.encode("utf-8"))
    except Exception as exc:
        print(f"[error] Failed to fetch {url}: {exc}")
        return None


def fetch_sitemap_index() -> list[dict]:
    """Return list of {loc, lastmod, changefreq} from all child sitemaps."""
    root = fetch_xml(SITEMAP_INDEX_URL)
    if root is None:
        return []

    # Detect if this is a sitemap index or a plain sitemap
    tag_local = root.tag.split("}")[-1] if "}" in root.tag else root.tag

    if tag_local == "sitemapindex":
        child_urls = []
        for sitemap_el in root:
            loc_el = sitemap_el.find("loc")
            if loc_el is None:
                for child in sitemap_el:
                    tag_name = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    if tag_name == "loc":
                        loc_el = child
                        break
            if loc_el is not None and loc_el.text:
                child_urls.append(loc_el.text.strip())
        print(f"[info] Sitemap index has {len(child_urls)} child sitemaps")
        all_entries = []
        for child_url in child_urls:
            entries = fetch_child_sitemap(child_url)
            all_entries.extend(entries)
            time.sleep(0.5)
        return all_entries

    # It's a plain sitemap
    return parse_url_entries(root)


def fetch_child_sitemap(url: str) -> list[dict]:
    root = fetch_xml(url)
    if root is None:
        return []
    entries = parse_url_entries(root)
    print(f"  [info] {url} -> {len(entries)} URLs")
    return entries


def parse_url_entries(root: ET.Element) -> list[dict]:
    entries = []
    for url_el in root:
        entry = {"loc": None, "lastmod": None, "changefreq": None}
        for child in url_el:
            tag_name = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag_name == "loc" and child.text:
                entry["loc"] = child.text.strip()
            elif tag_name == "lastmod" and child.text:
                entry["lastmod"] = child.text.strip()
            elif tag_name == "changefreq" and child.text:
                entry["changefreq"] = child.text.strip()
        if entry["loc"]:
            entries.append(entry)
    return entries


# ---------------------------------------------------------------------------
# URL / slug helpers
# ---------------------------------------------------------------------------
def is_same_domain(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower().replace("www.", "")
    return host == DOMAIN.replace("www.", "")


def should_skip_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    for pat in SKIP_PATTERNS:
        if pat.search(url):
            return True
    # Skip if URL is just a bare content-type pattern with no slug after it
    # e.g. /en/markets/ or /en/economy/ with no article slug
    path_lower = path.lower()
    for cp in CONTENT_PATH_PATTERNS:
        cp_stripped = cp.rstrip("/")
        if path_lower == cp_stripped or path_lower.endswith(cp_stripped):
            # This is just the category page itself
            return True
    return False


def has_content_pattern(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    return any(cp in path for cp in CONTENT_PATH_PATTERNS)


def detect_content_type(url_path: str) -> tuple[str, Optional[str]]:
    """Return (content_type, category) from URL path."""
    path_lower = url_path.lower()
    # Sort patterns longest-first so nested patterns match before parents
    sorted_patterns = sorted(CONTENT_PATH_PATTERNS, key=len, reverse=True)

    matched = []
    for cp in sorted_patterns:
        if cp in path_lower:
            matched.append(cp.strip("/"))

    if not matched:
        return ("article", None)

    if len(matched) >= 2:
        deepest = matched[0]
        parent = matched[1]
        return (deepest, parent)

    return (matched[0], None)


def generate_slug(url: str, existing_slugs: set) -> str:
    parsed = urlparse(url)
    path = unquote(parsed.path).strip("/")
    segments = [s for s in path.split("/") if s]

    # Filter out language prefix segments like 'en', 'ar'
    if segments and len(segments[0]) == 2 and segments[0].isalpha():
        segments = segments[1:]

    # Filter out date segments (YYYY, MM, DD patterns)
    meaningful = []
    for seg in segments:
        if re.match(r"^\d{4}$", seg):
            continue
        if re.match(r"^\d{1,2}$", seg) and len(seg) <= 2:
            continue
        if re.match(r"^\d{4}-\d{2}$", seg):
            continue
        meaningful.append(seg)

    # Remove segments that match content path patterns
    filtered = []
    content_slugs = {cp.strip("/") for cp in CONTENT_PATH_PATTERNS}
    for seg in meaningful:
        if seg.lower() not in content_slugs:
            filtered.append(seg)

    if filtered:
        slug = filtered[-1]
    elif meaningful:
        slug = meaningful[-1]
    elif segments:
        slug = segments[-1]
    else:
        slug = hashlib.md5(url.encode()).hexdigest()[:10]

    # Clean slug
    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    if not slug:
        slug = hashlib.md5(url.encode()).hexdigest()[:10]

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
def extract_date(soup: BeautifulSoup, url: str, lastmod: Optional[str],
                 headers: Optional[dict] = None) -> Optional[str]:
    """Extract publish date using priority chain. Returns ISO date string or None."""

    def normalize_date(raw: str) -> Optional[str]:
        raw = raw.strip()
        # ISO-8601 full datetime
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        # Try other common formats
        for fmt in ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y",
                    "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]:
            try:
                from datetime import datetime
                dt = datetime.strptime(raw[:len(raw)].split("T")[0].split("+")[0], fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    # 1. article:published_time
    meta_apt = soup.find("meta", property="article:published_time")
    if meta_apt and meta_apt.get("content"):
        d = normalize_date(meta_apt["content"])
        if d:
            return d

    # 2. meta name=date / publish-date
    for name in ["date", "publish-date", "publishdate", "publication_date"]:
        meta_d = soup.find("meta", attrs={"name": name})
        if meta_d and meta_d.get("content"):
            d = normalize_date(meta_d["content"])
            if d:
                return d

    # 3. <time> element
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        d = normalize_date(time_el["datetime"])
        if d:
            return d

    # 4. class*=date elements
    for selector in ["[class*='date']", "[class*='timestamp']", "[class*='published']"]:
        els = soup.select(selector)
        for el in els:
            if el.name in SKIP_TAGS:
                continue
            text = el.get_text(strip=True)
            if text:
                d = normalize_date(text)
                if d:
                    return d
            if el.get("datetime"):
                d = normalize_date(el["datetime"])
                if d:
                    return d

    # 5. JSON-LD datePublished
    for script_el in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script_el.string or "")
            if isinstance(ld, list):
                ld = ld[0] if ld else {}
            dp = ld.get("datePublished") or ld.get("dateCreated")
            if dp:
                d = normalize_date(str(dp))
                if d:
                    return d
        except (json.JSONDecodeError, TypeError, IndexError):
            continue

    # 6. URL date segments
    m = DATE_PATH_RE.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    m = DATE_PATH_YM_RE.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"

    # 7. sitemap lastmod
    if lastmod:
        d = normalize_date(lastmod)
        if d:
            return d

    # 8. Last-Modified header
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


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------
def extract_tags(soup: BeautifulSoup) -> list[str]:
    tags = set()

    # 1. meta keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        for kw in meta_kw["content"].split(","):
            kw = kw.strip().lower()
            if kw:
                tags.add(kw)

    # 2. article:tag (multiple)
    for meta_tag in soup.find_all("meta", property="article:tag"):
        if meta_tag.get("content"):
            tags.add(meta_tag["content"].strip().lower())

    # 3. JSON-LD keywords
    for script_el in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script_el.string or "")
            if isinstance(ld, list):
                ld = ld[0] if ld else {}
            kw = ld.get("keywords")
            if isinstance(kw, str):
                for k in kw.split(","):
                    k = k.strip().lower()
                    if k:
                        tags.add(k)
            elif isinstance(kw, list):
                for k in kw:
                    if isinstance(k, str):
                        tags.add(k.strip().lower())
            # about / mentions
            for field in ["about", "mentions"]:
                items = ld.get(field, [])
                if isinstance(items, dict):
                    items = [items]
                for item in items:
                    if isinstance(item, dict):
                        name = item.get("name", "")
                        if name:
                            tags.add(name.strip().lower())
        except (json.JSONDecodeError, TypeError, IndexError):
            continue

    # 4. Visible tag links
    for selector in ['a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a",
                     "[class*='tag-link']", ".cat-links a", ".entry-categories a",
                     "[class*='topic'] a"]:
        for a_el in soup.select(selector):
            text = a_el.get_text(strip=True).lower()
            if text and len(text) < 60:
                tags.add(text)

    return sorted(tags)


# ---------------------------------------------------------------------------
# Title & brief extraction
# ---------------------------------------------------------------------------
def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()
    title_el = soup.find("title")
    if title_el:
        return title_el.get_text(strip=True)
    return "Untitled"


def extract_brief(soup: BeautifulSoup) -> str:
    og = soup.find("meta", property="og:description")
    if og and og.get("content"):
        return og["content"].strip()
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()
    return ""


# ---------------------------------------------------------------------------
# Image handling
# ---------------------------------------------------------------------------
def resolve_wp_proxy(src: str) -> tuple[str, str]:
    """Return (fetch_url, filename_base) handling WordPress proxy URLs."""
    m = WP_PROXY_RE.match(src)
    if m:
        original_path = m.group(1)
        # Strip query params for filename
        clean_path = original_path.split("?")[0]
        return (src.split("?")[0], clean_path)
    return (src.split("?")[0], src.split("?")[0])


def download_image(src: str, slug: str) -> Optional[str]:
    """Download image, return local filename or None."""
    if not src or src.startswith("data:"):
        return None

    fetch_url, name_base = resolve_wp_proxy(src)

    # Determine extension
    path_part = urlparse(name_base).path
    ext = os.path.splitext(path_part)[1].lower()
    if ext not in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp", ".ico"}:
        ext = ".jpg"

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
    except Exception as exc:
        print(f"    [warn] Image download failed: {src}: {exc}")
        return None


# ---------------------------------------------------------------------------
# HTML -> Markdown converter
# ---------------------------------------------------------------------------
def html_to_markdown(element: Tag, slug: str, depth: int = 0) -> str:
    """Convert a BeautifulSoup element tree to markdown recursively."""
    parts = []
    for child in element.children:
        if isinstance(child, NavigableString):
            text = str(child)
            if text.strip():
                parts.append(text)
            elif text and parts:
                parts.append(" ")
            continue

        if not isinstance(child, Tag):
            continue

        tag = child.name.lower()
        if tag in SKIP_TAGS:
            continue

        # Skip subscribe / share / paywall widgets
        cls = " ".join(child.get("class", [])).lower()
        if any(kw in cls for kw in ["subscribe", "share", "social", "paywall",
                                     "newsletter", "signup", "comments", "cookie",
                                     "related", "recommended", "sidebar",
                                     "breadcrumb", "pagination", "promo",
                                     "banner", "popup", "modal", "ad-"]):
            continue

        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag[1])
            text = child.get_text(strip=True)
            if text:
                parts.append(f"\n\n{'#' * level} {text}\n\n")

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
            # If wrapping an image, recurse into children
            if child.find("img"):
                parts.append(html_to_markdown(child, slug, depth + 1))
            else:
                text = child.get_text(strip=True)
                if text and href:
                    parts.append(f"[{text}]({href})")
                elif text:
                    parts.append(text)

        elif tag == "img":
            src = child.get("src") or child.get("data-src") or ""
            alt = child.get("alt", "").strip()
            if src:
                img_file = download_image(src, slug)
                if img_file:
                    parts.append(f"![{alt}](../images/{img_file})")
                else:
                    parts.append(f"![{alt}]({src})")

        elif tag == "picture":
            img = child.find("img")
            if img:
                src = img.get("src") or img.get("data-src") or ""
                alt = img.get("alt", "").strip()
                if src:
                    img_file = download_image(src, slug)
                    if img_file:
                        parts.append(f"![{alt}](../images/{img_file})")
                    else:
                        parts.append(f"![{alt}]({src})")
            else:
                source = child.find("source")
                if source and source.get("srcset"):
                    src = source["srcset"].split(",")[0].strip().split(" ")[0]
                    img_file = download_image(src, slug)
                    if img_file:
                        parts.append(f"![](../images/{img_file})")

        elif tag == "figure":
            inner = html_to_markdown(child, slug, depth + 1).strip()
            caption_el = child.find("figcaption")
            caption = ""
            if caption_el:
                caption = caption_el.get_text(strip=True)
            # Remove figcaption from inner since we handle it separately
            if caption and caption in inner:
                inner = inner.replace(caption, "").strip()
            if inner:
                parts.append(f"\n\n{inner}")
            if caption:
                parts.append(f"\n*{caption}*\n\n")
            elif inner:
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
                code_cls = " ".join(code_el.get("class", []))
                lang_match = re.search(r"language-(\w+)", code_cls)
                if lang_match:
                    lang = lang_match.group(1)
                code_text = code_el.get_text()
                parts.append(f"\n\n```{lang}\n{code_text}\n```\n\n")
            else:
                parts.append(f"\n\n```\n{child.get_text()}\n```\n\n")

        elif tag == "code":
            # Inline code (not inside pre)
            parts.append(f"`{child.get_text()}`")

        elif tag in ("ul", "ol"):
            items = child.find_all("li", recursive=False)
            list_parts = []
            for idx, li in enumerate(items, 1):
                inner = html_to_markdown(li, slug, depth + 1).strip()
                if inner:
                    prefix = "- " if tag == "ul" else f"{idx}. "
                    list_parts.append(f"{prefix}{inner}")
            if list_parts:
                parts.append("\n\n" + "\n".join(list_parts) + "\n\n")

        elif tag == "li":
            inner = html_to_markdown(child, slug, depth + 1).strip()
            if inner:
                parts.append(inner)

        elif tag == "table":
            parts.append(convert_table(child))

        elif tag == "br":
            parts.append("\n")

        elif tag == "hr":
            parts.append("\n\n---\n\n")

        elif tag in ("div", "span", "section", "article", "main"):
            inner = html_to_markdown(child, slug, depth + 1)
            if inner.strip():
                parts.append(inner)

        elif tag == "figcaption":
            # Handled by figure parent
            pass

        else:
            inner = html_to_markdown(child, slug, depth + 1)
            if inner.strip():
                parts.append(inner)

    result = "".join(parts)
    # Collapse excessive newlines
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


def convert_table(table: Tag) -> str:
    """Convert an HTML table to markdown."""
    rows = []
    for tr in table.find_all("tr"):
        cells = []
        for cell in tr.find_all(["th", "td"]):
            cells.append(cell.get_text(strip=True).replace("|", "\\|"))
        rows.append(cells)

    if not rows:
        return ""

    # Normalize column count
    max_cols = max(len(r) for r in rows)
    for r in rows:
        while len(r) < max_cols:
            r.append("")

    lines = []
    # Header row
    lines.append("| " + " | ".join(rows[0]) + " |")
    lines.append("| " + " | ".join(["---"] * max_cols) + " |")
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n\n" + "\n".join(lines) + "\n\n"


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------
def find_content_container(soup: BeautifulSoup) -> Optional[Tag]:
    """Find the main content container using priority selectors."""
    for selector in CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            return el
    body = soup.find("body")
    return body


def remove_noise(soup: BeautifulSoup) -> None:
    """Remove navigation, ads, and other noise elements."""
    for selector in NOISE_SELECTORS:
        for el in soup.select(selector):
            el.decompose()


def detect_paywall(soup: BeautifulSoup, word_count: int) -> bool:
    """Check for paywall indicators."""
    text_lower = soup.get_text(separator=" ").lower()
    for indicator in PAYWALL_INDICATORS:
        if indicator in text_lower:
            return True
    # Suspiciously short with paywall-class elements
    if word_count < 100:
        paywall_els = soup.select("[class*='paywall'], [class*='subscribe'], [id*='paywall']")
        if paywall_els:
            return True
    return False


def is_listing_page(soup: BeautifulSoup, url: str, content_el: Optional[Tag]) -> bool:
    """Determine if a page is a listing/index page rather than an article."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # URL ends with content-type pattern and nothing meaningful after
    path_segments = [s for s in path.split("/") if s]
    if path_segments:
        last_seg = path_segments[-1].lower()
        content_slugs = {cp.strip("/") for cp in CONTENT_PATH_PATTERNS}
        if last_seg in content_slugs:
            return True

    # Title indicators
    title = extract_title(soup).lower()
    for kw in ["archive", "all posts", "page 2", "page 3", "category:"]:
        if kw in title:
            return True

    # Check content area
    if content_el is None:
        return True

    text = content_el.get_text(separator=" ", strip=True)
    words = text.split()
    word_count = len(words)

    # Link-heavy, content-light
    internal_links = content_el.find_all("a", href=True)
    if word_count < 200 and len(internal_links) > 10:
        return True

    # Multiple article cards but no single dominant article
    article_cards = content_el.find_all("article")
    h2_links = content_el.select("h2 a, h3 a")
    if len(article_cards) > 3 or len(h2_links) > 5:
        if word_count < 300:
            return True

    # No date indicators at all
    has_date = bool(
        soup.find("meta", property="article:published_time")
        or soup.find("time", attrs={"datetime": True})
        or soup.select_one("[class*='date']")
    )
    has_og_article = soup.find("meta", property="og:type", content="article")
    if not has_date and not has_og_article and word_count < 300:
        return True

    return word_count < 200


# ---------------------------------------------------------------------------
# Process a single page
# ---------------------------------------------------------------------------
def process_page(entry: dict, state: dict, existing_slugs: set,
                 content_hashes: dict, force: bool) -> Optional[dict]:
    """
    Fetch and process a single URL entry.
    Returns updated state info dict or None on skip/failure.
    """
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq", "")

    slug = generate_slug(url, existing_slugs)
    existing_slugs.add(slug)

    # Check incremental state
    slug_dir = BASE_DIR / slug
    content_file = slug_dir / "content.md"
    meta_file = slug_dir / "meta.yaml"

    if not force:
        stored = state.get(slug)
        if stored and stored.get("lastmod") == lastmod and content_file.exists():
            print(f"[skip] {slug} -- unchanged")
            return None

    # Fetch page
    try:
        time.sleep(REQUEST_DELAY)
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[error] {slug}: {exc}")
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    remove_noise(soup)

    content_el = find_content_container(soup)

    # Listing vs article detection
    if is_listing_page(soup, url, content_el):
        print(f"[skip] {slug} -- listing/index page")
        return None

    if content_el is None:
        print(f"[skip] {slug} -- no content container found")
        return None

    # Extract content
    markdown = html_to_markdown(content_el, slug).strip()

    # Word count check
    plain_text = content_el.get_text(separator=" ", strip=True)
    word_count = len(plain_text.split())
    if word_count < 200:
        print(f"[skip] {slug} -- too few words ({word_count})")
        return None

    # Content deduplication
    content_hash = hashlib.md5(markdown.encode("utf-8")).hexdigest()
    if content_hash in content_hashes:
        original_slug = content_hashes[content_hash]
        print(f"[dedup] {slug} -- duplicate of {original_slug}")
        return None
    content_hashes[content_hash] = slug

    # Extract metadata
    title = extract_title(soup)
    brief = extract_brief(soup)
    pub_date = extract_date(soup, url, lastmod, dict(resp.headers))
    tags = extract_tags(soup)
    content_type, category = detect_content_type(urlparse(url).path)
    is_truncated = detect_paywall(soup, word_count)

    # Build meta
    meta = {
        "title": title,
        "publish-date": pub_date or "",
        "change-frequency": changefreq or "",
        "short-brief": brief,
        "source-url": url,
        "content-type": content_type,
    }
    if category:
        meta["category"] = category
    if tags:
        meta["tags"] = tags
    if is_truncated:
        meta["truncated"] = True

    # Write to disk
    slug_dir.mkdir(parents=True, exist_ok=True)
    with open(meta_file, "w", encoding="utf-8") as fh:
        yaml.dump(meta, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)
    with open(content_file, "w", encoding="utf-8") as fh:
        fh.write(markdown)

    print(f"[ok] {slug} ({word_count} words)")

    return {
        "slug": slug,
        "lastmod": lastmod,
        "content_hash": content_hash,
    }


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------
def localize_internal_links(base_dir: Path) -> int:
    """Replace internal links in content.md files with local relative paths."""
    replaced_count = 0
    slug_dirs = {
        d.name for d in base_dir.iterdir()
        if d.is_dir() and d.name != "images" and (d / "content.md").exists()
    }

    domain_pattern = re.compile(
        r"\[([^\]]+)\]\(https?://(?:www\.)?" + re.escape(DOMAIN.replace("www.", "")) + r"/([^)]+)\)"
    )

    for slug_name in slug_dirs:
        content_file = base_dir / slug_name / "content.md"
        try:
            text = content_file.read_text(encoding="utf-8")
        except Exception:
            continue

        original = text

        def replace_link(match):
            link_text = match.group(1)
            path = match.group(2)
            # Strip query params and fragments
            clean_path = path.split("?")[0].split("#")[0].rstrip("/")
            # Try to find matching local slug
            last_segment = clean_path.split("/")[-1] if clean_path else ""
            if last_segment and last_segment in slug_dirs:
                return f"[{link_text}](../{last_segment}/content.md)"
            return match.group(0)

        text = domain_pattern.sub(replace_link, text)

        if text != original:
            content_file.write_text(text, encoding="utf-8")
            replaced_count += 1

    return replaced_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description=f"Scrape {DOMAIN} via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch all pages ignoring state")
    parser.add_argument("--slug", type=str, help="Fetch only this slug")
    args = parser.parse_args()

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    state = {} if args.force else load_state()
    content_hashes: dict[str, str] = {}
    # Rebuild hash index from state
    for slug_key, slug_state in state.items():
        h = slug_state.get("content_hash")
        if h:
            content_hashes[h] = slug_key

    # Fetch sitemap
    print(f"[info] Fetching sitemap index: {SITEMAP_INDEX_URL}")
    entries = fetch_sitemap_index()
    print(f"[info] Total URL entries found: {len(entries)}")

    # Filter to content URLs only
    filtered = []
    for entry in entries:
        url = entry["loc"]
        if not has_content_pattern(url):
            continue
        if should_skip_url(url):
            continue
        filtered.append(entry)

    print(f"[info] Content URLs after filtering: {len(filtered)}")

    if not filtered:
        print("[warn] No content URLs found. Exiting.")
        return

    # If --slug, filter to that slug only
    if args.slug:
        target_slug = args.slug.lower().strip()
        slug_filtered = [
            e for e in filtered
            if target_slug in e["loc"].lower()
        ]
        if not slug_filtered:
            print(f"[error] No URL matching slug '{args.slug}' found in sitemap")
            return
        filtered = slug_filtered
        print(f"[info] Filtered to {len(filtered)} URL(s) matching slug '{args.slug}'")

    existing_slugs: set[str] = set()
    stats = {"ok": 0, "skip": 0, "error": 0, "dedup": 0}

    def worker(entry):
        return process_page(entry, state, existing_slugs, content_hashes, args.force)

    # Process with thread pool
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker, e): e for e in filtered}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    slug = result["slug"]
                    state[slug] = {
                        "lastmod": result["lastmod"],
                        "content_hash": result["content_hash"],
                    }
                    stats["ok"] += 1
                else:
                    stats["skip"] += 1
            except Exception as exc:
                entry = futures[future]
                print(f"[error] {entry['loc']}: {exc}")
                stats["error"] += 1

    # Save state
    save_state(state)

    # Post-scrape: localize internal links
    print("[info] Localizing internal links...")
    link_count = localize_internal_links(BASE_DIR)
    print(f"[info] Updated internal links in {link_count} files")

    # Summary
    print("\n--- Summary ---")
    print(f"  Fetched:    {stats['ok']}")
    print(f"  Skipped:    {stats['skip']}")
    print(f"  Errors:     {stats['error']}")
    print(f"  Duplicates: {stats['dedup']}")
    image_count = len(list(IMAGES_DIR.glob("*"))) if IMAGES_DIR.exists() else 0
    print(f"  Images:     {image_count}")
    print(f"  Output:     {BASE_DIR}")


if __name__ == "__main__":
    main()
