#!/usr/bin/env python3
from __future__ import annotations
"""
Scraper for gulfbusiness.com via sitemap index.

Usage:
    python3 scrape_gulfbusiness.py              # incremental run
    python3 scrape_gulfbusiness.py --force      # re-fetch everything
    python3 scrape_gulfbusiness.py --slug X     # fetch only slug X

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
import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "gulfbusiness.com"
SITEMAP_INDEX_URL = "https://gulfbusiness.com/sitemap/main.xml"
BASE_DIR = Path(__file__).resolve().parent / "gulf_business"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

MAX_WORKERS = 5
REQUEST_DELAY = 1.0
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

CONTENT_PATH_PATTERNS = [
    "/news/", "/articles/", "/press-release/", "/blogs/",
    "/insights/", "/market-insights/", "/opinion/", "/business/",
    "/lifestyle/", "/economy/", "/analysis/", "/review/", "/reports/",
]

SKIP_URL_PATTERNS = re.compile(
    r"(/page/\d+|/tag/|/category/|/author/|/search|/login|/register"
    r"|/about|/contact|/privacy|/terms|/cart|/checkout|/account"
    r"|/api/|/feed/|/comments/|/wp-admin|/wp-login|/wp-json"
    r"|/sitemap|\.xml$|\.rss$|\.atom$|\.pdf$|\.zip$)",
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
    "[class*='modal']", ".cookie-notice", ".wp-block-group",
    ".sharedaddy", ".post-navigation", "#comments",
    ".td-post-sharing", ".td-post-source-tags",
    ".td-post-next-prev", ".td-related-span",
    ".td-a-rec", ".td-crumb-container",
]

# WordPress content selectors in priority order (Gulf Business uses flavor theme)
CONTENT_SELECTORS = [
    "div.td-post-content",          # Gulf Business (flavor theme)
    "div.entry-content",            # WordPress standard
    "article .post-content",        # WP theme variant
    "div.single-content",           # WP theme variant
    "div.article-content",          # Generic news
    "div.post-body",                # HubSpot / generic
    "article",                      # Fallback semantic
    "main",                         # Fallback semantic
    "div.content",                  # Generic
    "div#content",                  # Generic
]

SKIP_TAGS = frozenset(["script", "style", "noscript", "svg", "button", "iframe", "form", "input", "select", "textarea"])

WP_PROXY_PATTERN = re.compile(r"^https?://i[0-3]\.wp\.com/(.+)$")

DATE_SEGMENT_RE = re.compile(r"^\d{4}$|^\d{2}$|^\d{4}-\d{2}$")
URL_DATE_RE = re.compile(r"/(\d{4})/(\d{2})(?:/(\d{2}))?/")

# XML namespaces for sitemaps
NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
}


# ---------------------------------------------------------------------------
# Shared session
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})
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


# ---------------------------------------------------------------------------
# Sitemap fetching
# ---------------------------------------------------------------------------
def fetch_sitemap_index(url: str) -> list[str]:
    """Fetch the sitemap index and return child sitemap URLs."""
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    child_urls: list[str] = []
    # Handle both sitemapindex and urlset at the top level
    for sitemap_el in root.findall("sm:sitemap", NS):
        loc_el = sitemap_el.find("sm:loc", NS)
        if loc_el is not None and loc_el.text:
            child_urls.append(loc_el.text.strip())

    # If no <sitemap> children found, the file itself might be a urlset
    if not child_urls:
        child_urls.append(url)

    return child_urls


def fetch_sitemap_urls(sitemap_url: str) -> list[dict[str, str | None]]:
    """Fetch a single sitemap and return list of {loc, lastmod, changefreq}."""
    try:
        resp = SESSION.get(sitemap_url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[warn] Could not fetch sitemap {sitemap_url}: {exc}")
        return []

    root = ET.fromstring(resp.content)
    entries: list[dict[str, str | None]] = []

    # Might be a nested sitemapindex
    nested_sitemaps = root.findall("sm:sitemap", NS)
    if nested_sitemaps:
        for sm_el in nested_sitemaps:
            loc_el = sm_el.find("sm:loc", NS)
            if loc_el is not None and loc_el.text:
                entries.extend(fetch_sitemap_urls(loc_el.text.strip()))
        return entries

    for url_el in root.findall("sm:url", NS):
        loc_el = url_el.find("sm:loc", NS)
        if loc_el is None or not loc_el.text:
            continue
        lastmod_el = url_el.find("sm:lastmod", NS)
        changefreq_el = url_el.find("sm:changefreq", NS)
        entries.append({
            "loc": loc_el.text.strip(),
            "lastmod": lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else None,
            "changefreq": changefreq_el.text.strip() if changefreq_el is not None and changefreq_el.text else None,
        })

    return entries


def gather_all_sitemap_entries() -> list[dict[str, str | None]]:
    """Fetch sitemap index, then all children, merge URL entries."""
    print(f"[info] Fetching sitemap index: {SITEMAP_INDEX_URL}")
    child_sitemaps = fetch_sitemap_index(SITEMAP_INDEX_URL)
    print(f"[info] Found {len(child_sitemaps)} child sitemaps")

    all_entries: list[dict[str, str | None]] = []
    seen_locs: set[str] = set()

    for sm_url in child_sitemaps:
        print(f"  [fetch] {sm_url}")
        entries = fetch_sitemap_urls(sm_url)
        for entry in entries:
            loc = entry["loc"]
            if loc and loc not in seen_locs:
                seen_locs.add(loc)
                all_entries.append(entry)
        time.sleep(0.5)

    print(f"[info] Total unique URLs from sitemaps: {len(all_entries)}")
    return all_entries


# ---------------------------------------------------------------------------
# URL classification
# ---------------------------------------------------------------------------
def is_content_url(url: str) -> bool:
    """Return True if the URL looks like an article page, not a listing."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Must be on the right domain
    if DOMAIN not in parsed.netloc:
        return False

    # Skip known non-content patterns
    if SKIP_URL_PATTERNS.search(path):
        return False

    # Root path is not content
    if not path or path == "/":
        return False

    segments = [s for s in path.split("/") if s]

    # Pure category / listing pages: URL ends at a content pattern with no slug after
    # e.g. /news/ or /business/ with no further segment
    if len(segments) <= 1:
        for pat in CONTENT_PATH_PATTERNS:
            pat_seg = pat.strip("/")
            if segments and segments[0] == pat_seg:
                return False  # Just the category, no article slug
        # Single-segment slug like /some-article is allowed
        if segments:
            return True
        return False

    # Has a slug after the pattern — likely an article
    return True


def is_listing_page(soup: BeautifulSoup, url: str) -> bool:
    """Heuristic check if a fetched page is a listing rather than an article."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Pagination in URL
    if re.search(r"/page/\d+", path):
        return True

    # Check title for listing indicators
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        title_lower = title_tag.string.lower()
        if any(kw in title_lower for kw in ("archive", "all posts", "page 2", "category:")):
            return True

    # Check for og:type = article
    og_type = soup.find("meta", property="article:published_time")
    has_time = soup.find("time")
    has_article_meta = og_type is not None or has_time is not None

    # Count article cards vs prose
    content_el = _find_content_element(soup)
    if content_el:
        text = content_el.get_text(separator=" ", strip=True)
        word_count = len(text.split())
        if word_count < 200:
            # Check if link-heavy
            links = content_el.find_all("a", href=True)
            if len(links) > 10:
                return True
            if not has_article_meta:
                return True

    return False


# ---------------------------------------------------------------------------
# Slug generation
# ---------------------------------------------------------------------------
def generate_slug(url_path: str) -> str:
    """Extract the last meaningful path segment as slug."""
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    if not segments:
        return "index"

    # Walk backwards to find the last non-date, non-pattern segment
    for seg in reversed(segments):
        # Skip date segments
        if DATE_SEGMENT_RE.match(seg):
            continue
        # Skip pure content-type pattern segments
        if f"/{seg}/" in "".join(CONTENT_PATH_PATTERNS):
            continue
        # Skip purely numeric parent segments (keep if it's the only candidate)
        if seg.isdigit() and len(segments) > 1:
            continue
        slug = seg.lower()
        slug = re.sub(r"[^a-z0-9\-]", "-", slug)
        slug = re.sub(r"-+", "-", slug).strip("-")
        if slug:
            return slug

    # Fallback: use last segment regardless
    slug = segments[-1].lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or "page"


def resolve_slug_collision(slug: str, used_slugs: set[str]) -> str:
    """Append -2, -3, etc. if slug already used."""
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
    """
    Return (content_type, category) from URL path.

    /news/headline              -> ('news', None)
    /insights/market-insights/x -> ('market-insights', 'insights')
    /business/slug              -> ('business', None)
    """
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    matched_patterns: list[tuple[int, str]] = []  # (index, pattern_name)
    for i, seg in enumerate(segments):
        if f"/{seg}/" in "".join(f" {p} " for p in CONTENT_PATH_PATTERNS):
            matched_patterns.append((i, seg))

    if not matched_patterns:
        return ("article", None)

    if len(matched_patterns) >= 2:
        parent = matched_patterns[0][1]
        child = matched_patterns[-1][1]
        return (child, parent)

    return (matched_patterns[0][1], None)


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------
def extract_date(soup: BeautifulSoup, url: str, lastmod: str | None) -> str | None:
    """Extract publish date using the priority chain."""

    # 1. article:published_time
    meta_pub = soup.find("meta", property="article:published_time")
    if meta_pub and meta_pub.get("content"):
        return _normalize_date(meta_pub["content"])

    # 2. meta name=date
    for name in ("date", "publish-date", "publish_date"):
        meta_date = soup.find("meta", attrs={"name": name})
        if meta_date and meta_date.get("content"):
            return _normalize_date(meta_date["content"])

    # 3. <time> element
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        return _normalize_date(time_el["datetime"])

    # 4. class*=date elements
    for selector in ["[class*='date']", "[class*='timestamp']"]:
        date_el = soup.select_one(selector)
        if date_el:
            text = date_el.get_text(strip=True)
            parsed = _parse_date_text(text)
            if parsed:
                return parsed

    # 5. JSON-LD datePublished
    for script_tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script_tag.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict):
                dp = data.get("datePublished")
                if dp:
                    return _normalize_date(dp)
        except (json.JSONDecodeError, TypeError, IndexError):
            continue

    # 6. URL date segments
    m = URL_DATE_RE.search(url)
    if m:
        year, month = m.group(1), m.group(2)
        day = m.group(3) or "01"
        return f"{year}-{month}-{day}"

    # 7. Sitemap lastmod
    if lastmod:
        return _normalize_date(lastmod)

    # 8. Will be handled at fetch time via Last-Modified header
    return None


def _normalize_date(raw: str) -> str | None:
    """Normalize various date formats to YYYY-MM-DD."""
    raw = raw.strip()
    # ISO 8601 variants
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # Slash format
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
    return raw[:10] if len(raw) >= 10 else raw


def _parse_date_text(text: str) -> str | None:
    """Try to extract a date from visible text."""
    import calendar
    months = {m.lower(): str(i).zfill(2) for i, m in enumerate(calendar.month_name) if m}
    months_abbr = {m.lower(): str(i).zfill(2) for i, m in enumerate(calendar.month_abbr) if m}
    all_months = {**months, **months_abbr}

    # "July 15, 2026" or "Jul 15, 2026"
    m = re.search(
        r"(\b(?:" + "|".join(all_months.keys()) + r")\b)\s+(\d{1,2}),?\s+(\d{4})",
        text.lower(),
    )
    if m:
        month_num = all_months[m.group(1)]
        day = m.group(2).zfill(2)
        year = m.group(3)
        return f"{year}-{month_num}-{day}"

    # "15 July 2026"
    m = re.search(
        r"(\d{1,2})\s+(\b(?:" + "|".join(all_months.keys()) + r")\b)\s+(\d{4})",
        text.lower(),
    )
    if m:
        day = m.group(1).zfill(2)
        month_num = all_months[m.group(2)]
        year = m.group(3)
        return f"{year}-{month_num}-{day}"

    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------
def extract_tags(soup: BeautifulSoup) -> list[str]:
    """Gather tags from multiple sources and deduplicate."""
    raw_tags: list[str] = []

    # 1. meta keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        raw_tags.extend(t.strip() for t in meta_kw["content"].split(","))

    # 2. article:tag (may appear multiple times)
    for meta_tag in soup.find_all("meta", property="article:tag"):
        if meta_tag.get("content"):
            raw_tags.append(meta_tag["content"].strip())

    # 3. JSON-LD keywords
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
                # about / mentions
                for field in ("about", "mentions"):
                    items = data.get(field, [])
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict) and item.get("name"):
                                raw_tags.append(item["name"])
        except (json.JSONDecodeError, TypeError, IndexError):
            continue

    # 4. Visible tag links
    for selector in ['a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a",
                      "[class*='tag-link']", ".td-tags a", ".entry-tags a"]:
        for el in soup.select(selector):
            text = el.get_text(strip=True)
            if text:
                raw_tags.append(text)

    # 5. WordPress category links
    for selector in [".cat-links a", ".entry-categories a", ".td-category a"]:
        for el in soup.select(selector):
            text = el.get_text(strip=True)
            if text:
                raw_tags.append(text)

    # Normalize and deduplicate
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
    """Find the main content container using priority selectors."""
    for selector in CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            return el
    return soup.find("body")


# ---------------------------------------------------------------------------
# Noise removal
# ---------------------------------------------------------------------------
def remove_noise(soup: BeautifulSoup) -> None:
    """Remove known noise elements in place."""
    for selector in NOISE_SELECTORS:
        for el in soup.select(selector):
            el.decompose()

    # Remove skip tags
    for tag_name in SKIP_TAGS:
        for el in soup.find_all(tag_name):
            el.decompose()


# ---------------------------------------------------------------------------
# Image handling
# ---------------------------------------------------------------------------
def _resolve_wp_proxy(src: str) -> tuple[str, str]:
    """
    Handle WordPress image proxy URLs.
    Returns (fetch_url, filename_source_path).
    """
    m = WP_PROXY_PATTERN.match(src)
    if m:
        original_path = m.group(1)
        # Strip query params for extension detection
        clean_path = original_path.split("?")[0]
        # Fetch from the proxy URL but derive name from original
        fetch_url = src.split("?")[0]
        return (src, clean_path)
    return (src, src.split("?")[0])


def _image_extension(path: str) -> str:
    """Extract file extension from URL path."""
    path_clean = path.split("?")[0].split("#")[0]
    ext = os.path.splitext(path_clean)[1].lower()
    if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp", ".tiff"):
        return ext
    return ".jpg"  # default


def download_image(src: str, slug: str) -> str | None:
    """Download image, return local filename or None on failure."""
    if not src or src.startswith("data:"):
        return None

    # Resolve absolute URL
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
        print(f"    [warn] Image download failed: {src} — {exc}")
        return None


# ---------------------------------------------------------------------------
# HTML -> Markdown converter
# ---------------------------------------------------------------------------
def html_to_markdown(element: Tag, slug: str, depth: int = 0) -> str:
    """Recursively convert an HTML element tree to markdown."""
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
            # If wrapping an image, recurse without flattening to text
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
            caption = ""
            if caption_el:
                caption = caption_el.get_text(strip=True)
            # Remove the figcaption text from inner if it got included
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
                code_class = code_el.get("class", [])
                if code_class:
                    for cls in code_class:
                        if cls.startswith("language-"):
                            lang = cls.replace("language-", "")
                            break
                code_text = code_el.get_text()
                parts.append(f"\n\n```{lang}\n{code_text}\n```\n\n")
            else:
                code_text = child.get_text()
                parts.append(f"\n\n```\n{code_text}\n```\n\n")

        elif tag == "code" and depth > 0:
            code_text = child.get_text()
            parts.append(f"`{code_text}`")

        elif tag == "ul":
            items = child.find_all("li", recursive=False)
            list_md = []
            for li in items:
                item_text = html_to_markdown(li, slug, depth + 1).strip()
                if item_text:
                    list_md.append(f"- {item_text}")
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

        elif tag == "li":
            inner = html_to_markdown(child, slug, depth + 1)
            parts.append(inner)

        elif tag == "table":
            parts.append(_table_to_markdown(child))

        elif tag == "hr":
            parts.append("\n\n---\n\n")

        elif tag == "br":
            parts.append("\n")

        elif tag == "div":
            # Generic div — recurse
            inner = html_to_markdown(child, slug, depth + 1)
            if inner.strip():
                parts.append(inner)

        elif tag == "span":
            inner = html_to_markdown(child, slug, depth + 1)
            parts.append(inner)

        else:
            # Unknown tag — recurse
            inner = html_to_markdown(child, slug, depth + 1)
            if inner.strip():
                parts.append(inner)

    result = "".join(parts)
    # Collapse 3+ newlines to 2
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


def _table_to_markdown(table: Tag) -> str:
    """Convert an HTML table to markdown table."""
    rows: list[list[str]] = []
    for tr in table.find_all("tr"):
        cells = []
        for cell in tr.find_all(["th", "td"]):
            cells.append(cell.get_text(strip=True).replace("|", "\\|"))
        if cells:
            rows.append(cells)

    if not rows:
        return ""

    # Normalize column count
    max_cols = max(len(r) for r in rows)
    for row in rows:
        while len(row) < max_cols:
            row.append("")

    lines: list[str] = []
    # Header row
    lines.append("| " + " | ".join(rows[0]) + " |")
    lines.append("| " + " | ".join("---" for _ in rows[0]) + " |")
    # Data rows
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n\n" + "\n".join(lines) + "\n\n"


# ---------------------------------------------------------------------------
# Page processing
# ---------------------------------------------------------------------------
def process_page(
    entry: dict[str, str | None],
    slug: str,
    state: dict[str, Any],
    content_hashes: dict[str, str],
    force: bool = False,
) -> bool:
    """Fetch and process a single article page. Returns True if saved."""
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq")

    slug_dir = BASE_DIR / slug

    # Incremental check
    if not force:
        stored = state.get("slugs", {}).get(slug)
        if stored and stored.get("lastmod") == lastmod and (slug_dir / "content.md").exists():
            print(f"  [skip] {slug} — unchanged")
            return False

    time.sleep(REQUEST_DELAY)

    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  [error] {slug} — fetch failed: {exc}")
        return False

    last_modified_header = resp.headers.get("Last-Modified")

    soup = BeautifulSoup(resp.text, "lxml")

    # Check if this is a listing page
    if is_listing_page(soup, url):
        print(f"  [skip] {slug} — listing page")
        return False

    # Remove noise
    remove_noise(soup)

    # Find content element
    content_el = _find_content_element(soup)
    if not content_el:
        print(f"  [skip] {slug} — no content element found")
        return False

    # Convert to markdown
    markdown = html_to_markdown(content_el, slug).strip()

    # Word count check
    word_count = len(markdown.split())
    if word_count < 200:
        # Check for paywall / truncation indicators
        paywall_indicators = soup.select(
            "[class*='paywall'], [class*='subscribe'], [class*='premium'], "
            "[class*='locked'], [id*='paywall']"
        )
        if paywall_indicators and word_count > 0:
            # Truncated content — save with flag
            truncated = True
        else:
            print(f"  [skip] {slug} — only {word_count} words")
            return False
    else:
        truncated = False

    # Content deduplication
    content_hash = hashlib.md5(markdown.encode()).hexdigest()
    if content_hash in content_hashes:
        original_slug = content_hashes[content_hash]
        print(f"  [dedup] {slug} — duplicate of {original_slug}")
        return False
    content_hashes[content_hash] = slug

    # Extract metadata
    title = _extract_title(soup)
    publish_date = extract_date(soup, url, lastmod)
    if not publish_date and last_modified_header:
        publish_date = _normalize_date(last_modified_header)
    short_brief = _extract_brief(soup)
    tags = extract_tags(soup)

    parsed_url = urlparse(url)
    content_type, category = detect_content_type(parsed_url.path)

    # Write files
    slug_dir.mkdir(parents=True, exist_ok=True)

    # meta.yaml
    meta: dict[str, Any] = {
        "title": title,
        "publish-date": publish_date,
        "change-frequency": changefreq or "unknown",
        "short-brief": short_brief,
        "source-url": url,
        "content-type": content_type,
    }
    if category:
        meta["category"] = category
    if tags:
        meta["tags"] = tags
    if truncated:
        meta["truncated"] = True

    with open(slug_dir / "meta.yaml", "w", encoding="utf-8") as fh:
        yaml.dump(meta, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)

    # content.md
    with open(slug_dir / "content.md", "w", encoding="utf-8") as fh:
        fh.write(markdown)

    # Update state
    if "slugs" not in state:
        state["slugs"] = {}
    state["slugs"][slug] = {
        "lastmod": lastmod,
        "content_hash": content_hash,
    }

    print(f"  [saved] {slug} ({word_count} words)")
    return True


def _extract_title(soup: BeautifulSoup) -> str:
    """Extract article title."""
    # h1
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text

    # og:title
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        return og_title["content"].strip()

    # <title>
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        # Strip site name suffix
        title = title_tag.string.strip()
        for sep in (" | ", " - ", " — ", " – "):
            if sep in title:
                title = title.split(sep)[0].strip()
        return title

    return "Untitled"


def _extract_brief(soup: BeautifulSoup) -> str:
    """Extract short description / brief."""
    # og:description
    og_desc = soup.find("meta", property="og:description")
    if og_desc and og_desc.get("content"):
        return og_desc["content"].strip()

    # meta description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()

    return ""


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------
def replace_internal_links(base_dir: Path) -> int:
    """Replace internal links in all content.md files with local relative paths."""
    slug_dirs = {d.name for d in base_dir.iterdir() if d.is_dir() and d.name != "images"}
    replaced_count = 0
    domain_pattern = re.compile(
        r"\[([^\]]*)\]\(https?://(?:www\.)?" + re.escape(DOMAIN) + r"/([^)]*)\)"
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
            # Strip query params and fragments
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
    parser = argparse.ArgumentParser(description="Scrape gulfbusiness.com via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything ignoring state")
    parser.add_argument("--slug", type=str, help="Fetch only this slug")
    args = parser.parse_args()

    # Ensure output dirs exist
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    state = load_state()
    content_hashes: dict[str, str] = {}

    # Rebuild content hash index from existing state
    for s, info in state.get("slugs", {}).items():
        ch = info.get("content_hash")
        if ch:
            content_hashes[ch] = s

    # Gather sitemap entries
    all_entries = gather_all_sitemap_entries()

    # Filter to content URLs
    content_entries: list[dict[str, str | None]] = []
    for entry in all_entries:
        loc = entry["loc"]
        if not loc:
            continue
        if not is_content_url(loc):
            continue
        content_entries.append(entry)

    print(f"[info] {len(content_entries)} content URLs to process")

    # Assign slugs
    used_slugs: set[str] = set()
    entries_with_slugs: list[tuple[dict[str, str | None], str]] = []

    for entry in content_entries:
        parsed = urlparse(entry["loc"])
        raw_slug = generate_slug(parsed.path)
        slug = resolve_slug_collision(raw_slug, used_slugs)
        used_slugs.add(slug)
        entries_with_slugs.append((entry, slug))

    # Filter to single slug if requested
    if args.slug:
        entries_with_slugs = [(e, s) for e, s in entries_with_slugs if s == args.slug]
        if not entries_with_slugs:
            print(f"[error] Slug '{args.slug}' not found in sitemap entries")
            return

    # Process pages with thread pool
    saved_count = 0
    skipped_count = 0
    error_count = 0

    def _worker(item: tuple[dict[str, str | None], str]) -> bool:
        entry, slug = item
        try:
            return process_page(entry, slug, state, content_hashes, force=args.force)
        except Exception as exc:
            print(f"  [error] {slug} — {exc}")
            return False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_worker, item): item for item in entries_with_slugs}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    saved_count += 1
                else:
                    skipped_count += 1
            except Exception:
                error_count += 1

    # Save state
    save_state(state)

    # Post-scrape: internal link replacement
    print("[info] Replacing internal links...")
    link_count = replace_internal_links(BASE_DIR)

    # Summary
    print("\n--- Summary ---")
    print(f"  Total sitemap entries : {len(all_entries)}")
    print(f"  Content URLs filtered : {len(content_entries)}")
    print(f"  Saved                 : {saved_count}")
    print(f"  Skipped               : {skipped_count}")
    print(f"  Errors                : {error_count}")
    print(f"  Internal links fixed  : {link_count}")
    print(f"  Output directory      : {BASE_DIR}")


if __name__ == "__main__":
    main()
