#!/usr/bin/env python3
"""
Sitemap-based scraper for www.newarab.com (Drupal / simple_sitemap).
Outputs structured markdown + YAML metadata to the_new_arab/.

Usage:
    python3 scrape_newarab.py              # incremental run
    python3 scrape_newarab.py --force      # re-fetch everything
    python3 scrape_newarab.py --slug X     # fetch only slug X
"""

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
from urllib.parse import urljoin, urlparse, urlunparse

import requests
import yaml
from bs4 import BeautifulSoup, NavigableString, Tag

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DOMAIN = "www.newarab.com"
BASE_URL = f"https://{DOMAIN}"
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap.xml"
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR / "the_new_arab"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

MAX_WORKERS = 5
REQUEST_DELAY = 1.0  # seconds between requests
REQUEST_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Content-type URL patterns
# ---------------------------------------------------------------------------
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
    "/features/", "/sport/", "/politics/", "/in-depth/",
    "/asia-pacific/", "/north-africa/", "/gulf/", "/levant/",
]

SKIP_PATTERNS = {
    "/search", "/tags", "/page/", "/archive", "/about",
    "/contact", "/privacy", "/terms", "/author/", "/authors/",
    "/login", "/register", "/rss", "/feed", "/sitemap",
}

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup",
    ".subscription-widget", ".comments", ".comment-section",
    ".author-bio", ".disclaimer", ".cookie-banner",
    ".breadcrumb", ".pagination", ".ad", ".advertisement",
    "[class*='promo']", "[class*='banner']", "[class*='popup']",
    "[class*='modal']", ".block-views", ".block-social",
    ".field--name-field-tags", ".node__links",
    ".article-share", ".article-tags-wrapper",
    ".tna-related", ".related-content",
]

SKIP_TAGS = {"script", "style", "noscript", "svg", "button", "form", "iframe"}

# ---------------------------------------------------------------------------
# Session setup
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})

# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if FETCH_STATE_FILE.exists():
        with open(FETCH_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"lastmod": {}, "content_hashes": {}}


def save_state(state: dict) -> None:
    FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FETCH_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Sitemap parsing
# ---------------------------------------------------------------------------

def fetch_sitemap_index(url: str) -> list[dict]:
    """Fetch sitemap index, then each child sitemap. Return list of {loc, lastmod}."""
    print(f"[sitemap] Fetching index: {url}")
    resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ns = _detect_ns(root)

    # Check if this is a sitemap index or a direct sitemap
    if root.tag == f"{{{ns}}}sitemapindex" or root.find(f"{{{ns}}}sitemap") is not None:
        child_urls = []
        for sitemap_el in root.findall(f"{{{ns}}}sitemap"):
            loc_el = sitemap_el.find(f"{{{ns}}}loc")
            if loc_el is not None and loc_el.text:
                child_urls.append(loc_el.text.strip())
        print(f"[sitemap] Found {len(child_urls)} child sitemaps")
        entries = []
        for child_url in child_urls:
            time.sleep(REQUEST_DELAY)
            entries.extend(_fetch_sitemap(child_url, ns))
        return entries

    # Direct sitemap with <url> entries
    return _parse_url_entries(root, ns)


def _detect_ns(root: ET.Element) -> str:
    tag = root.tag
    if tag.startswith("{"):
        return tag[1:tag.index("}")]
    return ""


def _fetch_sitemap(url: str, parent_ns: str) -> list[dict]:
    """Fetch a single sitemap and return its URL entries."""
    print(f"[sitemap] Fetching child: {url}")
    try:
        resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        ns = _detect_ns(root) or parent_ns
        return _parse_url_entries(root, ns)
    except Exception as exc:
        print(f"[error] Failed to fetch sitemap {url}: {exc}")
        return []


def _parse_url_entries(root: ET.Element, ns: str) -> list[dict]:
    """Extract {loc, lastmod, changefreq} from a sitemap's <url> elements."""
    entries = []
    prefix = f"{{{ns}}}" if ns else ""
    for url_el in root.findall(f"{prefix}url"):
        loc_el = url_el.find(f"{prefix}loc")
        if loc_el is None or not loc_el.text:
            continue
        lastmod_el = url_el.find(f"{prefix}lastmod")
        changefreq_el = url_el.find(f"{prefix}changefreq")
        entries.append({
            "loc": loc_el.text.strip(),
            "lastmod": lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else None,
            "changefreq": changefreq_el.text.strip() if changefreq_el is not None and changefreq_el.text else None,
        })
    return entries


# ---------------------------------------------------------------------------
# URL analysis helpers
# ---------------------------------------------------------------------------

def is_listing_page(url: str) -> bool:
    """Return True if the URL looks like a listing/index page rather than an article."""
    path = urlparse(url).path.rstrip("/")

    # Pagination
    if re.search(r"/page/\d+", path):
        return True

    # Pure category/section root (e.g. /news, /opinion with no slug after)
    segments = [s for s in path.split("/") if s]
    if len(segments) <= 1:
        return True

    # Skip patterns
    for skip in SKIP_PATTERNS:
        if skip in path:
            return True

    return False


def detect_content_type(url_path: str) -> tuple[str, str | None]:
    """
    Derive content_type and category from URL path.
    /news/headline-here         -> ('news', None)
    /news/world-news/headline   -> ('world-news', 'news')
    /opinion/piece-title        -> ('opinion', None)
    """
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    # Remove date segments (4-digit year, 1-2 digit month/day)
    segments = [s for s in segments if not re.match(r"^\d{1,4}$", s)]

    if len(segments) < 2:
        return ("article", None)

    # Check for nested content type pattern
    matched_indices = []
    for i, seg in enumerate(segments[:-1]):  # exclude last (slug)
        for pattern in CONTENT_PATH_PATTERNS:
            pat_seg = pattern.strip("/")
            if seg == pat_seg:
                matched_indices.append(i)
                break

    if len(matched_indices) >= 2:
        category = segments[matched_indices[0]]
        content_type = segments[matched_indices[-1]]
        return (content_type, category)
    elif len(matched_indices) == 1:
        return (segments[matched_indices[0]], None)

    # Fallback: first segment is content type
    return (segments[0], None)


def generate_slug(url: str) -> str:
    """
    Extract the last meaningful path segment as slug.
    Strips date segments and numeric-only parents.
    """
    path = urlparse(url).path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    if not segments:
        return "index"

    # Last segment is the slug
    slug = segments[-1]

    # Clean up
    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    slug = slug.strip("-")

    return slug or "index"


def _resolve_slug_collision(slug: str, existing_slugs: set) -> str:
    """Append -2, -3, etc. to avoid collisions."""
    if slug not in existing_slugs:
        return slug
    counter = 2
    while f"{slug}-{counter}" in existing_slugs:
        counter += 1
    return f"{slug}-{counter}"


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------

def extract_date(soup: BeautifulSoup, sitemap_lastmod: str | None) -> str | None:
    """
    Extract publish date using priority chain.
    Returns ISO date string (YYYY-MM-DD) or None.
    """
    # 1. article:published_time
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        return _normalize_date(meta["content"])

    # 2. meta name=date or publish-date
    for name in ("date", "publish-date", "publish_date", "pubdate"):
        meta = soup.find("meta", attrs={"name": name})
        if meta and meta.get("content"):
            return _normalize_date(meta["content"])

    # 3. <time datetime="...">
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        return _normalize_date(time_el["datetime"])

    # 4. date class patterns
    for selector in [".date", "[class*='date']", "[class*='timestamp']", ".field--name-created"]:
        el = soup.select_one(selector)
        if el and el.get_text(strip=True):
            parsed = _parse_date_text(el.get_text(strip=True))
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
    # patterns: /2025/1/1/ or /2025/01/01/
    url_match = re.search(r"/(\d{4})/(\d{1,2})/(\d{1,2})/", soup.find("link", rel="canonical", href=True)["href"] if soup.find("link", rel="canonical", href=True) else "")
    if url_match:
        y, m, d = url_match.groups()
        return f"{y}-{int(m):02d}-{int(d):02d}"

    # 7. Sitemap lastmod
    if sitemap_lastmod:
        return _normalize_date(sitemap_lastmod)

    # 8. (HTTP Last-Modified handled at caller level if needed)
    return None


def _normalize_date(raw: str) -> str | None:
    """Normalize various date formats to YYYY-MM-DD."""
    if not raw:
        return None
    raw = raw.strip()
    # ISO format: 2025-01-01T12:00:00+00:00
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def _parse_date_text(text: str) -> str | None:
    """Try to parse human-readable date text."""
    import locale
    month_map = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12",
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "jun": "06", "jul": "07", "aug": "08", "sep": "09",
        "oct": "10", "nov": "11", "dec": "12",
    }
    text = text.lower().strip()

    # "15 January 2025" or "January 15, 2025"
    m = re.search(r"(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\s+(\d{4})", text)
    if m:
        return f"{m.group(3)}-{month_map[m.group(2)]}-{int(m.group(1)):02d}"

    m = re.search(r"(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\s+(\d{1,2}),?\s+(\d{4})", text)
    if m:
        return f"{m.group(3)}-{month_map[m.group(1)]}-{int(m.group(2)):02d}"

    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------

def extract_tags(soup: BeautifulSoup) -> list[str]:
    """Extract and deduplicate tags from multiple sources."""
    tags = set()

    # 1. meta keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        for kw in meta_kw["content"].split(","):
            kw = kw.strip().lower()
            if kw:
                tags.add(kw)

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
                kws = data.get("keywords")
                if isinstance(kws, list):
                    for kw in kws:
                        tags.add(str(kw).strip().lower())
                elif isinstance(kws, str):
                    for kw in kws.split(","):
                        kw = kw.strip().lower()
                        if kw:
                            tags.add(kw)
        except (json.JSONDecodeError, TypeError):
            pass

    # 4. Visible tag links (Drupal field--name-field-tags, generic tag links)
    for selector in [
        'a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a",
        "[class*='tag-link']", ".field--name-field-tags a",
        ".cat-links a", ".entry-categories a",
    ]:
        for a in soup.select(selector):
            txt = a.get_text(strip=True).lower()
            if txt and len(txt) < 80:
                tags.add(txt)

    return sorted(tags)


# ---------------------------------------------------------------------------
# HTML to Markdown converter
# ---------------------------------------------------------------------------

def html_to_markdown(element: Tag, slug: str, base_url: str) -> str:
    """Convert an HTML element tree to markdown."""
    lines = _convert_element(element, slug, base_url)
    md = "\n".join(lines)
    # Collapse excessive blank lines
    md = re.sub(r"\n{3,}", "\n\n", md)
    return md.strip()


def _convert_element(element, slug: str, base_url: str) -> list[str]:
    """Recursively convert an element to markdown lines."""
    if isinstance(element, NavigableString):
        text = str(element)
        # Collapse whitespace but preserve single newlines
        text = re.sub(r"[ \t]+", " ", text)
        if text.strip():
            return [text.strip()]
        return []

    if not isinstance(element, Tag):
        return []

    tag_name = element.name.lower() if element.name else ""

    if tag_name in SKIP_TAGS:
        return []

    # Headings
    if tag_name in ("h1", "h2", "h3", "h4", "h5", "h6"):
        level = int(tag_name[1])
        text = element.get_text(strip=True)
        if text:
            return ["", f"{'#' * level} {text}", ""]
        return []

    # Paragraph
    if tag_name == "p":
        children_md = _convert_children(element, slug, base_url)
        text = " ".join(children_md).strip()
        if text:
            return ["", text, ""]
        return []

    # Bold
    if tag_name in ("strong", "b"):
        text = _inline_children(element, slug, base_url)
        return [f"**{text}**"] if text else []

    # Italic
    if tag_name in ("em", "i"):
        text = _inline_children(element, slug, base_url)
        return [f"*{text}*"] if text else []

    # Links
    if tag_name == "a":
        href = element.get("href", "")
        # If the link wraps an image, recurse into children
        if element.find("img"):
            return _convert_children(element, slug, base_url)
        text = _inline_children(element, slug, base_url)
        if not text:
            return []
        if href:
            href = _resolve_url(href, base_url)
            return [f"[{text}]({href})"]
        return [text]

    # Images
    if tag_name == "img":
        src = element.get("src") or element.get("data-src") or ""
        alt = element.get("alt", "").strip()
        if src:
            src = _resolve_url(src, base_url)
            local_name = _download_image(src, slug)
            if local_name:
                return [f"![{alt}](../images/{local_name})"]
        return []

    # Picture: find inner img or first source
    if tag_name == "picture":
        img = element.find("img")
        if img:
            return _convert_element(img, slug, base_url)
        source = element.find("source")
        if source and source.get("srcset"):
            src = source["srcset"].split(",")[0].strip().split(" ")[0]
            src = _resolve_url(src, base_url)
            local_name = _download_image(src, slug)
            if local_name:
                return [f"![](../images/{local_name})"]
        return []

    # Figure
    if tag_name == "figure":
        result = []
        for child in element.children:
            if isinstance(child, Tag) and child.name == "figcaption":
                cap = child.get_text(strip=True)
                if cap:
                    result.append(f"*{cap}*")
            else:
                result.extend(_convert_element(child, slug, base_url))
        return result

    # Blockquote
    if tag_name == "blockquote":
        children_md = _convert_children(element, slug, base_url)
        text = " ".join(children_md).strip()
        if text:
            lines = text.split("\n")
            return [""] + [f"> {line}" for line in lines] + [""]
        return []

    # Pre / Code
    if tag_name == "pre":
        code = element.find("code")
        if code:
            lang = ""
            cls = code.get("class", [])
            for c in cls:
                if c.startswith("language-"):
                    lang = c[9:]
                    break
            return ["", f"```{lang}", code.get_text(), "```", ""]
        return ["", "```", element.get_text(), "```", ""]

    if tag_name == "code":
        return [f"`{element.get_text()}`"]

    # Lists
    if tag_name in ("ul", "ol"):
        result = [""]
        for i, li in enumerate(element.find_all("li", recursive=False)):
            prefix = "- " if tag_name == "ul" else f"{i + 1}. "
            li_text = _inline_children(li, slug, base_url)
            if li_text:
                result.append(f"{prefix}{li_text}")
        result.append("")
        return result

    # BR
    if tag_name == "br":
        return ["\n"]

    # HR
    if tag_name == "hr":
        return ["", "---", ""]

    # Table
    if tag_name == "table":
        return _convert_table(element, slug, base_url)

    # Default: recurse children
    return _convert_children(element, slug, base_url)


def _convert_children(element: Tag, slug: str, base_url: str) -> list[str]:
    result = []
    for child in element.children:
        result.extend(_convert_element(child, slug, base_url))
    return result


def _inline_children(element: Tag, slug: str, base_url: str) -> str:
    parts = _convert_children(element, slug, base_url)
    return " ".join(p.strip() for p in parts if p.strip())


def _convert_table(table: Tag, slug: str, base_url: str) -> list[str]:
    rows = []
    for tr in table.find_all("tr"):
        cells = []
        for td in tr.find_all(["th", "td"]):
            cells.append(_inline_children(td, slug, base_url).replace("|", "\\|"))
        if cells:
            rows.append(cells)
    if not rows:
        return []
    # Normalize column count
    max_cols = max(len(r) for r in rows)
    for r in rows:
        while len(r) < max_cols:
            r.append("")
    result = [""]
    result.append("| " + " | ".join(rows[0]) + " |")
    result.append("| " + " | ".join(["---"] * max_cols) + " |")
    for row in rows[1:]:
        result.append("| " + " | ".join(row) + " |")
    result.append("")
    return result


def _resolve_url(url: str, base_url: str) -> str:
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return base_url.rstrip("/") + url
    if not url.startswith("http"):
        return urljoin(base_url + "/", url)
    return url


# ---------------------------------------------------------------------------
# Image downloader
# ---------------------------------------------------------------------------

def _download_image(src: str, slug: str) -> str | None:
    """Download image, return local filename or None."""
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Handle WordPress image proxies
    parsed = urlparse(src)
    effective_path = parsed.path
    if re.match(r"i[0-3]\.wp\.com", parsed.netloc):
        # Path after the proxy domain contains the original URL path
        effective_path = "/" + "/".join(parsed.path.split("/")[1:])  # strip proxy prefix

    # Determine extension
    path_for_ext = effective_path.split("?")[0]
    ext = os.path.splitext(path_for_ext)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp"):
        ext = ".jpg"  # default

    # Generate filename
    url_hash = hashlib.md5(src.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    filepath = IMAGES_DIR / filename

    if filepath.exists():
        return filename

    try:
        resp = SESSION.get(src, timeout=REQUEST_TIMEOUT, stream=True)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        return filename
    except Exception as exc:
        print(f"[warn] Image download failed: {src} — {exc}")
        return None


# ---------------------------------------------------------------------------
# Page scraper
# ---------------------------------------------------------------------------

def scrape_page(
    entry: dict,
    state: dict,
    existing_slugs: set,
    force: bool = False,
) -> dict | None:
    """
    Fetch and process a single page.
    Returns {slug, meta, content, content_hash} or None on skip/error.
    """
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq")

    # Generate slug
    slug = generate_slug(url)
    slug = _resolve_slug_collision(slug, existing_slugs)
    existing_slugs.add(slug)

    # Incremental check
    if not force:
        stored_lastmod = state.get("lastmod", {}).get(slug)
        slug_dir = BASE_DIR / slug
        if stored_lastmod and lastmod and stored_lastmod == lastmod and (slug_dir / "content.md").exists():
            print(f"[skip] {slug} — unchanged")
            return None

    # Rate limit
    time.sleep(REQUEST_DELAY)

    # Fetch
    try:
        resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[error] Failed to fetch {url}: {exc}")
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # Listing page detection (HTML-level)
    if _is_listing_html(soup, url):
        print(f"[skip] {slug} — listing page")
        return None

    # Extract content container (Drupal selectors first, then fallbacks)
    content_el = _find_content_element(soup)
    if content_el is None:
        print(f"[skip] {slug} — no content container found")
        return None

    # Remove noise
    for sel in NOISE_SELECTORS:
        for el in content_el.select(sel):
            el.decompose()

    # Convert to markdown
    md = html_to_markdown(content_el, slug, BASE_URL)

    # Word count check
    word_count = len(md.split())
    if word_count < 50:
        print(f"[skip] {slug} — too short ({word_count} words)")
        return None

    # Content dedup
    content_hash = hashlib.md5(md.encode()).hexdigest()
    existing_hash_owner = state.get("content_hashes", {}).get(content_hash)
    if existing_hash_owner and existing_hash_owner != slug:
        print(f"[dedup] {slug} — duplicate of {existing_hash_owner}")
        return None

    # Extract title
    title = _extract_title(soup)

    # Extract date
    publish_date = extract_date(soup, lastmod)

    # Extract brief
    brief = _extract_brief(soup)

    # Extract tags
    tags = extract_tags(soup)

    # Content type
    url_path = urlparse(url).path
    content_type, category = detect_content_type(url_path)

    # Paywall check
    truncated = _check_truncated(soup, word_count)

    # Build meta
    meta = {
        "title": title,
        "publish-date": publish_date,
        "change-frequency": changefreq,
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

    return {
        "slug": slug,
        "meta": meta,
        "content": md,
        "content_hash": content_hash,
        "lastmod": lastmod,
    }


def _find_content_element(soup: BeautifulSoup) -> Tag | None:
    """Find the main content container using Drupal-first, then generic selectors."""
    selectors = [
        # Drupal-specific (The New Arab is Drupal)
        "div.field--name-body",
        "article .node__content",
        "div.node__content",
        ".field--name-field-body",
        # News site patterns
        "div.article-body",
        "div.article__content",
        "div[class*='ArticleBody']",
        "div.body-content",
        "div.story-body",
        # Generic fallbacks
        "article",
        "main",
        "div.content",
        "div.post",
        "div#content",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return el
    return None


def _is_listing_html(soup: BeautifulSoup, url: str) -> bool:
    """Check HTML signals that indicate a listing page."""
    # og:type check
    og_type = soup.find("meta", property="og:type")
    if og_type and og_type.get("content", "").lower() in ("website", "blog"):
        # Only indicative, not conclusive — check further
        pass

    # Many article cards with little prose = listing
    article_cards = soup.select("article.node--view-mode-teaser, .views-row, .article-card")
    if len(article_cards) > 3:
        return True

    # Title contains listing keywords
    title_el = soup.find("title")
    if title_el:
        title_text = title_el.get_text(strip=True).lower()
        for kw in ("archive", "all posts", "page 2", "category:", "tag:"):
            if kw in title_text:
                return True

    return False


def _extract_title(soup: BeautifulSoup) -> str:
    """Extract article title."""
    # Prefer h1
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
        return title_el.get_text(strip=True)

    return "Untitled"


def _extract_brief(soup: BeautifulSoup) -> str | None:
    """Extract description / subtitle."""
    for attr_set in [
        {"property": "og:description"},
        {"name": "description"},
        {"name": "twitter:description"},
    ]:
        meta = soup.find("meta", attrs=attr_set)
        if meta and meta.get("content"):
            return meta["content"].strip()
    return None


def _check_truncated(soup: BeautifulSoup, word_count: int) -> bool:
    """Check for paywall / truncation indicators."""
    paywall_selectors = [
        "[class*='paywall']", "[class*='subscribe']", "[class*='premium']",
        "[class*='locked']", ".metered-content", ".truncated",
    ]
    for sel in paywall_selectors:
        if soup.select_one(sel):
            if word_count < 200:
                return True
    return False


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------

def replace_internal_links(base_dir: Path) -> int:
    """Replace internal links in all content.md files with local relative paths."""
    count = 0
    slug_dirs = {d.name for d in base_dir.iterdir() if d.is_dir() and d.name != "images"}
    content_files = list(base_dir.glob("*/content.md"))

    domain_pattern = re.compile(
        r"\[([^\]]+)\]\(https?://(?:www\.)?newarab\.com/([^)?\s#]+)[^)]*\)"
    )

    for md_file in content_files:
        text = md_file.read_text(encoding="utf-8")
        original = text

        def _replacer(m):
            link_text = m.group(1)
            path = m.group(2).rstrip("/")
            target_slug = path.split("/")[-1] if "/" in path else path
            target_slug = target_slug.lower()
            target_slug = re.sub(r"[^a-z0-9\-]", "-", target_slug)
            target_slug = re.sub(r"-{2,}", "-", target_slug).strip("-")
            if target_slug in slug_dirs:
                return f"[{link_text}](../{target_slug}/content.md)"
            return m.group(0)

        text = domain_pattern.sub(_replacer, text)

        if text != original:
            md_file.write_text(text, encoding="utf-8")
            count += 1

    return count


# ---------------------------------------------------------------------------
# Write output
# ---------------------------------------------------------------------------

def write_article(result: dict) -> None:
    """Write meta.yaml and content.md for a scraped article."""
    slug_dir = BASE_DIR / result["slug"]
    slug_dir.mkdir(parents=True, exist_ok=True)

    # meta.yaml
    meta_path = slug_dir / "meta.yaml"
    with open(meta_path, "w", encoding="utf-8") as f:
        yaml.dump(
            result["meta"],
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

    # content.md
    content_path = slug_dir / "content.md"
    content_path.write_text(result["content"], encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape www.newarab.com via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything, ignore state")
    parser.add_argument("--slug", type=str, help="Fetch only this slug")
    args = parser.parse_args()

    # Ensure output dirs
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    state = load_state()
    if args.force:
        state = {"lastmod": {}, "content_hashes": {}}

    # Fetch sitemap
    print("=" * 60)
    print("Scraping www.newarab.com")
    print("=" * 60)

    entries = fetch_sitemap_index(SITEMAP_INDEX_URL)
    print(f"[sitemap] Total URLs found: {len(entries)}")

    # Filter entries
    filtered = []
    for entry in entries:
        url = entry["loc"]
        if is_listing_page(url):
            continue
        # Only process URLs from this domain
        parsed = urlparse(url)
        if parsed.netloc and DOMAIN not in parsed.netloc:
            continue
        filtered.append(entry)

    print(f"[sitemap] After filtering listings/skips: {len(filtered)} article candidates")

    # Single slug mode
    if args.slug:
        filtered = [e for e in filtered if args.slug in e["loc"]]
        if not filtered:
            print(f"[error] No sitemap entry matches slug '{args.slug}'")
            return
        print(f"[slug] Matched {len(filtered)} entries for '{args.slug}'")

    # Track stats
    stats = {"fetched": 0, "skipped": 0, "errors": 0, "dedup": 0, "images": 0}
    existing_slugs = set()

    # Pre-populate existing slugs
    if BASE_DIR.exists():
        for d in BASE_DIR.iterdir():
            if d.is_dir() and d.name != "images":
                existing_slugs.add(d.name)

    # Process with thread pool
    results = []

    def _process(entry):
        return scrape_page(entry, state, existing_slugs, force=args.force)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_process, entry): entry for entry in filtered}
        for future in as_completed(futures):
            entry = futures[future]
            try:
                result = future.result()
                if result is None:
                    stats["skipped"] += 1
                    continue
                results.append(result)
            except Exception as exc:
                print(f"[error] {entry['loc']}: {exc}")
                stats["errors"] += 1

    # Write results
    for result in results:
        write_article(result)
        # Update state
        state.setdefault("lastmod", {})[result["slug"]] = result["lastmod"]
        state.setdefault("content_hashes", {})[result["content_hash"]] = result["slug"]
        stats["fetched"] += 1
        print(f"[saved] {result['slug']}")

    # Save state
    save_state(state)

    # Internal link replacement
    print("[post] Replacing internal links...")
    link_count = replace_internal_links(BASE_DIR)
    print(f"[post] Updated {link_count} files with local links")

    # Count images
    if IMAGES_DIR.exists():
        stats["images"] = len(list(IMAGES_DIR.iterdir()))

    # Report
    print()
    print("=" * 60)
    print("Scrape complete")
    print(f"  Fetched:    {stats['fetched']}")
    print(f"  Skipped:    {stats['skipped']}")
    print(f"  Duplicates: {stats['dedup']}")
    print(f"  Errors:     {stats['errors']}")
    print(f"  Images:     {stats['images']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
