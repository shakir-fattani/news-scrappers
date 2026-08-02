#!/usr/bin/env python3
"""
Sitemap-based scraper for saudigazette.com.sa

Fetches all article URLs from sitemap, downloads content as markdown,
extracts metadata to YAML, and downloads images locally.

Usage:
    python3 scrape_saudigazette.py              # incremental run
    python3 scrape_saudigazette.py --force      # re-fetch everything
    python3 scrape_saudigazette.py --slug X     # fetch only slug X
"""

# ── Dependency check ──────────────────────────────────────────────────
_MISSING = []
try:
    import requests
except ImportError:
    _MISSING.append("requests")
try:
    from bs4 import BeautifulSoup
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
    print("Missing dependencies: " + ", ".join(_MISSING))
    print("Install with:\n  pip3 install --user --break-system-packages " + " ".join(_MISSING))
    raise SystemExit(1)

# ── Standard library ─────────────────────────────────────────────────
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

# ── Constants ─────────────────────────────────────────────────────────
DOMAIN = "saudigazette.com.sa"
SITE_URL = f"https://{DOMAIN}"
SITEMAP_URLS = [
    f"{SITE_URL}/sitemap/main.xml",
    f"{SITE_URL}/sitemaps/sitemap_0.xml",
    f"{SITE_URL}/sitemap.xml",
]

BASE_DIR = Path(__file__).resolve().parent / "saudi_gazette"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

MAX_WORKERS = 5
REQUEST_DELAY = 1.0

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Saudi Gazette uses /article/NUMERIC_ID/SLUG pattern
# Also has sections like /saudi-arabia/, /world/, /business/, /opinion/, /sports/, /life/
CONTENT_PATH_PATTERNS = [
    "/article/", "/news/", "/articles/", "/press-release/", "/blogs/",
    "/insights/", "/market-insights/", "/latest-insights/", "/wealth-insights/",
    "/posts/", "/newsroom/", "/announcements/",
    "/opinion/", "/business/", "/lifestyle/", "/sports/",
    "/life/", "/saudi-arabia/", "/world/", "/region/",
    "/economy/", "/analysis/", "/topic/",
    "/technology/", "/entertainment/", "/health/",
    "/editorial/", "/columns/", "/features/",
]

SKIP_PATTERNS = {
    "/page/", "/tag/", "/category/", "/author/",
    "/search", "/login", "/register", "/account",
    "/about", "/contact", "/privacy", "/terms",
    "/feed", "/rss", "/api/", "/wp-json/",
    "/archive", "/print/",
}

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup",
    ".subscription-widget", ".subscribe",
    ".comments", ".comment-section", ".author-bio",
    ".disclaimer", ".cookie-banner",
    ".breadcrumb", ".breadcrumbs", ".pagination",
    ".ad", ".advertisement", "[class*='promo']",
    "[class*='banner']", "[class*='popup']", "[class*='modal']",
    ".tags-area", ".article-tags", ".post-tags",
    ".related-news", ".more-news", ".trending",
    ".follow-us", ".social-media",
    "script", "style", "noscript", "svg", "button", "iframe",
]

# Saudi Gazette content selectors (priority order)
CONTENT_SELECTORS = [
    "div.article-body",
    "div.article-content",
    "div.article_body",
    "div.entry-content",
    "div.post-content",
    "div.news-content",
    "div.content-area",
    "div.story-body",
    "div.article-text",
    "div.field--name-body",
    "article .post-content",
    "article",
    "main",
    "div.content",
    "div#content",
    "div.post",
]

# ── Session ───────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


# ── State management ──────────────────────────────────────────────────
def load_state():
    """Load incremental fetch state from disk."""
    if FETCH_STATE_FILE.exists():
        with open(FETCH_STATE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_state(state):
    """Persist fetch state to disk."""
    FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FETCH_STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)


# ── Sitemap fetching ──────────────────────────────────────────────────
def fetch_sitemap_entries():
    """Fetch and merge all sitemap entries. Returns list of dicts with loc, lastmod, changefreq."""
    entries = []
    sitemap_index_urls = []

    for url in SITEMAP_URLS:
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
        except Exception:
            continue

        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        # Check if sitemap index
        if root.tag == "{http://www.sitemaps.org/schemas/sitemap/0.9}sitemapindex" or "sitemapindex" in root.tag:
            for sitemap_el in root.findall(".//sm:sitemap", ns) or root.findall("sitemap"):
                loc_el = sitemap_el.find("sm:loc", ns)
                if loc_el is None:
                    loc_el = sitemap_el.find("loc")
                if loc_el is not None and loc_el.text:
                    sitemap_index_urls.append(loc_el.text.strip())
        else:
            entries.extend(_parse_urlset(root, ns))
        break  # Use first working sitemap

    # Fetch child sitemaps from index
    for child_url in sitemap_index_urls:
        try:
            time.sleep(REQUEST_DELAY)
            resp = SESSION.get(child_url, timeout=30)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            entries.extend(_parse_urlset(root, ns))
        except Exception as exc:
            print(f"[warn] Failed to fetch child sitemap {child_url}: {exc}")

    return entries


def _parse_urlset(root, ns):
    """Parse a <urlset> element into a list of entry dicts."""
    entries = []
    url_elements = root.findall(".//sm:url", ns)
    if not url_elements:
        # Try without namespace
        url_elements = root.findall(".//url")
    for url_el in url_elements:
        loc_el = url_el.find("sm:loc", ns)
        if loc_el is None:
            loc_el = url_el.find("loc")
        lastmod_el = url_el.find("sm:lastmod", ns)
        if lastmod_el is None:
            lastmod_el = url_el.find("lastmod")
        changefreq_el = url_el.find("sm:changefreq", ns)
        if changefreq_el is None:
            changefreq_el = url_el.find("changefreq")

        if loc_el is not None and loc_el.text:
            entries.append({
                "loc": loc_el.text.strip(),
                "lastmod": (lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else None),
                "changefreq": (changefreq_el.text.strip() if changefreq_el is not None and changefreq_el.text else None),
            })
    return entries


# ── URL classification ────────────────────────────────────────────────
def is_article_url(url):
    """Return True if the URL looks like an article rather than a listing/utility page."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Skip utility / non-content patterns
    for skip in SKIP_PATTERNS:
        if skip in path.lower():
            return False

    # Skip bare domain / homepage
    if not path or path == "/":
        return False

    # Saudi Gazette uses /article/ID/slug pattern -- always an article
    if "/article/" in path:
        return True

    # Check known content patterns -- but only if there's a slug after the pattern
    path_lower = path.lower()
    for pattern in CONTENT_PATH_PATTERNS:
        if pattern in path_lower:
            # Ensure there is a slug segment after the pattern
            idx = path_lower.find(pattern)
            after = path[idx + len(pattern):].strip("/")
            if after and not re.match(r"^page/\d+$", after):
                return True

    # Fallback: if URL has 2+ path segments and last is text (not just a section), treat as article
    segments = [s for s in path.split("/") if s]
    if len(segments) >= 2:
        last = segments[-1]
        # Skip if last segment looks like a section index
        if re.match(r"^[a-z-]+$", last) and len(last) > 3:
            return True

    return False


# ── Listing detection from HTML ───────────────────────────────────────
def is_listing_page(soup):
    """Heuristic: detect listing/index pages by checking for many article cards but little prose."""
    # Count article cards
    cards = soup.select("article, .article-card, .news-item, h2 a, h3 a")
    # Count words in main content area
    main = soup.select_one("article, main, div.content, div#content")
    if main:
        text = main.get_text(separator=" ", strip=True)
        word_count = len(text.split())
    else:
        word_count = 0
        text = ""

    internal_links = len([a for a in soup.select("a[href]") if DOMAIN in (a.get("href", "") or "")])

    # Listing: many cards, few words, many links
    if len(cards) > 10 and word_count < 200:
        return True
    if word_count < 200 and internal_links > 10:
        return True

    # Check title for listing indicators
    title_el = soup.find("title")
    if title_el:
        title_text = title_el.get_text().lower()
        for indicator in ["archive", "all posts", "page 2", "category:", "tag:"]:
            if indicator in title_text:
                return True

    return False


# ── Slug generation ───────────────────────────────────────────────────
def generate_slug(url_path):
    """Extract a slug from the URL path.

    /article/638985/Headline-here  -> headline-here
    /saudi-arabia/some-news-slug   -> some-news-slug
    /news/2026/07/15/slug          -> slug
    """
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    if not segments:
        return "index"

    # Strip date-like segments (4-digit year, 1-2 digit month/day)
    filtered = []
    for seg in segments:
        if re.match(r"^\d{4}$", seg):
            continue
        if re.match(r"^\d{1,2}$", seg) and len(seg) <= 2:
            continue
        filtered.append(seg)

    if not filtered:
        filtered = segments  # Restore if everything was stripped

    # For /article/ID/slug pattern, prefer the text slug over the numeric ID
    if "article" in filtered:
        idx = filtered.index("article")
        remaining = filtered[idx + 1:]
        # Skip numeric ID, take last text segment
        text_segments = [s for s in remaining if not s.isdigit()]
        if text_segments:
            slug = text_segments[-1]
        elif remaining:
            slug = remaining[-1]
        else:
            slug = filtered[-1]
    else:
        # Use last meaningful segment
        # Skip segments that are known content-type names if there is something after
        slug = filtered[-1]

    # Normalize
    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")

    return slug or "untitled"


_SLUG_COUNTER = {}


def unique_slug(raw_slug):
    """Ensure slug uniqueness by appending -2, -3, etc. on collision."""
    if raw_slug not in _SLUG_COUNTER:
        _SLUG_COUNTER[raw_slug] = 1
        return raw_slug
    _SLUG_COUNTER[raw_slug] += 1
    return f"{raw_slug}-{_SLUG_COUNTER[raw_slug]}"


# ── Content type detection ────────────────────────────────────────────
def detect_content_type(url_path):
    """Detect content-type and category from URL path.

    /article/123/headline        -> content_type='article', category=None
    /saudi-arabia/some-news      -> content_type='saudi-arabia', category=None
    /news/world-news/slug        -> content_type='world-news', category='news'
    /opinion/editorial/slug      -> content_type='editorial', category='opinion'
    """
    path = url_path.lower().rstrip("/")
    segments = [s for s in path.split("/") if s]

    if not segments:
        return "general", None

    content_type = None
    category = None

    # Check for nested content type patterns
    for pattern in CONTENT_PATH_PATTERNS:
        pat = pattern.strip("/")
        if pat in segments:
            idx = segments.index(pat)
            # Check if there is a deeper known pattern
            for inner_pattern in CONTENT_PATH_PATTERNS:
                inner_pat = inner_pattern.strip("/")
                if inner_pat != pat and inner_pat in segments:
                    inner_idx = segments.index(inner_pat)
                    if inner_idx > idx:
                        content_type = inner_pat
                        category = pat
                        return content_type, category
            content_type = pat
            break

    if content_type is None:
        # Use first segment as content type
        content_type = segments[0] if segments else "general"

    return content_type, category


# ── Date extraction ───────────────────────────────────────────────────
def extract_date(soup, sitemap_lastmod=None, url_path="", response_headers=None):
    """Extract publish date using priority chain. Returns ISO date string or None."""

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

    # 4. Date-class elements
    for selector in [".date", ".article-date", ".publish-date", ".post-date",
                     "[class*='date']", "[class*='timestamp']", ".time"]:
        el = soup.select_one(selector)
        if el:
            text = el.get_text(strip=True)
            parsed = _parse_date_text(text)
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
        except (json.JSONDecodeError, TypeError):
            pass

    # 6. URL path date segments: /2026/07/15/
    date_match = re.search(r"/(\d{4})/(\d{1,2})/(\d{1,2})/", url_path)
    if date_match:
        y, m, d = date_match.groups()
        return f"{y}-{int(m):02d}-{int(d):02d}"

    # 7. Sitemap lastmod fallback
    if sitemap_lastmod:
        return _normalize_date(sitemap_lastmod)

    # 8. HTTP Last-Modified header
    if response_headers:
        last_mod = response_headers.get("Last-Modified")
        if last_mod:
            return _parse_date_text(last_mod)

    return None


def _normalize_date(date_str):
    """Normalize various date formats to YYYY-MM-DD."""
    if not date_str:
        return None
    # Handle ISO 8601: 2026-07-15T10:30:00+03:00
    date_str = date_str.strip()
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if match:
        return match.group(0)
    return _parse_date_text(date_str)


def _parse_date_text(text):
    """Try to parse free-form date text into YYYY-MM-DD."""
    if not text:
        return None
    text = text.strip()

    # ISO-like
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # DD/MM/YYYY or MM/DD/YYYY
    m = re.match(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})", text)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), m.group(3)
        if a > 12:
            return f"{y}-{b:02d}-{a:02d}"
        return f"{y}-{a:02d}-{b:02d}"

    # Month name formats: "July 15, 2026" or "15 July 2026"
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    m = re.search(
        r"(\d{1,2})\s+("
        + "|".join(months.keys())
        + r")\s+(\d{4})",
        text.lower(),
    )
    if m:
        d, mon, y = int(m.group(1)), months[m.group(2)], m.group(3)
        return f"{y}-{mon:02d}-{d:02d}"
    m = re.search(
        r"(" + "|".join(months.keys()) + r")\s+(\d{1,2}),?\s+(\d{4})",
        text.lower(),
    )
    if m:
        mon, d, y = months[m.group(1)], int(m.group(2)), m.group(3)
        return f"{y}-{mon:02d}-{d:02d}"

    return None


# ── Tag extraction ────────────────────────────────────────────────────
def extract_tags(soup):
    """Extract and deduplicate tags from multiple HTML sources."""
    tags = set()

    # 1. meta keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        for kw in meta_kw["content"].split(","):
            kw = kw.strip().lower()
            if kw and len(kw) < 80:
                tags.add(kw)

    # 2. article:tag meta (can appear multiple times)
    for meta in soup.find_all("meta", property="article:tag"):
        val = (meta.get("content") or "").strip().lower()
        if val:
            tags.add(val)

    # 3. JSON-LD keywords
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            if isinstance(data, dict):
                kws = data.get("keywords")
                if isinstance(kws, str):
                    for kw in kws.split(","):
                        kw = kw.strip().lower()
                        if kw:
                            tags.add(kw)
                elif isinstance(kws, list):
                    for kw in kws:
                        if isinstance(kw, str):
                            tags.add(kw.strip().lower())
        except (json.JSONDecodeError, TypeError):
            pass

    # 4. Visible tag links
    for selector in ['a[rel="tag"]', ".tags a", ".post-tags a",
                     ".article-tags a", "[class*='tag-link']",
                     ".cat-links a", ".entry-categories a",
                     ".keywords a", ".topics a"]:
        for a_el in soup.select(selector):
            text = a_el.get_text(strip=True).lower()
            if text and len(text) < 80:
                tags.add(text)

    return sorted(tags) if tags else []


# ── Image handling ────────────────────────────────────────────────────
def download_image(img_url, slug):
    """Download an image and return the local filename, or None on failure."""
    if not img_url or img_url.startswith("data:"):
        return None

    # Resolve relative URLs
    if img_url.startswith("//"):
        img_url = "https:" + img_url
    elif img_url.startswith("/"):
        img_url = SITE_URL + img_url

    # Handle WordPress proxy URLs (i0-i3.wp.com)
    wp_match = re.match(r"https?://i[0-3]\.wp\.com/(.+)", img_url)
    if wp_match:
        original_path = wp_match.group(1).split("?")[0]
        ext = _get_extension(original_path)
        fetch_url = img_url.split("?")[0]  # Strip resize params for fetching
    else:
        fetch_url = img_url.split("?")[0]
        ext = _get_extension(fetch_url)

    # Generate filename
    url_hash = hashlib.md5(img_url.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    filepath = IMAGES_DIR / filename

    if filepath.exists():
        return filename

    try:
        resp = SESSION.get(img_url, timeout=20, stream=True)
        if resp.status_code != 200:
            return None
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb") as fh:
            for chunk in resp.iter_content(8192):
                fh.write(chunk)
        return filename
    except Exception:
        return None


def _get_extension(url_or_path):
    """Extract file extension from a URL or path."""
    path = urlparse(url_or_path).path if "://" in url_or_path else url_or_path
    _, ext = os.path.splitext(path.split("?")[0])
    if ext.lower() in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp", ".ico"):
        return ext.lower()
    return ".jpg"  # default


# ── HTML to Markdown ──────────────────────────────────────────────────
def html_to_markdown(element, slug, depth=0):
    """Recursively convert a BeautifulSoup element to markdown."""
    if element is None:
        return ""

    from bs4 import NavigableString, Tag

    if isinstance(element, NavigableString):
        text = str(element)
        if not text.strip():
            return " " if text else ""
        return text

    if not isinstance(element, Tag):
        return ""

    tag = element.name.lower() if element.name else ""

    # Skip noise tags
    if tag in ("script", "style", "noscript", "svg", "button", "iframe",
               "nav", "form", "input", "select", "textarea"):
        return ""

    # Skip noise classes
    el_class = " ".join(element.get("class", []))
    el_id = element.get("id", "")
    noise_indicators = [
        "share", "social", "subscribe", "newsletter", "comment",
        "sidebar", "related", "recommended", "advertisement",
        "popup", "modal", "banner", "promo", "cookie",
        "breadcrumb", "pagination", "follow",
    ]
    for indicator in noise_indicators:
        if indicator in el_class.lower() or indicator in el_id.lower():
            return ""

    # Handle specific tags
    if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
        level = int(tag[1])
        inner = _children_to_md(element, slug, depth)
        inner = inner.strip()
        if inner:
            return f"\n\n{'#' * level} {inner}\n\n"
        return ""

    if tag == "p":
        inner = _children_to_md(element, slug, depth)
        inner = inner.strip()
        if inner:
            return f"\n\n{inner}\n\n"
        return ""

    if tag in ("strong", "b"):
        inner = _children_to_md(element, slug, depth)
        inner = inner.strip()
        if inner:
            return f"**{inner}**"
        return ""

    if tag in ("em", "i"):
        inner = _children_to_md(element, slug, depth)
        inner = inner.strip()
        if inner:
            return f"*{inner}*"
        return ""

    if tag == "a":
        href = element.get("href", "")
        # If wrapping an image, recurse into children
        if element.find("img"):
            return _children_to_md(element, slug, depth)
        inner = _children_to_md(element, slug, depth)
        inner = inner.strip()
        if inner and href:
            return f"[{inner}]({href})"
        return inner

    if tag == "img":
        src = element.get("src") or element.get("data-src") or ""
        alt = element.get("alt", "").strip()
        if not src:
            return ""
        filename = download_image(src, slug)
        if filename:
            return f"![{alt}](../images/{filename})"
        return f"![{alt}]({src})"

    if tag == "picture":
        img = element.find("img")
        if img:
            return html_to_markdown(img, slug, depth)
        source = element.find("source")
        if source and source.get("srcset"):
            src = source["srcset"].split(",")[0].strip().split(" ")[0]
            filename = download_image(src, slug)
            if filename:
                return f"![](../images/{filename})"
        return ""

    if tag == "figure":
        inner = _children_to_md(element, slug, depth)
        caption = element.find("figcaption")
        result = inner.strip()
        if caption:
            cap_text = caption.get_text(strip=True)
            if cap_text:
                result += f"\n*{cap_text}*"
        return f"\n\n{result}\n\n" if result else ""

    if tag == "blockquote":
        inner = _children_to_md(element, slug, depth)
        lines = inner.strip().split("\n")
        quoted = "\n".join(f"> {line}" for line in lines)
        return f"\n\n{quoted}\n\n"

    if tag == "pre":
        code_el = element.find("code")
        if code_el:
            lang_class = " ".join(code_el.get("class", []))
            lang = ""
            lang_match = re.search(r"language-(\w+)", lang_class)
            if lang_match:
                lang = lang_match.group(1)
            code_text = code_el.get_text()
            return f"\n\n```{lang}\n{code_text}\n```\n\n"
        return f"\n\n```\n{element.get_text()}\n```\n\n"

    if tag == "code" and (not element.parent or element.parent.name != "pre"):
        return f"`{element.get_text()}`"

    if tag in ("ul", "ol"):
        items = []
        for i, li in enumerate(element.find_all("li", recursive=False)):
            inner = _children_to_md(li, slug, depth + 1).strip()
            if tag == "ol":
                items.append(f"{i + 1}. {inner}")
            else:
                items.append(f"- {inner}")
        return "\n\n" + "\n".join(items) + "\n\n"

    if tag == "li":
        return _children_to_md(element, slug, depth)

    if tag == "br":
        return "\n"

    if tag == "hr":
        return "\n\n---\n\n"

    if tag == "table":
        return _table_to_md(element, slug, depth)

    # Default: recurse children
    return _children_to_md(element, slug, depth)


def _children_to_md(element, slug, depth):
    """Convert all children of an element to markdown."""
    from bs4 import NavigableString, Tag
    parts = []
    for child in element.children:
        if isinstance(child, NavigableString):
            parts.append(str(child))
        elif isinstance(child, Tag):
            parts.append(html_to_markdown(child, slug, depth))
    return "".join(parts)


def _table_to_md(table, slug, depth):
    """Convert an HTML table to markdown table."""
    rows = []
    for tr in table.find_all("tr"):
        cells = []
        for cell in tr.find_all(["th", "td"]):
            text = cell.get_text(separator=" ", strip=True)
            text = text.replace("|", "\\|")
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

    md = "\n\n"
    md += "| " + " | ".join(rows[0]) + " |\n"
    md += "| " + " | ".join(["---"] * max_cols) + " |\n"
    for row in rows[1:]:
        md += "| " + " | ".join(row) + " |\n"
    md += "\n"
    return md


def clean_markdown(md):
    """Collapse excessive blank lines and trim whitespace."""
    md = re.sub(r"\n{3,}", "\n\n", md)
    md = md.strip()
    return md


# ── Content extraction ────────────────────────────────────────────────
def find_content_container(soup):
    """Find the main article content container using priority selectors."""
    for selector in CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            # Verify it has substantial text
            text = el.get_text(separator=" ", strip=True)
            if len(text.split()) > 50:
                return el
    # Last resort: body
    return soup.find("body")


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
        text = title_el.get_text(strip=True)
        # Strip site name suffix
        for sep in [" | ", " - ", " :: ", " \u2013 ", " \u2014 "]:
            if sep in text:
                text = text.split(sep)[0].strip()
        return text
    return "Untitled"


def extract_brief(soup):
    """Extract short brief / description."""
    og_desc = soup.find("meta", property="og:description")
    if og_desc and og_desc.get("content"):
        return og_desc["content"].strip()[:300]
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()[:300]
    return ""


# ── Per-article scraping ──────────────────────────────────────────────
def scrape_article(entry, state, force=False):
    """Scrape a single article. Returns (slug, success, reason)."""
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq")
    parsed = urlparse(url)

    raw_slug = generate_slug(parsed.path)
    slug = unique_slug(raw_slug)

    slug_dir = BASE_DIR / slug

    # Incremental check
    if not force:
        stored = state.get(slug, {})
        stored_lastmod = stored.get("lastmod") if isinstance(stored, dict) else stored
        if stored_lastmod and stored_lastmod == lastmod and (slug_dir / "content.md").exists():
            return slug, False, "skip"

    time.sleep(REQUEST_DELAY)

    try:
        resp = SESSION.get(url, timeout=30)
        if resp.status_code != 200:
            return slug, False, f"http-{resp.status_code}"
    except Exception as exc:
        return slug, False, f"error: {exc}"

    soup = BeautifulSoup(resp.text, "lxml")

    # Listing page check
    if is_listing_page(soup):
        return slug, False, "listing"

    # Remove noise elements
    for selector in NOISE_SELECTORS:
        try:
            for el in soup.select(selector):
                el.decompose()
        except Exception:
            pass

    # Extract content
    container = find_content_container(soup)
    if container is None:
        return slug, False, "no-content"

    md_content = html_to_markdown(container, slug)
    md_content = clean_markdown(md_content)

    # Word count check
    word_count = len(md_content.split())
    if word_count < 50:
        return slug, False, "too-short"

    # Content dedup
    content_hash = hashlib.md5(md_content.encode("utf-8")).hexdigest()
    for existing_slug, existing_data in state.items():
        if isinstance(existing_data, dict) and existing_data.get("content_hash") == content_hash:
            print(f"[dedup] {slug} -- duplicate of {existing_slug}")
            return slug, False, "dedup"

    # Extract metadata
    title = extract_title(soup)
    brief = extract_brief(soup)
    publish_date = extract_date(soup, sitemap_lastmod=lastmod,
                                url_path=parsed.path,
                                response_headers=dict(resp.headers))
    tags = extract_tags(soup)
    content_type, category = detect_content_type(parsed.path)

    # Truncation detection
    truncated = False
    paywall_indicators = [".paywall", ".subscribe-to-read", ".premium-content",
                          "[class*='paywall']", "[class*='subscribe']"]
    original_soup = BeautifulSoup(resp.text, "lxml")
    for sel in paywall_indicators:
        try:
            if original_soup.select_one(sel):
                truncated = True
                break
        except Exception:
            pass
    if word_count < 100 and truncated:
        truncated = True

    # Write output
    slug_dir.mkdir(parents=True, exist_ok=True)

    # meta.yaml
    meta = {
        "title": title,
        "publish-date": publish_date,
        "change-frequency": changefreq or "unknown",
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

    with open(slug_dir / "meta.yaml", "w", encoding="utf-8") as fh:
        yaml.dump(meta, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)

    # content.md
    with open(slug_dir / "content.md", "w", encoding="utf-8") as fh:
        fh.write(md_content)

    # Update state
    state[slug] = {
        "lastmod": lastmod,
        "content_hash": content_hash,
        "source_url": url,
    }

    return slug, True, "ok"


# ── Internal link replacement ─────────────────────────────────────────
def replace_internal_links(state):
    """After scraping, replace internal links in content.md with local relative paths."""
    # Build mapping: URL path -> slug
    url_to_slug = {}
    for slug, data in state.items():
        if isinstance(data, dict) and "source_url" in data:
            parsed = urlparse(data["source_url"])
            path = parsed.path.rstrip("/")
            url_to_slug[path] = slug

    # Scan all content.md files
    for slug_dir in BASE_DIR.iterdir():
        if not slug_dir.is_dir() or slug_dir.name in ("images",):
            continue
        content_file = slug_dir / "content.md"
        if not content_file.exists():
            continue

        content = content_file.read_text(encoding="utf-8")
        modified = False

        # Find markdown links pointing to the same domain
        def replace_link(match):
            nonlocal modified
            text = match.group(1)
            href = match.group(2)
            parsed = urlparse(href)

            # Only replace internal links
            if parsed.hostname and DOMAIN not in (parsed.hostname or ""):
                return match.group(0)

            # Strip query params and fragments
            clean_path = parsed.path.rstrip("/")
            if not clean_path:
                return match.group(0)

            # Try to find the target slug
            target_slug = url_to_slug.get(clean_path)
            if target_slug and (BASE_DIR / target_slug / "content.md").exists():
                modified = True
                return f"[{text}](../{target_slug}/content.md)"
            return match.group(0)

        new_content = re.sub(r"\[([^\]]*)\]\(([^)]+)\)", replace_link, content)

        if modified:
            content_file.write_text(new_content, encoding="utf-8")


# ── Main ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description=f"Scrape {DOMAIN} via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything ignoring state")
    parser.add_argument("--slug", type=str, help="Fetch only this slug")
    args = parser.parse_args()

    # Ensure output dirs
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    state = {} if args.force else load_state()

    # Fetch sitemap
    print(f"Fetching sitemap for {DOMAIN}...")
    entries = fetch_sitemap_entries()
    print(f"Found {len(entries)} URLs in sitemap")

    # Filter to article URLs
    article_entries = [e for e in entries if is_article_url(e["loc"])]
    print(f"Filtered to {len(article_entries)} article URLs")

    # Single slug mode
    if args.slug:
        matches = [e for e in article_entries if args.slug in e["loc"]]
        if not matches:
            # Also check raw entries (maybe filtered out)
            matches = [e for e in entries if args.slug in e["loc"]]
        if not matches:
            print(f"[error] No sitemap entry matching slug '{args.slug}'")
            return
        article_entries = matches
        print(f"Single-slug mode: {len(matches)} match(es)")

    # Scrape with thread pool
    fetched = 0
    skipped = 0
    failed = 0
    deduped = 0

    def _worker(entry):
        return scrape_article(entry, state, force=args.force)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_worker, entry): entry for entry in article_entries}
        for future in as_completed(futures):
            try:
                slug, success, reason = future.result()
                if success:
                    fetched += 1
                    print(f"[ok] {slug}")
                elif reason == "skip":
                    skipped += 1
                    print(f"[skip] {slug} -- unchanged")
                elif reason == "dedup":
                    deduped += 1
                elif reason == "listing":
                    skipped += 1
                elif reason == "too-short":
                    skipped += 1
                    print(f"[skip] {slug} -- too short")
                else:
                    failed += 1
                    print(f"[fail] {slug} -- {reason}")
            except Exception as exc:
                failed += 1
                print(f"[error] {exc}")

    # Save state
    save_state(state)

    # Post-processing: internal link replacement
    print("Replacing internal links...")
    replace_internal_links(state)

    # Summary
    print(f"\nDone! Fetched: {fetched}, Skipped: {skipped}, "
          f"Deduped: {deduped}, Failed: {failed}")
    print(f"Output: {BASE_DIR}")


if __name__ == "__main__":
    main()
