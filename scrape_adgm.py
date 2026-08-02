#!/usr/bin/env python3
"""
Sitemap-based scraper for www.adgm.com (Abu Dhabi Global Market).
Fetches all content pages from the sitemap, extracts metadata + markdown content,
and downloads images. Supports incremental runs via .fetch-state.json.

Usage:
    python3 scrape_adgm.py              # incremental run
    python3 scrape_adgm.py --force      # re-fetch everything
    python3 scrape_adgm.py --slug X     # fetch only slug X
"""

# --- Dependency check ---
_MISSING = []
for _pkg, _imp in [("requests", "requests"), ("beautifulsoup4", "bs4"), ("pyyaml", "yaml"), ("lxml", "lxml")]:
    try:
        __import__(_imp)
    except ImportError:
        _MISSING.append(_pkg)
if _MISSING:
    print(f"Missing packages: {', '.join(_MISSING)}")
    print(f"Install with:  pip3 install --user --break-system-packages {' '.join(_MISSING)}")
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
from urllib.parse import urljoin, urlparse, unquote

import requests
import yaml
from bs4 import BeautifulSoup, NavigableString, Tag

# --- Constants ---
DOMAIN = "www.adgm.com"
SITEMAP_URL = "https://www.adgm.com/sitemap.xml"
BASE_DIR = Path(__file__).resolve().parent / "adgm"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"
MAX_WORKERS = 5
REQUEST_DELAY = 1.0
MIN_ARTICLE_WORDS = 200

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
    # ADGM-specific patterns
    "/media-centre/", "/media-center/",
    "/doing-business/", "/registration-authority/",
    "/financial-services-regulatory-authority/",
    "/operating-in-adgm/", "/setting-up/",
    "/public-consultations/", "/guidance/",
    "/events/", "/careers/",
    "/documents/", "/legal-framework/",
    "/fsra/", "/ra/", "/adgm-courts/",
]

SKIP_PATTERNS = {
    "/page/", "/tag/", "/category/", "/author/", "/search",
    "/archive", "/login", "/register", "/sitemap", "/feed",
    "/wp-json/", "/wp-admin/", "/cdn-cgi/",
}

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside", ".sidebar", ".related-articles",
    ".recommended", ".social-share", ".share-buttons", ".newsletter-signup",
    ".subscription-widget", ".comments", ".comment-section", ".author-bio",
    ".disclaimer", ".cookie-banner", ".breadcrumb", ".pagination",
    ".ad", ".advertisement", "[class*='promo']", "[class*='banner']",
    "[class*='popup']", "[class*='modal']", ".nav-tabs", ".tab-content",
    "#header", "#footer", "#sidebar", ".mega-menu", ".top-bar",
]

SKIP_TAGS = {"script", "style", "noscript", "svg", "button", "iframe", "form", "input", "select", "textarea"}

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


# --- Session ---
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


# --- Sitemap Parsing ---
def fetch_sitemap(url):
    """Fetch and parse a sitemap, handling sitemapindex recursion."""
    entries = []
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[error] Failed to fetch sitemap {url}: {e}")
        return entries

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        print(f"[error] Failed to parse XML from {url}: {e}")
        return entries

    tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

    if tag == "sitemapindex":
        for sitemap_el in root.findall("sm:sitemap", NS):
            loc_el = sitemap_el.find("sm:loc", NS)
            if loc_el is not None and loc_el.text:
                child_url = loc_el.text.strip()
                print(f"[sitemap] Fetching child sitemap: {child_url}")
                entries.extend(fetch_sitemap(child_url))
                time.sleep(0.5)
    elif tag == "urlset":
        for url_el in root.findall("sm:url", NS):
            loc_el = url_el.find("sm:loc", NS)
            lastmod_el = url_el.find("sm:lastmod", NS)
            changefreq_el = url_el.find("sm:changefreq", NS)
            if loc_el is not None and loc_el.text:
                entry = {
                    "loc": loc_el.text.strip(),
                    "lastmod": lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else None,
                    "changefreq": changefreq_el.text.strip() if changefreq_el is not None and changefreq_el.text else None,
                }
                entries.append(entry)
    else:
        # Try without namespace
        for sitemap_el in root.findall("sitemap"):
            loc_el = sitemap_el.find("loc")
            if loc_el is not None and loc_el.text:
                entries.extend(fetch_sitemap(loc_el.text.strip()))
                time.sleep(0.5)
        for url_el in root.findall("url"):
            loc_el = url_el.find("loc")
            lastmod_el = url_el.find("lastmod")
            changefreq_el = url_el.find("changefreq")
            if loc_el is not None and loc_el.text:
                entry = {
                    "loc": loc_el.text.strip(),
                    "lastmod": lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else None,
                    "changefreq": changefreq_el.text.strip() if changefreq_el is not None and changefreq_el.text else None,
                }
                entries.append(entry)

    return entries


# --- URL Analysis ---
def should_skip_url(url):
    """Check if URL should be skipped."""
    parsed = urlparse(url)
    path = parsed.path.lower()
    for pattern in SKIP_PATTERNS:
        if pattern in path:
            return True
    return False


def is_listing_page(url, soup, text_content):
    """Determine if a page is a listing/index rather than an article."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    for pattern in CONTENT_PATH_PATTERNS:
        clean = pattern.rstrip("/")
        if path.lower() == clean or path.lower().endswith(clean):
            segments_after = path.lower().split(clean)[-1].strip("/")
            if not segments_after:
                return True

    if re.search(r"/page/\d+", path) or re.search(r"[?&]page=\d+", url):
        return True

    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        title_lower = title_tag.string.lower()
        if any(x in title_lower for x in ["archive", "all posts", "page 2", "category:"]):
            return True

    word_count = len(text_content.split())
    if word_count < MIN_ARTICLE_WORDS:
        links = soup.select("a[href]")
        if len(links) > 10:
            return True

    return False


def detect_content_type(url_path):
    """Extract content-type and category from URL path."""
    path = url_path.lower().rstrip("/")
    content_type = None
    category = None

    sorted_patterns = sorted(CONTENT_PATH_PATTERNS, key=len, reverse=True)
    for pattern in sorted_patterns:
        clean = pattern.strip("/").lower()
        if f"/{clean}/" in path or path.endswith(f"/{clean}"):
            parts = clean.split("/")
            if len(parts) > 1:
                content_type = parts[-1]
                category = parts[0]
            else:
                content_type = parts[0]
            break

    if not content_type:
        segments = [s for s in path.split("/") if s and not re.match(r"^\d{4}$", s)]
        if len(segments) >= 2:
            content_type = segments[-2] if len(segments[-1]) > 3 else segments[-1]
        elif segments:
            content_type = segments[0]

    return content_type or "page", category


def generate_slug(url_path):
    """Generate a slug from the URL path."""
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]

    meaningful = []
    for seg in segments:
        if re.match(r"^\d{4}$", seg):
            continue
        if re.match(r"^\d{2}$", seg):
            continue
        meaningful.append(seg)

    if not meaningful:
        return hashlib.md5(url_path.encode()).hexdigest()[:12]

    slug = meaningful[-1]
    slug = re.sub(r"\.(html?|aspx?|php|jsp)$", "", slug, flags=re.IGNORECASE)
    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or hashlib.md5(url_path.encode()).hexdigest()[:12]


# --- Date Extraction ---
def extract_date(soup, sitemap_lastmod=None):
    """Extract publish date using priority chain."""
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        return _normalize_date(meta["content"])

    for name in ("date", "publish-date", "publish_date", "pubdate"):
        meta = soup.find("meta", attrs={"name": name})
        if meta and meta.get("content"):
            return _normalize_date(meta["content"])

    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        return _normalize_date(time_el["datetime"])

    for selector in [".date", "[class*='date']", "[class*='timestamp']", ".post-date"]:
        el = soup.select_one(selector)
        if el and el.get_text(strip=True):
            parsed = _try_parse_date_text(el.get_text(strip=True))
            if parsed:
                return parsed

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                data = data[0]
            if isinstance(data, dict):
                dp = data.get("datePublished")
                if dp:
                    return _normalize_date(dp)
        except (json.JSONDecodeError, TypeError, IndexError):
            pass

    url_date = _extract_date_from_url(soup)
    if url_date:
        return url_date

    if sitemap_lastmod:
        return _normalize_date(sitemap_lastmod)

    return None


def _normalize_date(date_str):
    """Normalize a date string to YYYY-MM-DD."""
    if not date_str:
        return None
    date_str = date_str.strip()
    match = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", date_str)
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if match:
        return f"{match.group(3)}-{int(match.group(1)):02d}-{int(match.group(2)):02d}"
    return date_str[:10] if len(date_str) >= 10 else date_str


def _try_parse_date_text(text):
    """Try to parse a human-readable date string."""
    months = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12",
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "jun": "06", "jul": "07", "aug": "08", "sep": "09",
        "oct": "10", "nov": "11", "dec": "12",
    }
    text = text.strip().lower()
    match = re.search(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", text)
    if match:
        day, month_name, year = match.groups()
        month = months.get(month_name)
        if month:
            return f"{year}-{month}-{int(day):02d}"
    match = re.search(r"([a-z]+)\s+(\d{1,2}),?\s+(\d{4})", text)
    if match:
        month_name, day, year = match.groups()
        month = months.get(month_name)
        if month:
            return f"{year}-{month}-{int(day):02d}"
    return None


def _extract_date_from_url(soup):
    """Try to extract date from canonical or og:url."""
    url = None
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"):
        url = canonical["href"]
    if not url:
        og_url = soup.find("meta", property="og:url")
        if og_url and og_url.get("content"):
            url = og_url["content"]
    if url:
        match = re.search(r"/(\d{4})/(\d{1,2})/(\d{1,2})/", url)
        if match:
            return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        match = re.search(r"/(\d{4})-(\d{1,2})/", url)
        if match:
            return f"{match.group(1)}-{int(match.group(2)):02d}-01"
    return None


# --- Tag Extraction ---
def extract_tags(soup):
    """Extract tags from multiple sources."""
    tags = set()

    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        for t in meta_kw["content"].split(","):
            t = t.strip().lower()
            if t and len(t) < 60:
                tags.add(t)

    for meta in soup.find_all("meta", property="article:tag"):
        if meta.get("content"):
            tags.add(meta["content"].strip().lower())

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                data = data[0]
            if isinstance(data, dict):
                kw = data.get("keywords")
                if isinstance(kw, list):
                    for k in kw:
                        tags.add(str(k).strip().lower())
                elif isinstance(kw, str):
                    for k in kw.split(","):
                        tags.add(k.strip().lower())
        except (json.JSONDecodeError, TypeError, IndexError):
            pass

    for selector in ['a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a", '[class*="tag-link"]']:
        for el in soup.select(selector):
            t = el.get_text(strip=True).lower()
            if t and len(t) < 60:
                tags.add(t)

    return sorted(tags - {""})


# --- Image Handling ---
def download_image(img_url, slug):
    """Download an image and return the local filename."""
    if not img_url or img_url.startswith("data:"):
        return None

    original_path = img_url
    wp_match = re.match(r"https?://i[0-3]\.wp\.com/(.+)", img_url)
    if wp_match:
        original_path = "https://" + wp_match.group(1).split("?")[0]

    parsed = urlparse(original_path.split("?")[0])
    ext = Path(parsed.path).suffix.lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp", ".ico"):
        ext = ".jpg"

    url_hash = hashlib.md5(img_url.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    filepath = IMAGES_DIR / filename

    if filepath.exists():
        return filename

    try:
        resp = session.get(img_url, timeout=20, stream=True)
        resp.raise_for_status()
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        return filename
    except requests.RequestException:
        return None


# --- HTML to Markdown ---
def html_to_markdown(element, slug, depth=0):
    """Recursively convert an HTML element to markdown."""
    if isinstance(element, NavigableString):
        text = str(element)
        if not text.strip():
            return " " if text else ""
        return text

    if not isinstance(element, Tag):
        return ""

    tag = element.name.lower() if element.name else ""

    if tag in SKIP_TAGS:
        return ""

    el_classes = " ".join(element.get("class", []))
    el_id = element.get("id", "")
    noise_indicators = [
        "share", "social", "comment", "subscribe", "newsletter", "related",
        "sidebar", "breadcrumb", "pagination", "cookie", "popup", "modal",
        "nav", "menu", "footer", "header", "ad-", "promo", "banner",
    ]
    for indicator in noise_indicators:
        if indicator in el_classes.lower() or indicator in el_id.lower():
            return ""

    children_md = ""
    for child in element.children:
        children_md += html_to_markdown(child, slug, depth + 1)

    if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
        level = int(tag[1])
        text = children_md.strip()
        if text:
            return f"\n\n{'#' * level} {text}\n\n"
        return ""

    if tag == "p":
        text = children_md.strip()
        if text:
            return f"\n\n{text}\n\n"
        return ""

    if tag in ("strong", "b"):
        text = children_md.strip()
        return f"**{text}**" if text else ""

    if tag in ("em", "i"):
        text = children_md.strip()
        return f"*{text}*" if text else ""

    if tag == "a":
        href = element.get("href", "")
        if element.find("img"):
            return children_md
        text = children_md.strip()
        if text and href:
            return f"[{text}]({href})"
        return text

    if tag == "img":
        src = element.get("src") or element.get("data-src") or ""
        alt = element.get("alt", "").strip()
        if src:
            if not src.startswith("http"):
                src = urljoin(f"https://{DOMAIN}", src)
            local_file = download_image(src, slug)
            if local_file:
                return f"![{alt}](../images/{local_file})"
        return ""

    if tag == "picture":
        img = element.find("img")
        if img:
            return html_to_markdown(img, slug, depth)
        source = element.find("source")
        if source and source.get("srcset"):
            src = source["srcset"].split(",")[0].strip().split(" ")[0]
            local_file = download_image(src, slug)
            if local_file:
                return f"![](../images/{local_file})"
        return ""

    if tag == "figure":
        result = ""
        for child in element.children:
            if isinstance(child, Tag) and child.name == "figcaption":
                caption = child.get_text(strip=True)
                if caption:
                    result += f"\n*{caption}*\n"
            else:
                result += html_to_markdown(child, slug, depth)
        return result

    if tag == "blockquote":
        text = children_md.strip()
        if text:
            lines = text.split("\n")
            quoted = "\n".join(f"> {line}" for line in lines)
            return f"\n\n{quoted}\n\n"
        return ""

    if tag == "pre":
        code_el = element.find("code")
        if code_el:
            lang = ""
            code_classes = code_el.get("class", [])
            for cls in code_classes:
                if cls.startswith("language-"):
                    lang = cls.replace("language-", "")
                    break
            code_text = code_el.get_text()
            return f"\n\n```{lang}\n{code_text}\n```\n\n"
        return f"\n\n```\n{element.get_text()}\n```\n\n"

    if tag == "code" and depth > 0:
        return f"`{element.get_text()}`"

    if tag in ("ul", "ol"):
        items = []
        for i, li in enumerate(element.find_all("li", recursive=False)):
            li_text = html_to_markdown(li, slug, depth + 1).strip()
            if tag == "ol":
                items.append(f"{i + 1}. {li_text}")
            else:
                items.append(f"- {li_text}")
        return "\n\n" + "\n".join(items) + "\n\n" if items else ""

    if tag == "li":
        return children_md.strip()

    if tag == "br":
        return "\n"

    if tag == "hr":
        return "\n\n---\n\n"

    if tag == "table":
        return _convert_table(element)

    if tag in ("div", "section", "article", "main", "span"):
        return children_md

    return children_md


def _convert_table(table):
    """Convert an HTML table to markdown."""
    rows = []
    for tr in table.find_all("tr"):
        cells = []
        for cell in tr.find_all(["th", "td"]):
            cells.append(cell.get_text(strip=True).replace("|", "\\|"))
        if cells:
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


# --- Content Extraction ---
def find_content_container(soup):
    """Find the main content container using ADGM-specific then generic selectors."""
    # ADGM uses a modern headless CMS (likely Contentful or similar)
    selectors = [
        # ADGM-specific selectors
        "div.article-content",
        "div.news-content",
        "div.press-release-content",
        "div.page-content-body",
        "div.content-area",
        "div.article-detail",
        "div.news-detail",
        "div.inner-content",
        "div.main-content",
        "div.page-content",
        "div.rich-text",
        "div.text-content",
        "div.body-content",
        # React/Next.js patterns (ADGM may use modern stack)
        "div[class*='article']",
        "div[class*='content-body']",
        "div[class*='richtext']",
        "div[class*='RichText']",
        # Contentful/Headless patterns
        "div.field-content",
        "div[class*='Content']",
        # Generic CMS
        "div.field--name-body",
        "article .node__content",
        "div.entry-content",
        "article .post-content",
        # Generic fallbacks
        "article",
        "main",
        "div.content",
        "div#content",
        "div.post",
        "div[role='main']",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return el
    return soup.find("body")


def remove_noise(soup):
    """Remove noise elements from the soup."""
    for selector in NOISE_SELECTORS:
        for el in soup.select(selector):
            el.decompose()
    return soup


def extract_brief(soup):
    """Extract short brief/description."""
    og = soup.find("meta", property="og:description")
    if og and og.get("content"):
        return og["content"].strip()
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return meta["content"].strip()
    return None


def extract_title(soup):
    """Extract title from multiple sources."""
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()
    title = soup.find("title")
    if title and title.string:
        return title.string.strip()
    return None


# --- Content Dedup ---
content_hashes = {}


def clean_markdown(md):
    """Clean up markdown."""
    md = re.sub(r"\n{3,}", "\n\n", md)
    md = re.sub(r" +\n", "\n", md)
    md = md.strip()
    return md


# --- Page Processor ---
def process_page(entry, state, force=False):
    """Fetch and process a single page."""
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq")

    if should_skip_url(url):
        return None, False

    parsed = urlparse(url)
    slug = generate_slug(parsed.path)

    if not force:
        stored = state.get(slug)
        if stored and stored.get("lastmod") == lastmod:
            slug_dir = BASE_DIR / slug
            if (slug_dir / "content.md").exists():
                print(f"[skip] {slug} -- unchanged")
                return slug, False

    time.sleep(REQUEST_DELAY)

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[error] {slug} -- {e}")
        return slug, False

    soup = BeautifulSoup(resp.content, "lxml")
    soup = remove_noise(soup)

    container = find_content_container(soup)
    if not container:
        print(f"[skip] {slug} -- no content container")
        return slug, False

    text_content = container.get_text(separator=" ", strip=True)

    if is_listing_page(url, soup, text_content):
        print(f"[skip] {slug} -- listing page")
        return slug, False

    word_count = len(text_content.split())
    if word_count < MIN_ARTICLE_WORDS:
        print(f"[skip] {slug} -- too short ({word_count} words)")
        return slug, False

    markdown = html_to_markdown(container, slug)
    markdown = clean_markdown(markdown)

    if not markdown.strip():
        print(f"[skip] {slug} -- empty content")
        return slug, False

    content_hash = hashlib.md5(markdown.encode()).hexdigest()
    if content_hash in content_hashes:
        print(f"[dedup] {slug} -- duplicate of {content_hashes[content_hash]}")
        return slug, False
    content_hashes[content_hash] = slug

    title = extract_title(soup)
    publish_date = extract_date(soup, lastmod)
    brief = extract_brief(soup)
    tags = extract_tags(soup)
    content_type, category = detect_content_type(parsed.path)

    truncated = False
    paywall_indicators = ["subscribe to continue", "premium content", "login to read", "sign in to read"]
    page_text = soup.get_text(separator=" ", strip=True).lower()
    for indicator in paywall_indicators:
        if indicator in page_text and word_count < 100:
            truncated = True
            break

    meta = {
        "title": title or slug,
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

    slug_dir = BASE_DIR / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    with open(slug_dir / "meta.yaml", "w", encoding="utf-8") as f:
        yaml.dump(meta, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    with open(slug_dir / "content.md", "w", encoding="utf-8") as f:
        f.write(markdown)

    state[slug] = {"lastmod": lastmod, "content_hash": content_hash}

    print(f"[saved] {slug} ({word_count} words)")
    return slug, True


# --- Internal Link Replacement ---
def replace_internal_links():
    """Replace internal links with local relative paths."""
    existing_slugs = set()
    for d in BASE_DIR.iterdir():
        if d.is_dir() and (d / "content.md").exists() and d.name != "images":
            existing_slugs.add(d.name)

    if not existing_slugs:
        return

    pattern = re.compile(
        r"\[([^\]]+)\]\(https?://(?:www\.)?" + re.escape("adgm.com") + r"(/[^)\s]*)\)"
    )

    for slug in existing_slugs:
        content_path = BASE_DIR / slug / "content.md"
        try:
            text = content_path.read_text(encoding="utf-8")
        except IOError:
            continue

        def _replace(match):
            link_text = match.group(1)
            url_path = match.group(2).split("?")[0].split("#")[0]
            target_slug = generate_slug(url_path)
            if target_slug in existing_slugs:
                return f"[{link_text}](../{target_slug}/content.md)"
            return match.group(0)

        new_text = pattern.sub(_replace, text)
        if new_text != text:
            content_path.write_text(new_text, encoding="utf-8")


# --- Main ---
def main():
    parser = argparse.ArgumentParser(description=f"Scrape {DOMAIN} via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch all pages ignoring state")
    parser.add_argument("--slug", type=str, help="Fetch only this slug")
    args = parser.parse_args()

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    state = {}
    if FETCH_STATE_FILE.exists() and not args.force:
        try:
            state = json.loads(FETCH_STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, IOError):
            state = {}

    print(f"[sitemap] Fetching {SITEMAP_URL}")
    entries = fetch_sitemap(SITEMAP_URL)
    print(f"[sitemap] Found {len(entries)} URLs")

    if not entries:
        print("[error] No entries found in sitemap")
        return

    if args.slug:
        entries = [e for e in entries if generate_slug(urlparse(e["loc"]).path) == args.slug]
        if not entries:
            print(f"[error] No entry found matching slug '{args.slug}'")
            return
        print(f"[filter] Processing only slug: {args.slug}")

    saved = 0
    skipped = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for entry in entries:
            future = executor.submit(process_page, entry, state, args.force)
            futures[future] = entry

        for future in as_completed(futures):
            try:
                slug, success = future.result()
                if success:
                    saved += 1
                elif slug:
                    skipped += 1
            except Exception as e:
                errors += 1
                print(f"[error] Unexpected: {e}")

    FETCH_STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

    print("[post] Replacing internal links...")
    replace_internal_links()

    print(f"\nDone: {saved} saved, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    main()
