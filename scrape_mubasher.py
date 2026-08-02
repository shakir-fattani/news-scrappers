#!/usr/bin/env python3
"""
Sitemap-based scraper for english.mubasher.info

Usage:
    python3 scrape_mubasher.py              # incremental run
    python3 scrape_mubasher.py --force      # re-fetch everything
    python3 scrape_mubasher.py --slug X     # fetch only slug X

Dependencies:
    pip3 install --user --break-system-packages requests beautifulsoup4 pyyaml lxml
"""

from __future__ import annotations

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
from urllib.parse import urljoin, urlparse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "english.mubasher.info"
SITEMAP_INDEX_URL = "https://english.mubasher.info/sitemap_index_en.xml"
CHILD_SITEMAP_COUNT = 51  # sitemap_1.xml through sitemap_51.xml
BASE_DIR = Path(__file__).resolve().parent / "mubasher"
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
    "/markets/", "/companies/", "/countries/",
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
    "[class*='modal']", ".cookie-notice",
    ".stock-info", ".ticker-bar", ".market-data-widget",
]

# Mubasher uses a React/Angular-style SPA with specific content containers
CONTENT_SELECTORS = [
    "div.article-body", "div[class*='article-body']",
    "div[class*='news-body']", "div[class*='newsBody']",
    "div.news-content", "div[class*='content-body']",
    "div.article-content", "div.post-content",
    "div.entry-content", "article",
    "main", "div.content", "div#content",
]

SKIP_TAGS = frozenset([
    "script", "style", "noscript", "svg", "button", "iframe",
    "form", "input", "select", "textarea",
])

WP_PROXY_PATTERN = re.compile(r"^https?://i[0-3]\.wp\.com/(.+)$")
DATE_SEGMENT_RE = re.compile(r"^\d{4}$|^\d{2}$|^\d{4}-\d{2}$")
URL_DATE_RE = re.compile(r"/(\d{4})/(\d{2})(?:/(\d{2}))?/")

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

PAYWALL_INDICATORS = [
    "subscribe to continue", "sign up to read", "premium content",
    "unlock this article", "membership required", "paywall",
    "register to read", "subscribe now to read",
]


# ---------------------------------------------------------------------------
# Shared session
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
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


# ---------------------------------------------------------------------------
# Sitemap fetching
# ---------------------------------------------------------------------------
def _fetch_sitemap_xml(url: str) -> ET.Element | None:
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        text = resp.text
        text = re.sub(r'xmlns="[^"]+"', "", text, count=1)
        return ET.fromstring(text.encode("utf-8"))
    except Exception as exc:
        print(f"[warn] Could not fetch sitemap {url}: {exc}")
        return None


def _parse_url_entries(root: ET.Element) -> list[dict[str, str | None]]:
    entries: list[dict[str, str | None]] = []
    for url_el in root:
        entry: dict[str, str | None] = {"loc": None, "lastmod": None, "changefreq": None}
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


def fetch_all_sitemap_entries() -> list[dict[str, str | None]]:
    """Fetch sitemap index, then all 51 child sitemaps."""
    all_entries: list[dict[str, str | None]] = []
    seen_locs: set[str] = set()

    # Try the index first
    print(f"[info] Fetching sitemap index: {SITEMAP_INDEX_URL}")
    root = _fetch_sitemap_xml(SITEMAP_INDEX_URL)

    child_urls: list[str] = []
    if root is not None:
        tag_local = root.tag.split("}")[-1] if "}" in root.tag else root.tag
        if tag_local == "sitemapindex":
            for el in root:
                for child in el:
                    tag_name = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    if tag_name == "loc" and child.text:
                        child_urls.append(child.text.strip())
            print(f"  [info] Index has {len(child_urls)} child sitemaps")
        else:
            # Not an index, parse directly
            entries = _parse_url_entries(root)
            for e in entries:
                if e["loc"] and e["loc"] not in seen_locs:
                    seen_locs.add(e["loc"])
                    all_entries.append(e)
            print(f"  [info] Direct sitemap: {len(entries)} URLs")

    # If index didn't yield children, build the list manually
    if not child_urls and not all_entries:
        base_url = SITEMAP_INDEX_URL.rsplit("/", 1)[0]
        child_urls = [f"{base_url}/sitemap_{i}.xml" for i in range(1, CHILD_SITEMAP_COUNT + 1)]
        print(f"  [info] Falling back to {len(child_urls)} expected child sitemaps")

    for child_url in child_urls:
        child_root = _fetch_sitemap_xml(child_url)
        if child_root is not None:
            entries = _parse_url_entries(child_root)
            for e in entries:
                if e["loc"] and e["loc"] not in seen_locs:
                    seen_locs.add(e["loc"])
                    all_entries.append(e)
            print(f"    [info] {child_url} -> {len(entries)} URLs")
        time.sleep(0.5)

    return all_entries


# ---------------------------------------------------------------------------
# URL classification
# ---------------------------------------------------------------------------
def is_content_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if DOMAIN not in parsed.netloc:
        return False
    if SKIP_URL_PATTERNS.search(path):
        return False
    if not path or path == "/":
        return False
    segments = [s for s in path.split("/") if s]
    if len(segments) <= 1:
        return False
    return True


def is_listing_page(soup: BeautifulSoup, url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if re.search(r"/page/\d+", path):
        return True
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        title_lower = title_tag.string.lower()
        if any(kw in title_lower for kw in ("archive", "all posts", "page 2", "category:")):
            return True
    has_article_meta = (
        soup.find("meta", property="article:published_time") is not None
        or soup.find("time") is not None
    )
    content_el = _find_content_element(soup)
    if content_el:
        text = content_el.get_text(separator=" ", strip=True)
        word_count = len(text.split())
        if word_count < 200:
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
    path = url_path.rstrip("/")
    segments = [s for s in path.split("/") if s]
    if not segments:
        return "index"
    content_slugs = {cp.strip("/") for cp in CONTENT_PATH_PATTERNS}
    for seg in reversed(segments):
        if DATE_SEGMENT_RE.match(seg):
            continue
        if seg.lower() in content_slugs:
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
def extract_date(soup: BeautifulSoup, url: str, lastmod: str | None,
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
    if lastmod:
        d = _normalize_date(lastmod)
        if d:
            return d
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
                                     "banner", "popup", "modal", "ad-"]):
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
# Page processing
# ---------------------------------------------------------------------------
def process_page(
    entry: dict[str, str | None],
    slug: str,
    state: dict[str, Any],
    content_hashes: dict[str, str],
    force: bool = False,
) -> bool:
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq")
    slug_dir = BASE_DIR / slug

    if not force:
        stored = state.get("slugs", {}).get(slug)
        if stored and stored.get("lastmod") == lastmod and (slug_dir / "content.md").exists():
            print(f"  [skip] {slug} -- unchanged")
            return False

    time.sleep(REQUEST_DELAY)

    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  [error] {slug} -- fetch failed: {exc}")
        return False

    soup = BeautifulSoup(resp.text, "lxml")

    if is_listing_page(soup, url):
        print(f"  [skip] {slug} -- listing page")
        return False

    remove_noise(soup)
    content_el = _find_content_element(soup)
    if not content_el:
        print(f"  [skip] {slug} -- no content element found")
        return False

    markdown = html_to_markdown(content_el, slug).strip()
    word_count = len(markdown.split())

    if word_count < 200:
        paywall_els = soup.select(
            "[class*='paywall'], [class*='subscribe'], [class*='premium'], [class*='locked']"
        )
        if paywall_els and word_count > 0:
            truncated = True
        else:
            print(f"  [skip] {slug} -- only {word_count} words")
            return False
    else:
        truncated = False

    content_hash = hashlib.md5(markdown.encode()).hexdigest()
    if content_hash in content_hashes:
        print(f"  [dedup] {slug} -- duplicate of {content_hashes[content_hash]}")
        return False
    content_hashes[content_hash] = slug

    title = _extract_title(soup)
    publish_date = extract_date(soup, url, lastmod, dict(resp.headers))
    short_brief = _extract_brief(soup)
    tags = extract_tags(soup)
    content_type, category = detect_content_type(urlparse(url).path)

    slug_dir.mkdir(parents=True, exist_ok=True)

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
    with open(slug_dir / "content.md", "w", encoding="utf-8") as fh:
        fh.write(markdown)

    if "slugs" not in state:
        state["slugs"] = {}
    state["slugs"][slug] = {"lastmod": lastmod, "content_hash": content_hash}

    print(f"  [saved] {slug} ({word_count} words)")
    return True


# ---------------------------------------------------------------------------
# Internal link replacement
# ---------------------------------------------------------------------------
def replace_internal_links(base_dir: Path) -> int:
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
    parser = argparse.ArgumentParser(description=f"Scrape {DOMAIN} via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything ignoring state")
    parser.add_argument("--slug", type=str, help="Fetch only this slug")
    args = parser.parse_args()

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    state = load_state()
    content_hashes: dict[str, str] = {}
    for s, info in state.get("slugs", {}).items():
        ch = info.get("content_hash")
        if ch:
            content_hashes[ch] = s

    all_entries = fetch_all_sitemap_entries()
    print(f"[info] Total unique URLs from sitemaps: {len(all_entries)}")

    content_entries = [e for e in all_entries if e["loc"] and is_content_url(e["loc"])]
    print(f"[info] {len(content_entries)} content URLs to process")

    used_slugs: set[str] = set()
    entries_with_slugs: list[tuple[dict[str, str | None], str]] = []
    for entry in content_entries:
        parsed = urlparse(entry["loc"])
        raw_slug = generate_slug(parsed.path)
        slug = resolve_slug_collision(raw_slug, used_slugs)
        used_slugs.add(slug)
        entries_with_slugs.append((entry, slug))

    if args.slug:
        entries_with_slugs = [(e, s) for e, s in entries_with_slugs if s == args.slug]
        if not entries_with_slugs:
            print(f"[error] Slug '{args.slug}' not found in sitemap entries")
            return

    saved_count = 0
    skipped_count = 0

    def _worker(item: tuple[dict[str, str | None], str]) -> bool:
        entry, slug = item
        try:
            return process_page(entry, slug, state, content_hashes, force=args.force)
        except Exception as exc:
            print(f"  [error] {slug} -- {exc}")
            return False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_worker, item): item for item in entries_with_slugs}
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
    print(f"  Total sitemap entries : {len(all_entries)}")
    print(f"  Content URLs filtered : {len(content_entries)}")
    print(f"  Saved                 : {saved_count}")
    print(f"  Skipped               : {skipped_count}")
    print(f"  Internal links fixed  : {link_count}")
    print(f"  Output directory      : {BASE_DIR}")


if __name__ == "__main__":
    main()
