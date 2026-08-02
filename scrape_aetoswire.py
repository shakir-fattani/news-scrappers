#!/usr/bin/env python3
"""
Scraper for www.aetoswire.com — a Middle East press release distribution wire.

Fetches all press releases listed in the sitemap, extracts metadata + full
markdown content + images, and stores everything incrementally.

Usage:
    python3 scrape_aetoswire.py              # incremental run
    python3 scrape_aetoswire.py --force      # re-fetch everything
    python3 scrape_aetoswire.py --slug X     # fetch only slug X
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
from urllib.parse import urljoin, urlparse, unquote

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
try:
    import requests
    from bs4 import BeautifulSoup, NavigableString, Tag
    import yaml
    import lxml  # noqa: F401 — used as bs4 parser
except ImportError:
    print(
        "Missing dependencies. Install with:\n"
        "  pip3 install --user --break-system-packages requests beautifulsoup4 pyyaml lxml"
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "www.aetoswire.com"
SITEMAP_URL = "https://www.aetoswire.com/en/sitemap.xml"
BASE_DIR = Path(__file__).resolve().parent / "aetoswire"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

REQUEST_DELAY = 1.0  # seconds between requests
MAX_WORKERS = 5

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# URL patterns recognised as content pages
CONTENT_PATH_PATTERNS = [
    "/press-release/", "/press-releases/", "/news/", "/news-release/",
    "/articles/", "/blogs/", "/insights/", "/announcements/",
    "/newsroom/", "/posts/", "/opinion/", "/analysis/",
    "/reports/", "/publications/", "/mediacenter/",
]

# Patterns to skip entirely
SKIP_PATTERNS = {
    "/page/", "/search", "/tags/", "/tag/", "/category/",
    "/author/", "/about", "/contact", "/privacy", "/terms",
    "/subscribe", "/login", "/register", "/archive",
    "/rss", "/feed",
}

# Noise selectors to remove before content extraction
NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup",
    ".subscription-widget", ".comments", ".comment-section",
    ".author-bio", ".disclaimer", ".cookie-banner",
    ".breadcrumb", ".pagination", ".ad", ".advertisement",
    "[class*='promo']", "[class*='banner']", "[class*='popup']",
    "[class*='modal']", "script", "style", "noscript", "svg",
    "button", "iframe", "form",
]

# ---------------------------------------------------------------------------
# Shared session
# ---------------------------------------------------------------------------
_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT})

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

def fetch_sitemap(url: str) -> list[dict]:
    """Fetch sitemap (or sitemap index) and return list of URL entries."""
    resp = _session.get(url, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    # Check for sitemap index
    if root.tag == f"{{{ns['sm']}}}sitemapindex" or root.find("sm:sitemap", ns) is not None:
        entries = []
        for sitemap_el in root.findall("sm:sitemap", ns):
            loc_el = sitemap_el.find("sm:loc", ns)
            if loc_el is not None and loc_el.text:
                print(f"[sitemap-index] fetching child: {loc_el.text.strip()}")
                time.sleep(REQUEST_DELAY)
                entries.extend(fetch_sitemap(loc_el.text.strip()))
        return entries

    # Regular sitemap
    entries = []
    for url_el in root.findall("sm:url", ns):
        loc_el = url_el.find("sm:loc", ns)
        lastmod_el = url_el.find("sm:lastmod", ns)
        changefreq_el = url_el.find("sm:changefreq", ns)

        if loc_el is None or not loc_el.text:
            continue

        entries.append({
            "loc": loc_el.text.strip(),
            "lastmod": lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else None,
            "changefreq": changefreq_el.text.strip() if changefreq_el is not None and changefreq_el.text else None,
        })

    return entries


# ---------------------------------------------------------------------------
# URL analysis helpers
# ---------------------------------------------------------------------------

def should_skip_url(url: str) -> bool:
    """Return True for listing, pagination, and non-content pages."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/").lower()

    # Skip patterns
    for pat in SKIP_PATTERNS:
        if pat in path:
            return True

    # Skip pure root / language root
    if path in ("", "/", "/en", "/ar", "/en/", "/ar/"):
        return True

    # Skip pagination query params
    if re.search(r'[?&]page=\d+', url):
        return True

    return False


def is_listing_page(soup: BeautifulSoup, url: str) -> bool:
    """Heuristic: listing pages are link-heavy with little prose."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Path ends with a known content-type segment and nothing after
    for pat in CONTENT_PATH_PATTERNS:
        clean_pat = pat.strip("/")
        if path.rstrip("/").endswith(clean_pat):
            return True

    # Title-based signals
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        title_lower = title_tag.string.lower()
        for indicator in ("archive", "all posts", "page 2", "category:"):
            if indicator in title_lower:
                return True

    # Count article cards vs prose
    article_cards = soup.select("article h2 a, .card a, .post-card a")
    if len(article_cards) > 5:
        # Check if there's a dominant content body
        body = _find_content_container(soup)
        if body:
            text = body.get_text(separator=" ", strip=True)
            words = text.split()
            if len(words) < 200 and len(article_cards) > 10:
                return True

    return False


def detect_content_type(url_path: str) -> tuple[str, str | None]:
    """
    Derive content-type and category from URL path.

    For aetoswire.com, most URLs follow /en/press-release/slug or similar.
    """
    path = url_path.strip("/").lower()
    segments = path.split("/")

    # Remove language prefix
    if segments and segments[0] in ("en", "ar"):
        segments = segments[1:]

    if not segments:
        return "press-release", None

    # For aetoswire, the dominant content type is press-release
    # Check for known content type segments
    matched_indices = []
    for i, seg in enumerate(segments):
        test_path = f"/{seg}/"
        if test_path in CONTENT_PATH_PATTERNS or f"/{seg}/" in CONTENT_PATH_PATTERNS:
            matched_indices.append(i)

    if len(matched_indices) >= 2:
        # Nested: parent is category, deepest is content-type
        category = segments[matched_indices[0]]
        content_type = segments[matched_indices[-1]]
        return content_type, category

    if len(matched_indices) == 1:
        return segments[matched_indices[0]], None

    # Default for aetoswire
    return "press-release", None


def generate_slug(url: str) -> str:
    """Extract the last meaningful path segment as slug."""
    parsed = urlparse(url)
    path = unquote(parsed.path).strip("/")
    segments = [s for s in path.split("/") if s]

    # Remove language prefix
    if segments and segments[0] in ("en", "ar"):
        segments = segments[1:]

    # Remove known content-type segments
    content_segments = {p.strip("/") for p in CONTENT_PATH_PATTERNS}
    filtered = []
    for seg in segments:
        # Skip date segments like 2026, 07, 15
        if re.match(r"^\d{4}$", seg) or re.match(r"^\d{1,2}$", seg):
            continue
        # Skip pure content-type segments unless it's the only one left
        if seg.lower() in content_segments:
            continue
        filtered.append(seg)

    if not filtered:
        # Fallback: use last segment from original
        filtered = segments[-1:] if segments else ["index"]

    slug = filtered[-1]

    # Normalize
    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")

    return slug or "index"


# ---------------------------------------------------------------------------
# Content container detection
# ---------------------------------------------------------------------------

# Aetoswire-specific selectors first, then generic fallbacks
CONTENT_SELECTORS = [
    # Aetoswire specific
    "div.press-release-content",
    "div.press-release-body",
    "div.article-content",
    "div.post-content",
    "div.entry-content",
    "article .content",
    "div.content-body",
    "div.article-body",
    "div.news-content",
    "div.page-content",
    "div.release-body",
    # Generic fallbacks
    "article",
    "main",
    "div.content",
    "div#content",
    "div.post",
    "[role='main']",
]


def _find_content_container(soup: BeautifulSoup) -> Tag | None:
    for selector in CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            return el
    # Last resort: body
    return soup.find("body")


# ---------------------------------------------------------------------------
# Date extraction (8-method priority chain)
# ---------------------------------------------------------------------------

def extract_date(soup: BeautifulSoup, url: str, sitemap_lastmod: str | None) -> str | None:
    """Extract publish date using the 8-method priority chain."""

    # 1. article:published_time
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        return _normalize_date(meta["content"])

    # 2. meta name=date / publish-date
    for name in ("date", "publish-date", "publish_date", "pubdate"):
        meta = soup.find("meta", attrs={"name": name})
        if meta and meta.get("content"):
            return _normalize_date(meta["content"])

    # 3. <time datetime="..."> inside article
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        return _normalize_date(time_el["datetime"])

    # 4. Date-classed elements
    for selector in [
        "[class*='date']", "[class*='timestamp']", "[class*='publish']",
        "span.date", "div.date", "p.date",
    ]:
        el = soup.select_one(selector)
        if el:
            date_str = el.get_text(strip=True)
            parsed = _parse_date_text(date_str)
            if parsed:
                return parsed

    # 5. JSON-LD datePublished
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            dp = data.get("datePublished")
            if dp:
                return _normalize_date(dp)
        except (json.JSONDecodeError, AttributeError, IndexError):
            continue

    # 6. URL path date segments
    path = urlparse(url).path
    m = re.search(r"/(\d{4})/(\d{1,2})/(\d{1,2})/", path)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = re.search(r"/(\d{4})-(\d{2})/", path)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"

    # 7. Sitemap lastmod
    if sitemap_lastmod:
        return _normalize_date(sitemap_lastmod)

    # 8. No date found
    return None


def _normalize_date(raw: str) -> str | None:
    """Normalize various date formats to YYYY-MM-DD."""
    if not raw:
        return None
    raw = raw.strip()
    # ISO format: 2026-07-15T10:30:00+00:00
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def _parse_date_text(text: str) -> str | None:
    """Try to parse human-readable date text."""
    import calendar

    text = text.strip()
    # "July 15, 2026" or "15 July 2026"
    months = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
    months_abbr = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
    all_months = {**months, **months_abbr}

    # Month DD, YYYY
    m = re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", text)
    if m and m.group(1).lower() in all_months:
        month_num = all_months[m.group(1).lower()]
        return f"{m.group(3)}-{month_num:02d}-{int(m.group(2)):02d}"

    # DD Month YYYY
    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if m and m.group(2).lower() in all_months:
        month_num = all_months[m.group(2).lower()]
        return f"{m.group(3)}-{month_num:02d}-{int(m.group(1)):02d}"

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
        for tag in meta_kw["content"].split(","):
            tag = tag.strip().lower()
            if tag:
                tags.add(tag)

    # 2. article:tag (may appear multiple times)
    for meta in soup.find_all("meta", property="article:tag"):
        if meta.get("content"):
            tags.add(meta["content"].strip().lower())

    # 3. JSON-LD keywords
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0] if data else {}
            keywords = data.get("keywords")
            if isinstance(keywords, list):
                for kw in keywords:
                    tags.add(str(kw).strip().lower())
            elif isinstance(keywords, str):
                for kw in keywords.split(","):
                    kw = kw.strip().lower()
                    if kw:
                        tags.add(kw)
        except (json.JSONDecodeError, AttributeError, IndexError):
            continue

    # 4. Tag links in HTML
    for selector in ['a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a", "[class*='tag-link']"]:
        for a in soup.select(selector):
            text = a.get_text(strip=True).lower()
            if text and len(text) < 80:
                tags.add(text)

    # 5. Category links
    for selector in [".cat-links a", ".entry-categories a", ".category a"]:
        for a in soup.select(selector):
            text = a.get_text(strip=True).lower()
            if text and len(text) < 80:
                tags.add(text)

    return sorted(tags)


# ---------------------------------------------------------------------------
# Brief / subtitle extraction
# ---------------------------------------------------------------------------

def extract_brief(soup: BeautifulSoup) -> str:
    """Extract a short description or subtitle."""
    # og:description
    meta = soup.find("meta", property="og:description")
    if meta and meta.get("content"):
        return meta["content"].strip()

    # meta description
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return meta["content"].strip()

    # Subtitle elements
    for selector in [".subtitle", ".sub-title", ".deck", ".excerpt", ".summary"]:
        el = soup.select_one(selector)
        if el:
            text = el.get_text(strip=True)
            if text:
                return text

    return ""


# ---------------------------------------------------------------------------
# Image downloading
# ---------------------------------------------------------------------------

def download_image(img_url: str, slug: str) -> str | None:
    """Download image, return local filename or None on failure."""
    if not img_url or img_url.startswith("data:"):
        return None

    # Handle WordPress proxy URLs
    original_path = img_url
    parsed = urlparse(img_url)
    if re.match(r"i[0-3]\.wp\.com", parsed.hostname or ""):
        # Extract the original path from the proxy
        original_path = parsed.path
    else:
        original_path = parsed.path

    # Derive extension
    path_clean = original_path.split("?")[0]
    ext = os.path.splitext(path_clean)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp"):
        ext = ".jpg"  # fallback

    # Generate filename
    url_hash = hashlib.md5(img_url.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    filepath = IMAGES_DIR / filename

    if filepath.exists():
        return filename

    try:
        resp = _session.get(img_url, timeout=20, stream=True)
        resp.raise_for_status()
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        return filename
    except Exception as exc:
        print(f"  [img-fail] {img_url}: {exc}")
        return None


# ---------------------------------------------------------------------------
# HTML to Markdown converter
# ---------------------------------------------------------------------------

def html_to_markdown(element: Tag, slug: str) -> str:
    """Recursively convert an HTML element tree to markdown."""
    parts = []
    for child in element.children:
        if isinstance(child, NavigableString):
            text = str(child)
            # Collapse internal whitespace but preserve single spaces
            if text.strip():
                parts.append(text)
            elif parts and parts[-1] not in ("\n", "\n\n"):
                parts.append(" ")
            continue

        if not isinstance(child, Tag):
            continue

        tag = child.name.lower()

        # Skip noise tags
        if tag in ("script", "style", "noscript", "svg", "button", "iframe", "form", "nav"):
            continue

        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag[1])
            text = child.get_text(strip=True)
            if text:
                parts.append(f"\n\n{'#' * level} {text}\n\n")

        elif tag == "p":
            inner = html_to_markdown(child, slug).strip()
            if inner:
                parts.append(f"\n\n{inner}\n\n")

        elif tag in ("strong", "b"):
            text = child.get_text(strip=True)
            if text:
                parts.append(f"**{text}**")

        elif tag in ("em", "i"):
            text = child.get_text(strip=True)
            if text:
                parts.append(f"*{text}*")

        elif tag == "a":
            href = child.get("href", "")
            # Check if wrapping an image
            inner_img = child.find("img")
            if inner_img:
                parts.append(_convert_img(inner_img, slug))
            else:
                text = child.get_text(strip=True)
                if text and href:
                    # Make relative URLs absolute
                    if href.startswith("/"):
                        href = f"https://{DOMAIN}{href}"
                    parts.append(f"[{text}]({href})")
                elif text:
                    parts.append(text)

        elif tag == "img":
            parts.append(_convert_img(child, slug))

        elif tag == "picture":
            inner_img = child.find("img")
            if inner_img:
                parts.append(_convert_img(inner_img, slug))
            else:
                source = child.find("source")
                if source and source.get("srcset"):
                    src = source["srcset"].split(",")[0].strip().split(" ")[0]
                    if src.startswith("/"):
                        src = f"https://{DOMAIN}{src}"
                    fname = download_image(src, slug)
                    if fname:
                        parts.append(f"\n\n![image](../images/{fname})\n\n")

        elif tag == "figure":
            inner = html_to_markdown(child, slug).strip()
            caption_el = child.find("figcaption")
            caption = ""
            if caption_el:
                caption = f"\n*{caption_el.get_text(strip=True)}*"
                # Remove figcaption from inner since we handle it separately
                inner = inner.replace(f"*{caption_el.get_text(strip=True)}*", "").strip()
            if inner:
                parts.append(f"\n\n{inner}{caption}\n\n")

        elif tag == "blockquote":
            inner = html_to_markdown(child, slug).strip()
            if inner:
                quoted = "\n".join(f"> {line}" for line in inner.split("\n"))
                parts.append(f"\n\n{quoted}\n\n")

        elif tag in ("pre", "code"):
            if tag == "pre":
                code_el = child.find("code")
                text = (code_el or child).get_text()
                lang_class = (code_el or child).get("class", [])
                lang = ""
                for cls in (lang_class if isinstance(lang_class, list) else [lang_class]):
                    m = re.match(r"language-(\w+)", cls)
                    if m:
                        lang = m.group(1)
                        break
                parts.append(f"\n\n```{lang}\n{text}\n```\n\n")
            else:
                text = child.get_text()
                parts.append(f"`{text}`")

        elif tag == "ul":
            items = child.find_all("li", recursive=False)
            if items:
                parts.append("\n")
                for li in items:
                    li_text = html_to_markdown(li, slug).strip()
                    if li_text:
                        parts.append(f"\n- {li_text}")
                parts.append("\n")

        elif tag == "ol":
            items = child.find_all("li", recursive=False)
            if items:
                parts.append("\n")
                for idx, li in enumerate(items, 1):
                    li_text = html_to_markdown(li, slug).strip()
                    if li_text:
                        parts.append(f"\n{idx}. {li_text}")
                parts.append("\n")

        elif tag == "br":
            parts.append("\n")

        elif tag == "hr":
            parts.append("\n\n---\n\n")

        elif tag == "table":
            parts.append(_convert_table(child))

        elif tag in ("div", "section", "span", "article", "main", "li"):
            inner = html_to_markdown(child, slug)
            if inner.strip():
                parts.append(inner)

        else:
            # Generic: recurse
            inner = html_to_markdown(child, slug)
            if inner.strip():
                parts.append(inner)

    result = "".join(parts)
    # Collapse 3+ newlines to 2
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


def _convert_img(img: Tag, slug: str) -> str:
    """Convert an <img> tag to markdown, downloading the image."""
    src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
    alt = img.get("alt", "image")
    if not src:
        return ""
    if src.startswith("/"):
        src = f"https://{DOMAIN}{src}"
    elif src.startswith("//"):
        src = f"https:{src}"

    fname = download_image(src, slug)
    if fname:
        return f"\n\n![{alt}](../images/{fname})\n\n"
    return ""


def _convert_table(table: Tag) -> str:
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

    # Normalize column count
    max_cols = max(len(r) for r in rows)
    for row in rows:
        while len(row) < max_cols:
            row.append("")

    lines = []
    # Header row
    lines.append("| " + " | ".join(rows[0]) + " |")
    lines.append("| " + " | ".join(["---"] * max_cols) + " |")
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n\n" + "\n".join(lines) + "\n\n"


# ---------------------------------------------------------------------------
# Title extraction
# ---------------------------------------------------------------------------

def extract_title(soup: BeautifulSoup) -> str:
    """Extract the best article title."""
    # H1
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text

    # og:title
    meta = soup.find("meta", property="og:title")
    if meta and meta.get("content"):
        return meta["content"].strip()

    # <title>
    title_el = soup.find("title")
    if title_el and title_el.string:
        # Strip site name suffix
        title = title_el.string.strip()
        for sep in (" | ", " - ", " :: ", " — "):
            if sep in title:
                title = title.split(sep)[0].strip()
        return title

    return "Untitled"


# ---------------------------------------------------------------------------
# Page scraper
# ---------------------------------------------------------------------------

def scrape_page(entry: dict, state: dict, force: bool = False) -> dict | None:
    """
    Fetch and process a single page.

    Returns a result dict or None on skip/failure.
    """
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq")

    # Skip non-content URLs
    if should_skip_url(url):
        return {"status": "skip-url", "url": url}

    slug = generate_slug(url)

    # Incremental check
    if not force:
        stored_lastmod = state.get("lastmod", {}).get(slug)
        slug_dir = BASE_DIR / slug
        if stored_lastmod and stored_lastmod == lastmod and (slug_dir / "content.md").exists():
            return {"status": "skip-unchanged", "slug": slug}

    # Fetch page
    try:
        time.sleep(REQUEST_DELAY)
        resp = _session.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        print(f"  [error] {url}: {exc}")
        return {"status": "error", "url": url, "error": str(exc)}

    soup = BeautifulSoup(resp.text, "lxml")

    # Listing page check
    if is_listing_page(soup, url):
        return {"status": "skip-listing", "url": url}

    # Remove noise
    for selector in NOISE_SELECTORS:
        for el in soup.select(selector):
            el.decompose()

    # Find content container
    container = _find_content_container(soup)
    if container is None:
        print(f"  [warn] no content container found: {url}")
        return {"status": "error", "url": url, "error": "no content container"}

    # Convert to markdown
    content_md = html_to_markdown(container, slug).strip()

    # Check minimum content length
    word_count = len(content_md.split())
    if word_count < 50:
        # Might be a stub or listing
        return {"status": "skip-short", "url": url, "words": word_count}

    # Content deduplication
    content_hash = hashlib.md5(content_md.encode()).hexdigest()
    existing_slug = state.get("content_hashes", {}).get(content_hash)
    if existing_slug and existing_slug != slug:
        print(f"  [dedup] {slug} — duplicate of {existing_slug}")
        return {"status": "dedup", "slug": slug, "duplicate_of": existing_slug}

    # Extract metadata
    title = extract_title(soup)
    publish_date = extract_date(soup, url, lastmod)
    brief = extract_brief(soup)
    tags = extract_tags(soup)
    content_type, category = detect_content_type(urlparse(url).path)

    # Detect truncation
    truncated = False
    for indicator in [".paywall", ".subscribe-wall", "[class*='paywall']", "[class*='subscribe']"]:
        # Already decomposed, but check original if needed
        pass
    if word_count < 100:
        truncated = True

    # Build meta.yaml
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

    # Write files
    slug_dir = BASE_DIR / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    with open(slug_dir / "meta.yaml", "w", encoding="utf-8") as f:
        yaml.dump(meta, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    with open(slug_dir / "content.md", "w", encoding="utf-8") as f:
        f.write(content_md)

    # Update state
    state.setdefault("lastmod", {})[slug] = lastmod
    state.setdefault("content_hashes", {})[content_hash] = slug

    return {
        "status": "ok",
        "slug": slug,
        "title": title,
        "words": word_count,
    }


# ---------------------------------------------------------------------------
# Internal link replacement (post-scrape)
# ---------------------------------------------------------------------------

def replace_internal_links(base_dir: Path) -> int:
    """Replace internal links in all content.md files with local relative paths."""
    replaced_count = 0
    known_slugs = {d.name for d in base_dir.iterdir() if d.is_dir() and (d / "content.md").exists()}

    for slug_name in known_slugs:
        content_path = base_dir / slug_name / "content.md"
        try:
            text = content_path.read_text(encoding="utf-8")
        except Exception:
            continue

        original_text = text

        # Match markdown links to internal URLs
        def _replace_link(match):
            link_text = match.group(1)
            href = match.group(2)
            parsed = urlparse(href)
            if parsed.hostname and parsed.hostname != DOMAIN:
                return match.group(0)
            if not parsed.hostname and not parsed.path.startswith("/"):
                return match.group(0)

            # Extract slug from the URL path
            target_slug = generate_slug(href)
            if target_slug in known_slugs and target_slug != slug_name:
                return f"[{link_text}](../{target_slug}/content.md)"
            return match.group(0)

        text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _replace_link, text)

        if text != original_text:
            content_path.write_text(text, encoding="utf-8")
            replaced_count += 1

    return replaced_count


# ---------------------------------------------------------------------------
# Slug collision handling
# ---------------------------------------------------------------------------

def resolve_slug_collisions(entries: list[dict]) -> list[dict]:
    """Add -2, -3 suffixes for duplicate slugs."""
    slug_counts: dict[str, int] = {}
    result = []

    for entry in entries:
        slug = generate_slug(entry["loc"])
        slug_counts[slug] = slug_counts.get(slug, 0) + 1
        if slug_counts[slug] > 1:
            entry["_slug_override"] = f"{slug}-{slug_counts[slug]}"
        result.append(entry)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=f"Scrape {DOMAIN} via sitemap")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything, ignore state")
    parser.add_argument("--slug", type=str, help="Fetch only this specific slug")
    args = parser.parse_args()

    print(f"=== Scraping {DOMAIN} ===")
    print(f"Output directory: {BASE_DIR}")

    # Ensure directories
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load state
    state = {} if args.force else load_state()
    if args.force:
        state = {"lastmod": {}, "content_hashes": {}}

    # Fetch sitemap
    print(f"\nFetching sitemap: {SITEMAP_URL}")
    try:
        entries = fetch_sitemap(SITEMAP_URL)
    except Exception as exc:
        print(f"Failed to fetch sitemap: {exc}")
        sys.exit(1)

    print(f"Found {len(entries)} URLs in sitemap\n")

    # Filter for --slug
    if args.slug:
        entries = [e for e in entries if args.slug in e["loc"] or generate_slug(e["loc"]) == args.slug]
        if not entries:
            print(f"No sitemap entry matched slug: {args.slug}")
            sys.exit(1)
        print(f"Filtered to {len(entries)} entries matching slug: {args.slug}")

    # Process pages
    stats = {"ok": 0, "skip-unchanged": 0, "skip-url": 0, "skip-listing": 0,
             "skip-short": 0, "dedup": 0, "error": 0}

    def _process(entry):
        return scrape_page(entry, state, force=args.force)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_process, entry): entry for entry in entries}
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                entry = futures[future]
                print(f"  [crash] {entry['loc']}: {exc}")
                stats["error"] += 1
                continue

            if result is None:
                continue

            status = result["status"]
            stats[status] = stats.get(status, 0) + 1

            if status == "ok":
                print(f"  [ok] {result['slug']} — {result.get('title', '')[:60]} ({result['words']} words)")
            elif status == "skip-unchanged":
                print(f"  [skip] {result['slug']} — unchanged")
            elif status == "skip-listing":
                print(f"  [skip-listing] {result['url']}")
            elif status == "skip-url":
                pass  # silent
            elif status == "error":
                print(f"  [error] {result['url']}: {result.get('error')}")

    # Save state
    save_state(state)

    # Internal link replacement
    print("\nReplacing internal links...")
    link_count = replace_internal_links(BASE_DIR)
    print(f"Updated internal links in {link_count} files")

    # Summary
    print(f"\n=== Summary ===")
    print(f"  Fetched:         {stats['ok']}")
    print(f"  Skipped (same):  {stats['skip-unchanged']}")
    print(f"  Skipped (URL):   {stats['skip-url']}")
    print(f"  Skipped (list):  {stats['skip-listing']}")
    print(f"  Skipped (short): {stats['skip-short']}")
    print(f"  Deduplicated:    {stats['dedup']}")
    print(f"  Errors:          {stats['error']}")
    print(f"  State saved to:  {FETCH_STATE_FILE}")


if __name__ == "__main__":
    main()
