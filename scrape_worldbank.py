#!/usr/bin/env python3
"""
World Bank News Scraper
Scrapes www.worldbank.org via sitemap.xml — extracts YAML metadata + markdown content + images.
Supports incremental runs that skip unchanged pages.

World Bank sitemap structure:
  - Sitemap index at /sitemap.xml with child sitemaps by content type
  - Content under /en/news/, /en/results/, /en/topic/, /en/publication/
  - Uses Drupal CMS with field--name-body content containers
  - Multilingual site — focuses on /en/ paths

Usage:
  python3 scrape_worldbank.py              # incremental run
  python3 scrape_worldbank.py --force      # re-fetch everything
  python3 scrape_worldbank.py --slug X     # fetch only slug X
"""

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
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
    print(
        "Missing dependencies. Install with:\n"
        f"  pip3 install --user --break-system-packages {' '.join(_MISSING)}"
    )
    raise SystemExit(1)

# ---------------------------------------------------------------------------
# Standard-library imports
# ---------------------------------------------------------------------------
import argparse
import hashlib
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN = "www.worldbank.org"
SITEMAP_URL = f"https://{DOMAIN}/sitemap.xml"
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR / "world_bank_news"
IMAGES_DIR = BASE_DIR / "images"
FETCH_STATE_FILE = BASE_DIR / ".fetch-state.json"

MAX_WORKERS = 5
REQUEST_DELAY = 1.0
MIN_ARTICLE_WORDS = 200

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})

CONTENT_PATH_PATTERNS = [
    "/en/news/", "/en/results/", "/en/topic/", "/en/publication/",
    "/en/programs/", "/en/projects-operations/", "/en/events/",
    "/en/research/", "/en/country/",
    "/news/", "/articles/", "/press-release/", "/blogs/",
    "/insights/", "/posts/", "/newsroom/", "/announcements/",
    "/opinion/", "/research/", "/reports/", "/publications/",
    "/speeches/", "/analysis/", "/news-release/",
    "/feature/", "/brief/", "/press-releases/",
]

# World Bank uses Drupal — content selectors
CONTENT_SELECTORS = [
    "div.field--name-body",
    "article .node__content",
    "div.body-content",
    "div.content-body",
    "div.l-body",
    "div.article-body",
    "div.page-body",
    "div.rich-text",
    "article .content",
    "article",
    "main .content",
    "div.content",
    "div#content",
    "main",
]

NOISE_SELECTORS = [
    "nav", "header", "footer", "aside",
    ".sidebar", ".related-articles", ".recommended",
    ".social-share", ".share-buttons", ".newsletter-signup", ".subscription-widget",
    ".comments", ".comment-section", ".author-bio", ".disclaimer", ".cookie-banner",
    ".breadcrumb", ".pagination", ".ad", ".advertisement",
    "[class*='promo']", "[class*='banner']", "[class*='popup']", "[class*='modal']",
    ".block-views", ".views-row", ".pager",
    ".field--name-field-related", ".field--name-field-tags",
    ".share-widget", ".social-links", ".print-links",
    "script", "style", "noscript", "svg", "button", "iframe",
]

SKIP_PATTERNS = {
    "/page/", "/search", "/login", "/register", "/account", "/cart",
    "/checkout", "/api/", "/graphql", "/webhook",
    "/sitemap", "/robots.txt", "/favicon",
    "?page=", "?p=", "/tag/", "/category/",
    "/archive", "/feed", "/rss",
}


# ---------------------------------------------------------------------------
# Fetch state
# ---------------------------------------------------------------------------
def load_state():
    if FETCH_STATE_FILE.exists():
        with open(FETCH_STATE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_state(state):
    FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FETCH_STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Sitemap parsing
# ---------------------------------------------------------------------------
def fetch_xml(url):
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        print(f"[error] Failed to fetch {url}: {exc}")
        return None


def parse_sitemap(url):
    xml_text = fetch_xml(url)
    if not xml_text:
        return []

    soup = BeautifulSoup(xml_text, "lxml-xml")

    sitemap_tags = soup.find_all("sitemap")
    if sitemap_tags:
        entries = []
        for sm in sitemap_tags:
            loc = sm.find("loc")
            if loc:
                child_url = loc.get_text(strip=True)
                print(f"[sitemap] Fetching child: {child_url}")
                time.sleep(0.5)
                entries.extend(parse_sitemap(child_url))
        return entries

    urls = []
    for url_tag in soup.find_all("url"):
        loc = url_tag.find("loc")
        if not loc:
            continue
        entry = {"loc": loc.get_text(strip=True)}
        lastmod = url_tag.find("lastmod")
        entry["lastmod"] = lastmod.get_text(strip=True) if lastmod else None
        changefreq = url_tag.find("changefreq")
        entry["changefreq"] = changefreq.get_text(strip=True) if changefreq else None
        urls.append(entry)
    return urls


# ---------------------------------------------------------------------------
# URL classification
# ---------------------------------------------------------------------------
def is_content_url(url):
    parsed = urlparse(url)
    path = parsed.path.lower()

    for skip in SKIP_PATTERNS:
        if skip in path or skip in (parsed.query or ""):
            return False

    ext = os.path.splitext(path)[1]
    if ext in {".css", ".js", ".json", ".xml", ".rss", ".atom", ".pdf",
               ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".avif",
               ".mp4", ".mp3", ".woff", ".woff2", ".ttf", ".eot", ".ico"}:
        return False

    for pattern in CONTENT_PATH_PATTERNS:
        if pattern in path:
            after = path.split(pattern, 1)[1].strip("/")
            if after and "/page/" not in after:
                return True

    return False


def is_listing_page(soup, text_content):
    word_count = len(text_content.split())
    if word_count < MIN_ARTICLE_WORDS:
        links = soup.find_all("a", href=True)
        internal_links = [a for a in links if DOMAIN in (a.get("href", "") or "")]
        if len(internal_links) > 10:
            return True

    title = soup.find("title")
    if title:
        title_text = title.get_text().lower()
        for indicator in ["archive", "all posts", "page 2", "category:",
                          "search results", "listing", "all news"]:
            if indicator in title_text:
                return True

    return False


# ---------------------------------------------------------------------------
# Content type & slug detection
# ---------------------------------------------------------------------------
def detect_content_type(url_path):
    path_lower = url_path.lower().strip("/")

    content_type = None
    category = None

    patterns_ordered = sorted(CONTENT_PATH_PATTERNS, key=len, reverse=True)
    for pattern in patterns_ordered:
        pattern_clean = pattern.strip("/").lower()
        if pattern_clean in path_lower:
            parts = pattern_clean.split("/")
            parts = [p for p in parts if p not in ("en",)]
            if len(parts) >= 2:
                content_type = parts[-1]
                category = parts[-2]
            elif len(parts) == 1:
                content_type = parts[0]
            break

    if not content_type:
        content_type = "general"

    return content_type, category


def generate_slug(url_path):
    parsed_path = unquote(url_path).strip("/")
    segments = [s for s in parsed_path.split("/") if s]

    # Remove language prefix
    if segments and segments[0].lower() in {"en", "fr", "es", "ar", "zh", "ru", "ja", "pt"}:
        segments = segments[1:]

    # Remove date segments
    filtered = []
    for seg in segments:
        if re.match(r"^\d{4}$", seg):
            continue
        if re.match(r"^\d{1,2}$", seg) and len(seg) <= 2:
            continue
        filtered.append(seg)

    content_type_segments = set()
    for pattern in CONTENT_PATH_PATTERNS:
        for part in pattern.strip("/").lower().split("/"):
            if part and part not in ("en",):
                content_type_segments.add(part)

    meaningful = []
    for seg in filtered:
        if seg.lower() not in content_type_segments:
            meaningful.append(seg)

    if not meaningful:
        meaningful = filtered[-1:] if filtered else segments[-1:]

    slug = meaningful[-1] if meaningful else "index"

    slug = slug.lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")

    return slug or "index"


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------
def extract_date(soup, sitemap_lastmod=None, response_headers=None):
    meta = soup.find("meta", property="article:published_time")
    if meta and meta.get("content"):
        return _normalize_date(meta["content"])

    for name in ("date", "publish-date", "publication_date", "dcterms.date"):
        meta = soup.find("meta", attrs={"name": name})
        if meta and meta.get("content"):
            return _normalize_date(meta["content"])

    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        return _normalize_date(time_el["datetime"])

    for selector in [".date", "[class*='date']", "[class*='timestamp']",
                     ".field--name-field-date", ".node__date"]:
        el = soup.select_one(selector)
        if el:
            date_str = el.get_text(strip=True)
            parsed = _normalize_date(date_str)
            if parsed:
                return parsed

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if isinstance(data, dict):
                dp = data.get("datePublished")
                if dp:
                    return _normalize_date(dp)
        except (json.JSONDecodeError, TypeError, IndexError):
            pass

    if sitemap_lastmod:
        return _normalize_date(sitemap_lastmod)

    if response_headers:
        lm = response_headers.get("Last-Modified")
        if lm:
            return _normalize_date(lm)

    return None


def _normalize_date(date_str):
    if not date_str:
        return None
    date_str = date_str.strip()

    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ):
        try:
            dt = datetime.strptime(date_str[:30], fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------
def extract_tags(soup):
    tags = set()

    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        for t in meta_kw["content"].split(","):
            t = t.strip().lower()
            if t:
                tags.add(t)

    for meta in soup.find_all("meta", property="article:tag"):
        t = (meta.get("content") or "").strip().lower()
        if t:
            tags.add(t)

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if isinstance(data, dict):
                kw = data.get("keywords")
                if isinstance(kw, str):
                    for t in kw.split(","):
                        t = t.strip().lower()
                        if t:
                            tags.add(t)
                elif isinstance(kw, list):
                    for t in kw:
                        if isinstance(t, str):
                            tags.add(t.strip().lower())
        except (json.JSONDecodeError, TypeError):
            pass

    for selector in ('a[rel="tag"]', ".tags a", ".post-tags a", ".article-tags a",
                     "[class*='tag-link']", ".field--name-field-tags a",
                     ".field--name-field-topic a"):
        for a in soup.select(selector):
            t = a.get_text(strip=True).lower()
            if t and len(t) < 60:
                tags.add(t)

    for selector in (".cat-links a", ".entry-categories a", ".field--name-field-region a"):
        for a in soup.select(selector):
            t = a.get_text(strip=True).lower()
            if t and len(t) < 60:
                tags.add(t)

    return sorted(tags)


# ---------------------------------------------------------------------------
# HTML to Markdown
# ---------------------------------------------------------------------------
def html_to_markdown(element, slug, images_downloaded):
    if element is None:
        return ""

    parts = []
    for child in element.children:
        if isinstance(child, str):
            text = child
            if text.strip():
                parts.append(text)
            continue

        if not hasattr(child, "name") or child.name is None:
            continue

        tag = child.name.lower()

        if tag in {"script", "style", "noscript", "svg", "button", "nav",
                    "footer", "header", "iframe", "form", "input", "select", "textarea"}:
            continue

        el_class = " ".join(child.get("class", []))
        if any(noise in el_class.lower() for noise in
               ["share", "social", "subscribe", "newsletter", "comment", "sidebar",
                "related", "promo", "banner", "popup", "modal", "cookie", "ad-",
                "advertisement", "pagination", "breadcrumb", "back-to-top"]):
            continue

        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag[1])
            text = child.get_text(strip=True)
            if text:
                parts.append(f"\n\n{'#' * level} {text}\n\n")

        elif tag == "p":
            inner = html_to_markdown(child, slug, images_downloaded)
            if inner.strip():
                parts.append(f"\n\n{inner.strip()}\n\n")

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
            img = child.find("img")
            if img:
                parts.append(_process_img(img, slug, images_downloaded))
            else:
                text = child.get_text(strip=True)
                if text and href:
                    parts.append(f"[{text}]({href})")
                elif text:
                    parts.append(text)

        elif tag == "img":
            parts.append(_process_img(child, slug, images_downloaded))

        elif tag == "picture":
            img = child.find("img")
            if img:
                parts.append(_process_img(img, slug, images_downloaded))
            else:
                source = child.find("source")
                if source and source.get("srcset"):
                    src = source["srcset"].split(",")[0].strip().split(" ")[0]
                    parts.append(_download_and_ref(src, slug, "", images_downloaded))

        elif tag == "figure":
            inner = html_to_markdown(child, slug, images_downloaded)
            caption = child.find("figcaption")
            if caption:
                cap_text = caption.get_text(strip=True)
                inner = inner.replace(caption.get_text(), "")
                if cap_text:
                    inner += f"\n*{cap_text}*"
            parts.append(inner)

        elif tag == "blockquote":
            inner = html_to_markdown(child, slug, images_downloaded)
            lines = inner.strip().split("\n")
            quoted = "\n".join(f"> {line}" for line in lines)
            parts.append(f"\n\n{quoted}\n\n")

        elif tag == "pre":
            code = child.find("code")
            if code:
                lang_class = " ".join(code.get("class", []))
                lang = ""
                lang_match = re.search(r"language-(\w+)", lang_class)
                if lang_match:
                    lang = lang_match.group(1)
                parts.append(f"\n\n```{lang}\n{code.get_text()}\n```\n\n")
            else:
                parts.append(f"\n\n```\n{child.get_text()}\n```\n\n")

        elif tag == "code":
            parts.append(f"`{child.get_text()}`")

        elif tag == "ul":
            for li in child.find_all("li", recursive=False):
                li_text = html_to_markdown(li, slug, images_downloaded).strip()
                if li_text:
                    parts.append(f"\n- {li_text}")
            parts.append("\n")

        elif tag == "ol":
            for idx, li in enumerate(child.find_all("li", recursive=False), 1):
                li_text = html_to_markdown(li, slug, images_downloaded).strip()
                if li_text:
                    parts.append(f"\n{idx}. {li_text}")
            parts.append("\n")

        elif tag == "br":
            parts.append("\n")

        elif tag == "hr":
            parts.append("\n\n---\n\n")

        elif tag == "table":
            parts.append(_table_to_markdown(child))

        elif tag in ("div", "section", "span", "article", "main", "li", "dd", "dt"):
            inner = html_to_markdown(child, slug, images_downloaded)
            if inner.strip():
                parts.append(inner)

    result = "".join(parts)
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


def _process_img(img, slug, images_downloaded):
    src = img.get("src") or img.get("data-src") or ""
    alt = img.get("alt", "")
    if not src or src.startswith("data:"):
        return ""
    return _download_and_ref(src, slug, alt, images_downloaded)


def _download_and_ref(src, slug, alt, images_downloaded):
    if not src:
        return ""

    if src.startswith("//"):
        src = "https:" + src
    elif src.startswith("/"):
        src = f"https://{DOMAIN}{src}"

    original_path = src
    wp_proxy_match = re.match(r"https?://i[0-3]\.wp\.com/(.+)", src)
    if wp_proxy_match:
        original_path = wp_proxy_match.group(1).split("?")[0]

    parsed = urlparse(original_path)
    path = parsed.path.split("?")[0]
    ext = os.path.splitext(path)[1].lower()
    if ext not in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".svg", ".bmp", ".ico"}:
        ext = ".jpg"

    url_hash = hashlib.md5(src.encode()).hexdigest()[:10]
    filename = f"{slug}_{url_hash}{ext}"
    filepath = IMAGES_DIR / filename

    if not filepath.exists():
        try:
            resp = SESSION.get(src, timeout=20, stream=True)
            resp.raise_for_status()
            IMAGES_DIR.mkdir(parents=True, exist_ok=True)
            with open(filepath, "wb") as fh:
                for chunk in resp.iter_content(8192):
                    fh.write(chunk)
            images_downloaded.append(filename)
        except Exception as exc:
            print(f"[warn] Image download failed: {src} — {exc}")
            return f"![{alt}]({src})"

    return f"![{alt}](../images/{filename})"


def _table_to_markdown(table):
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
    lines.append("| " + " | ".join("---" for _ in rows[0]) + " |")
    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n\n" + "\n".join(lines) + "\n\n"


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------
def find_content_container(soup):
    for selector in CONTENT_SELECTORS:
        el = soup.select_one(selector)
        if el:
            text = el.get_text(strip=True)
            if len(text.split()) >= 50:
                return el
    return None


def remove_noise(soup):
    for selector in NOISE_SELECTORS:
        for el in soup.select(selector):
            el.decompose()


def extract_title(soup):
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)

    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        return og_title["content"].strip()

    title = soup.find("title")
    if title:
        t = title.get_text(strip=True)
        for sep in (" - World Bank", " | World Bank", " – World Bank"):
            if sep in t:
                t = t.split(sep)[0].strip()
        return t

    return "Untitled"


def extract_brief(soup):
    og_desc = soup.find("meta", property="og:description")
    if og_desc and og_desc.get("content"):
        return og_desc["content"].strip()

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return meta_desc["content"].strip()

    return ""


# ---------------------------------------------------------------------------
# Process a single page
# ---------------------------------------------------------------------------
def process_page(entry, state, force, content_hashes):
    url = entry["loc"]
    lastmod = entry.get("lastmod")
    changefreq = entry.get("changefreq", "unknown")

    slug = generate_slug(urlparse(url).path)
    slug_dir = BASE_DIR / slug

    if not force:
        stored = state.get(slug)
        if stored and stored.get("lastmod") == lastmod:
            if (slug_dir / "content.md").exists():
                return {"status": "skip", "slug": slug}

    try:
        time.sleep(REQUEST_DELAY)
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        return {"status": "error", "slug": slug, "error": str(exc)}

    soup = BeautifulSoup(resp.text, "lxml")
    remove_noise(soup)

    container = find_content_container(soup)
    if not container:
        return {"status": "skip_no_content", "slug": slug}

    text_content = container.get_text(strip=True)

    if is_listing_page(soup, text_content):
        return {"status": "skip_listing", "slug": slug}

    word_count = len(text_content.split())
    if word_count < MIN_ARTICLE_WORDS:
        return {"status": "skip_short", "slug": slug, "words": word_count}

    content_hash = hashlib.md5(text_content.encode()).hexdigest()
    if content_hash in content_hashes:
        original_slug = content_hashes[content_hash]
        return {"status": "dedup", "slug": slug, "original": original_slug}
    content_hashes[content_hash] = slug

    title = extract_title(soup)
    brief = extract_brief(soup)
    publish_date = extract_date(soup, sitemap_lastmod=lastmod, response_headers=dict(resp.headers))
    tags = extract_tags(soup)
    content_type, category = detect_content_type(urlparse(url).path)

    images_downloaded = []
    markdown = html_to_markdown(container, slug, images_downloaded)

    if not markdown.strip():
        return {"status": "skip_empty_md", "slug": slug}

    truncated = False
    paywall_indicators = ["subscribe to continue", "sign in to read", "members only",
                          "premium content", "subscription required"]
    page_text = soup.get_text().lower()
    if any(ind in page_text for ind in paywall_indicators) and word_count < 100:
        truncated = True

    slug_dir.mkdir(parents=True, exist_ok=True)

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

    with open(slug_dir / "content.md", "w", encoding="utf-8") as fh:
        fh.write(markdown.strip() + "\n")

    state[slug] = {
        "lastmod": lastmod,
        "content_hash": content_hash,
        "last_fetched": datetime.utcnow().strftime("%Y-%m-%d"),
    }

    return {
        "status": "ok",
        "slug": slug,
        "title": title,
        "images": len(images_downloaded),
        "words": word_count,
    }


# ---------------------------------------------------------------------------
# Internal link replacement
# ---------------------------------------------------------------------------
def replace_internal_links(base_dir, domain):
    slug_dirs = {
        d.name for d in base_dir.iterdir()
        if d.is_dir() and d.name != "images" and (d / "content.md").exists()
    }

    domain_pattern = re.compile(
        r"\[([^\]]+)\]\(https?://" + re.escape(domain) + r"/([^)\s?#]+)[^)]*\)"
    )

    for slug in slug_dirs:
        content_file = base_dir / slug / "content.md"
        content = content_file.read_text(encoding="utf-8")

        def _replacer(match):
            link_text = match.group(1)
            path = match.group(2).strip("/")
            target_slug = generate_slug("/" + path)
            if target_slug in slug_dirs:
                return f"[{link_text}](../{target_slug}/content.md)"
            return match.group(0)

        new_content = domain_pattern.sub(_replacer, content)
        if new_content != content:
            content_file.write_text(new_content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="World Bank News Scraper")
    parser.add_argument("--force", action="store_true", help="Re-fetch everything ignoring state")
    parser.add_argument("--slug", type=str, help="Fetch only this slug")
    args = parser.parse_args()

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    state = load_state()
    content_hashes = {}

    for slug_name, slug_state in state.items():
        ch = slug_state.get("content_hash")
        if ch:
            content_hashes[ch] = slug_name

    print(f"[sitemap] Fetching {SITEMAP_URL}")
    entries = parse_sitemap(SITEMAP_URL)
    print(f"[sitemap] Found {len(entries)} total URLs")

    content_entries = [e for e in entries if is_content_url(e["loc"])]
    print(f"[filter] {len(content_entries)} content URLs after filtering")

    if args.slug:
        content_entries = [e for e in content_entries if generate_slug(urlparse(e["loc"]).path) == args.slug]
        if not content_entries:
            print(f"[error] No sitemap entry found matching slug '{args.slug}'")
            return

    stats = {"ok": 0, "skip": 0, "error": 0, "dedup": 0, "images": 0}

    def _worker(entry):
        return process_page(entry, state, args.force, content_hashes)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_worker, e): e for e in content_entries}
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                print(f"[error] Unhandled: {exc}")
                stats["error"] += 1
                continue

            status = result["status"]
            slug = result.get("slug", "?")

            if status == "ok":
                stats["ok"] += 1
                stats["images"] += result.get("images", 0)
                print(f"[ok] {slug} — {result.get('title', '')[:60]} ({result.get('words', 0)} words)")
            elif status == "skip":
                stats["skip"] += 1
                print(f"[skip] {slug} — unchanged")
            elif status == "dedup":
                stats["dedup"] += 1
                print(f"[dedup] {slug} — duplicate of {result.get('original')}")
            elif status.startswith("skip_"):
                stats["skip"] += 1
            elif status == "error":
                stats["error"] += 1
                print(f"[error] {slug} — {result.get('error', '')}")

    save_state(state)

    print("[post] Replacing internal links...")
    replace_internal_links(BASE_DIR, DOMAIN)

    print(f"\nDone: {stats['ok']} fetched, {stats['skip']} skipped, "
          f"{stats['dedup']} deduped, {stats['error']} errors, "
          f"{stats['images']} images downloaded")


if __name__ == "__main__":
    main()
