# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

This repository contains website scraper agents and the Python scraper scripts they generate. The agents produce site-specific scripts that archive news, finance, and corporate website content into structured markdown + YAML metadata.

## Agents

Two Claude Code agents live in `.claude/agents/`:

- **`homepage-scraper`** — Default entry point. Checks for sitemap first (delegates to sitemap-scraper if found), otherwise crawls from homepage via BFS. Generates `crawl_<domain>.py`.
- **`sitemap-scraper`** — Scrapes via sitemap.xml. Generates `scrape_<domain>.py`.

Both agents generate self-contained Python scripts — they don't share a common library. Each script is tailored to the target site's HTML structure after sampling 3 pages.

## Generated Script Structure

Each generated script produces this output layout:

```
<domain>/
├── images/
├── .fetch-state.json
├── <slug>/
│   ├── meta.yaml      # title, publish-date, content-type, category, tags, etc.
│   └── content.md     # article body with ../images/ refs and localized internal links
└── ...
```

## Running Generated Scripts

```bash
# Dependencies (auto-checked by scripts)
pip3 install requests beautifulsoup4 pyyaml lxml

# Incremental run (skips unchanged pages)
python3 scrape_<domain>.py
python3 crawl_<domain>.py

# Force re-fetch everything
python3 scrape_<domain>.py --force
python3 crawl_<domain>.py --recrawl --force

# Single page
python3 scrape_<domain>.py --slug some-article
python3 crawl_<domain>.py --slug some-article
```

## Key Design Decisions

- **Listing vs article detection**: Scripts must distinguish listing/index pages from actual articles. Articles have >200 words of prose, a publish date, and a specific slug. Listings are link-heavy with little unique content. Only articles are saved.
- **Content type from URL**: The `content-type` and `category` fields in `meta.yaml` are auto-detected from URL path patterns (e.g. `/news/forex-news/slug` → content-type: `forex-news`, category: `news`).
- **Incremental state**: sitemap-scraper uses `lastmod` comparison; homepage-scraper uses MD5 content hash. Both store state in `.fetch-state.json`.
- **Date fallback chain**: 8 extraction methods tried in priority order, ending with sitemap `lastmod` or HTTP `Last-Modified`.
- **WordPress image proxies**: `i0-i3.wp.com` URLs are handled — fetch from proxy but derive filename from the original path.
- **Internal link localization**: After scraping, all internal links in `content.md` files are replaced with `../slug/content.md` relative paths where the target exists locally.
