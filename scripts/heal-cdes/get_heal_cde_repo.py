#!/usr/bin/env python3
"""Download the HEAL CDE repository table and save it as a JSONL file."""

import json
import re

import click
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.nih.gov"
REPO_PATH = (
    "/heal/heal-initiative-requirements/data-sharing-policy"
    "/common-data-elements-cdes-program/cdes-repository"
)


def fetch_page(session, page):
    url = BASE_URL + REPO_PATH
    resp = session.get(url, params={"page": page})
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def get_last_page(soup):
    """Return the 0-indexed last page number from the pager nav."""
    page_nums = []
    for a in soup.select("nav.usa-pagination a[href]"):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            page_nums.append(int(m.group(1)))
    return max(page_nums) if page_nums else 0


def parse_rows(soup):
    """Yield dicts for each CDE row on the page."""
    for tr in soup.select("tr"):
        desc_td = tr.find("td", class_="views-field-body")
        topic_td = tr.find("td", class_="views-field-field-heal-research-topic")
        files_td = tr.find("td", class_="views-field-field-cde-files")
        if not (desc_td and files_td):
            continue
        description = desc_td.get_text(strip=True)
        research_topic = topic_td.get_text(strip=True) if topic_td else ""
        urls = [
            BASE_URL + a["href"]
            for a in files_td.find_all("a")
            if a.get("href")
        ]
        yield {
            "description": description,
            "research_topic": research_topic,
            "urls": urls,
        }


@click.command()
@click.argument("output", default="heal_cde_repo.jsonl", type=click.Path())
def main(output):
    """Download the HEAL CDE repository table and save it as a JSONL file."""
    session = requests.Session()
    session.headers["User-Agent"] = "heal-cde-scraper/1.0 (research)"

    click.echo("Fetching page 0 to determine total pages...")
    first_page = fetch_page(session, 0)
    last_page = get_last_page(first_page)
    click.echo(f"Total pages: {last_page + 1}")

    rows = []
    for page_num in range(last_page + 1):
        click.echo(f"  Fetching page {page_num + 1}/{last_page + 1}...")
        soup = first_page if page_num == 0 else fetch_page(session, page_num)
        rows.extend(parse_rows(soup))

    with open(output, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    click.echo(f"Wrote {len(rows)} entries to {output}")


if __name__ == "__main__":
    main()
