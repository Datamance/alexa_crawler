#!/usr/bin/env python3
"""Alexa Crawler.

scrapes the first 200K domains of the Alexa top million domains, finds their
favicon URL, and saves the results in a csv. The result csv should contain the
domain, its Alexa rank, and the full URL path to the favicon.
"""

__author__ = "Rico Rodriguez"

import zipfile
import aiohttp
from aiohttp.client_exceptions import ClientConnectorError
import asyncio
import io
import os
import requests
import csv
from collections import Counter
from lxml import etree
from pprint import pprint
from urllib.parse import urlparse
from bs4 import BeautifulSoup


TWO_HUNDRED_THOUSAND = 200 * 1000
DEFAULT_BATCH = TWO_HUNDRED_THOUSAND // 1000   # 1000 ClientSessions, 200 each.
ALEXA_TOP_MILLION_URL = "http://s3.amazonaws.com/alexa-static/top-1m.csv.zip"
DEFAULT_CSV = "top_200K_with_favicon_urls.csv"

HTML_PARSER = etree.HTMLParser()

# løøp, bröther
EVENT_LOOP = asyncio.get_event_loop()
THIRTY_SECONDS_TIMEOUT = aiohttp.ClientTimeout(30)
TEN_SECONDS_TIMEOUT = aiohttp.ClientTimeout(10)

# # Set up Queues
# FAVICON_GETTER_QUEUE = asyncio.Queue(loop=EVENT_LOOP)
# CSV_WRITER_QUEUE = asyncio.Queue(loop=EVENT_LOOP)


ERROR_COUNTER = Counter()


def get_ranks_and_domains(total_number, batch_size):
    """Synchronously prepare the data."""
    # Consideration - if this were a much larger file, we might opt to stream
    # instead and use iter_content() and a zlib decompressor to iteratively
    # decompress and read out the file.
    print("starting download...")
    content = requests.get(ALEXA_TOP_MILLION_URL).content

    with zipfile.ZipFile(io.BytesIO(content), "r") as zipped:
        print("unpacking bytes...")
        text_as_bytes = zipped.read("top-1m.csv")

    pairs = text_as_bytes.split(b"\n")[:total_number]

    chunks = []

    while pairs:
        chunks.append([pairs.pop().decode("utf-8").split(",")
                       for _ in range(batch_size)])

    return chunks


def get_favicon(html, url):
    """Slightly modified version of pyfav's get_favicon_from_markup method.

    https://github.com/phillipsm/pyfav/blob/master/pyfav/pyfav.py
    """
    parsed_site_uri = urlparse(url)

    soup = BeautifulSoup(html, features="lxml")

    # Do we have a link element with the icon?
    icon_link = soup.find('link', rel='icon')
    if icon_link and icon_link.has_attr('href'):

        favicon_url = icon_link['href']

        # Sometimes we get a protocol-relative path
        if favicon_url.startswith('//'):
            parsed_uri = urlparse(url)
            favicon_url = parsed_uri.scheme + ':' + favicon_url

        # An absolute path relative to the domain
        elif favicon_url.startswith('/'):
            favicon_url = parsed_site_uri.scheme + '://' + \
                parsed_site_uri.netloc + favicon_url

        # A relative path favicon
        elif not favicon_url.startswith('http'):
            path, filename = os.path.split(parsed_site_uri.path)
            favicon_url = parsed_site_uri.scheme + '://' + \
                parsed_site_uri.netloc + '/' + os.path.join(path, favicon_url)

        # We found a favicon in the markup and we've formatted the URL
        # so that it can be loaded independently of the rest of the page
        return favicon_url

    # No favicon in the markup
    return None


async def _fetch_html(session, url):
    """Helper method to fetch the html content."""
    async with session.get(
            url, timeout=TEN_SECONDS_TIMEOUT, ssl=False) as response:
        if response.status == 200:
            return await response.read()
        else:
            ERROR_COUNTER.update(["NON-200 RESPONSE"])
            return None


async def _try_get_favicon(session, url):
    """Blind attempt at getting the favicon."""
    maybe_favicon = url.rstrip("/") + "/favicon.ico"
    async with session.get(
            maybe_favicon, timeout=TEN_SECONDS_TIMEOUT, ssl=False) as response:
        if response.status == 200:
            body_bytes = await response.content.read()
            # Is there actually something here?
            return maybe_favicon if len(body_bytes) > 0 else None
        else:
            return None


async def get_row(session, rank, domain):
    url = f"http://www.{domain}"
    favicon_url = None

    # First, make a blind attempt.
    try:
        favicon_url = await _try_get_favicon(session, url)
        if favicon_url:
            return (url, rank, favicon_url)
    except Exception as e:
        pass  # We don't really care.

    # Otherwise, we have to dive into the HTML.
    try:
        text_content = await _fetch_html(session, url)
    except Exception as e:
        ERROR_COUNTER.update([type(e)])
        return None

    if not text_content:
        ERROR_COUNTER.update(["NO TEXT CONTENT"])
        return None

    favicon_url = get_favicon(text_content, url)

    if not favicon_url:
        ERROR_COUNTER.update(["NO FAVICON URL"])
        return None

    return (url, rank, favicon_url)


async def get_all(chunks_of_work, csv_writer):
    """Main doer of work."""
    all_results = []

    async with aiohttp.ClientSession(loop=EVENT_LOOP) as session:
        for rank_domain_pairs in chunks_of_work:
            rows = await asyncio.gather(
                *[get_row(session, *pair) for pair in rank_domain_pairs]
            )

            success_batch = [row for row in rows if row is not None]

            pprint(
                {
                    "Failures: ": ERROR_COUNTER.most_common(),
                    "Successes: ": len(success_batch)
                }
            )

            csv_writer.writerows(success_batch)

            all_results.extend(success_batch)

    return all_results


def main(csv_file_path=DEFAULT_CSV, total_number=TWO_HUNDRED_THOUSAND,
         batch_size=DEFAULT_BATCH):
    """Main execution context, controls event loop."""
    # 1) Set up CSV file.
    csv_file = open(csv_file_path, "w", batch_size)
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(("domain", "rank", "favicon_url"))

    # 2) Get work chunked up,
    chunks_of_work = get_ranks_and_domains(total_number, batch_size)

    try:
        EVENT_LOOP.run_until_complete(get_all(chunks_of_work, csv_writer))
    except Exception as e:  # this is pretty gross but whatever
        print(f"FATAL ERROR")
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
    finally:
        # Shutting down and closing file descriptors after interrupt
        csv_file.close()
        EVENT_LOOP.run_until_complete(EVENT_LOOP.shutdown_asyncgens())
        EVENT_LOOP.stop()
        EVENT_LOOP.close()


if __name__ == "__main__":
    main()
