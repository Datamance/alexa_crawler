#!/usr/bin/env python3
"""Alexa Crawler.

scrapes the first 200K domains of the Alexa top million domains, finds their
favicon URL, and saves the results in a csv. The result csv should contain the
domain, its Alexa rank, and the full URL path to the favicon.
"""

__author__ = "Rico Rodriguez"

import asyncio
import csv
import io
import itertools
import logging
import os
import time
import zipfile
from collections import Counter
from logging.config import fileConfig
from typing import Iterable, Tuple
from urllib.parse import urlparse

import aiohttp
import requests
from aiohttp.client_exceptions import ClientConnectorError
from lxml import etree


# Set up Logging.
fileConfig("logging_config.ini")
logger = logging.getLogger()

# Constants.
TWO_HUNDRED_THOUSAND = 200 * 1000
ALEXA_TOP_MILLION_URL = "http://s3.amazonaws.com/alexa-static/top-1m.csv.zip"
DEFAULT_CSV = "top_200K_with_favicon_urls.csv"

# Initialize now so we don't have to over and over again.
HTML_PARSER = etree.HTMLParser()

# Løøp, bröther.
EVENT_LOOP = asyncio.get_event_loop()
TEN_MINUTE_TIMEOUT = aiohttp.ClientTimeout(600)
THREE_MINUTE_TIMEOUT = aiohttp.ClientTimeout(180)
ONE_MINUTE_TIMEOUT = aiohttp.ClientTimeout(60)
THIRTY_SECONDS_TIMEOUT = aiohttp.ClientTimeout(30)
TEN_SECONDS_TIMEOUT = aiohttp.ClientTimeout(10)

# # Set up Queues.
CSV_WRITER_QUEUE = asyncio.Queue(loop=EVENT_LOOP)


def get_ranks_and_domains(total_number):
    """Synchronously prepare the data."""
    # Consideration - if this were a much larger file, we might opt to stream
    # instead and use iter_content() and a zlib decompressor to iteratively
    # decompress and read out the file.
    print("starting download...")
    content = requests.get(ALEXA_TOP_MILLION_URL).content

    with zipfile.ZipFile(io.BytesIO(content), "r") as zipped:
        print("unpacking zipfile...")
        text_as_bytes = zipped.read("top-1m.csv")

    pairs = text_as_bytes.split(b"\n")[:total_number]
    return [pair.decode("utf-8").split(",") for pair in pairs]


def get_favicon(html, url):
    """Slightly modified version of pyfav's get_favicon_from_markup method.

    Forgoing BS4 cuz it's too heavyweight.

    https://github.com/phillipsm/pyfav/blob/master/pyfav/pyfav.py
    """
    parsed_site_uri = urlparse(url)

    tree = etree.fromstring(html, parser=HTML_PARSER)

    if not tree:
        return None

    # Do we have a link element with the icon?
    possible_favicons = []
    for link in tree.cssselect("link"):
        if "icon" in link.attrib.get("rel", "").lower() and link.attrib.get(
            "href"
        ):
            possible_favicons.append(link)

    if possible_favicons:

        favicon_url = possible_favicons.pop().attrib["href"]

        # Sometimes we get a protocol-relative path
        if favicon_url.startswith("//"):
            parsed_uri = urlparse(url)
            favicon_url = parsed_uri.scheme + ":" + favicon_url

        # An absolute path relative to the domain
        elif favicon_url.startswith("/"):
            favicon_url = (
                parsed_site_uri.scheme
                + "://"
                + parsed_site_uri.netloc
                + favicon_url
            )

        # A relative path favicon
        elif not favicon_url.startswith("http"):
            path, filename = os.path.split(parsed_site_uri.path)
            favicon_url = (
                parsed_site_uri.scheme
                + "://"
                + parsed_site_uri.netloc
                + "/"
                + os.path.join(path, favicon_url)
            )

        # We found a favicon in the markup and we've formatted the URL
        # so that it can be loaded independently of the rest of the page
        return favicon_url

    # No favicon in the markup
    return None


async def _fetch_html(
    semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, url: str
):
    """Helper coroutine to fetch the html content.

    Args:
        semaphore: asyncio.Semaphore. Lock used to control connection pool.
        session: aiohttp.ClientSession. Session maintaining TCPConnector.
        url: String. the URL we're trying to fetch.

    Returns:
        bytes or None.
    """
    async with semaphore:
        async with session.get(
            url, timeout=ONE_MINUTE_TIMEOUT, ssl=False
        ) as response:
            if response.status == 200:
                return await response.read()
            else:
                return None


async def _try_get_favicon(
    semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, url: str
):
    """Blind attempt at getting the favicon.

    Args:
        semaphore: asyncio.Semaphore. Lock used to control connection pool.
        session: aiohttp.ClientSession. Session maintaining TCPConnector.
        url: String. The favicon URL we're blindly reaching out for.

    Returns:
        str or None.
    """
    maybe_favicon = url.rstrip("/") + "/favicon.ico"
    async with semaphore:
        async with session.get(
            # Short timeout because the favicon payload should be small.
            maybe_favicon,
            timeout=TEN_SECONDS_TIMEOUT,
            ssl=False,
        ) as response:
            if response.status == 200:
                body_bytes = await response.content.read()
                # Is there actually something here?
                return maybe_favicon if len(body_bytes) > 0 else None
            else:
                return None


async def write_to_csv(csv_writer):
    """Chained coroutine, writes rows from out of queue.

    Args:
        csv_writer: Object retrieved from csv.writer(). Writes to the CSV.

    Side Effects:
        Writes a completed row to a CSV.
    """
    while True:
        row = await CSV_WRITER_QUEUE.get()
        if row is None:
            break
        else:
            logger.info(f"Success for {row}")
            csv_writer.writerow(row)
            CSV_WRITER_QUEUE.task_done()  # Be polite.


async def get_row(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    rank: str,
    domain: str,
):
    """Coroutine/Task that puts successful attempts into the CSV_WRITER_QUEUE.

    Args:
        semaphore: asyncio.Semaphore. Lock used to control connection pool.
        session: aiohttp.ClientSession. Session maintaining TCPConnector.
        rank: String. The rank of the domain.
        domain: String. The domain itself, which is promptly formatted into a
            real url.

    Side Effects:
        Puts successful records into the queue.
    """
    url = f"http://www.{domain}"

    # First, make a blind attempt.
    try:
        favicon_url = await _try_get_favicon(semaphore, session, url)
    except Exception as e:
        favicon_url = None

    if favicon_url:  # Success.
        await CSV_WRITER_QUEUE.put((url, rank, favicon_url))

    # Otherwise, we have to dive into the HTML.
    try:
        text_content = await _fetch_html(semaphore, session, url)
    except Exception as e:
        logger.exception(e)
        return None  # Exit early.

    if not text_content:
        logger.debug(f"No text content for {url}.")
        return None  # Exit early.

    favicon_url = get_favicon(text_content, url)

    if not favicon_url:
        logger.debug(f"No favicon found for {url}.")
        return None  # Exit early.

    await CSV_WRITER_QUEUE.put((url, rank, favicon_url))


async def consume(chunked_pairs: Iterable[Iterable[Tuple[str, str]]]):
    """Consumes pairs of ranks and domains.

    Args:
        pairs: Iterable of tuples of rank and domain strings.

    Side Effects:
        Upon completion of the entire fleet of requests, puts a None/"term"
        signal into the queue.
    """

    # TODO(rico): Figure out why these get finicky when put into global scope?
    semaphore = asyncio.Semaphore(1000)

    for pairs in chunked_pairs:
        async with aiohttp.ClientSession(
            loop=EVENT_LOOP, connector=aiohttp.TCPConnector(limit=75)
        ) as session:
            rows = await asyncio.gather(
                *[get_row(semaphore, session, *pair) for pair in pairs]
            )

            # successes = [row for row in rows if row is not None]

        await CSV_WRITER_QUEUE.put(None)
        CSV_WRITER_QUEUE.task_done()

    return 0


def chunked(iterable: Iterable, chunk_size: int = 1000):
    """Generator to get things chunked.

    Args:
        iterable: Iterable. Stuff that we wanna chunk up.

    Yields:
        An iterable.
    """
    to_chunk = iter(iterable)

    while True:
        chunk = tuple(itertools.islice(to_chunk, chunk_size))
        if chunk:
            yield chunk
        else:
            return []


def main(
    csv_file_path: str = DEFAULT_CSV, total_number: int = TWO_HUNDRED_THOUSAND
):
    """Main execution context, controls event loop.

    Args:
        csv_file_path: String. The file path to write to.
        total_number: Integer. The total number of domains to process.

    Side Effects:
        Runs a loop.
    """
    # 1) Set up CSV file.
    csv_file = open(csv_file_path, "w", 1000)
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(("domain", "rank", "favicon_url"))

    # 2) Get work chunked up,
    pairs = get_ranks_and_domains(total_number)

    # 3) Run Event Loop.
    try:
        EVENT_LOOP.create_task(write_to_csv(csv_writer))
        EVENT_LOOP.run_until_complete(consume(chunked(pairs)))
    except Exception as e:
        # Note: I would never do bare exception handling in a real application,
        # This is just so we can see what this quick-and-dirty script does.
        CSV_WRITER_QUEUE.put_nowait(None)
        logger.error(f"***FATAL ERROR***: {e}")
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
    finally:
        # Shutting down and closing file descriptors after interrupt
        csv_file.close()
        EVENT_LOOP.run_until_complete(EVENT_LOOP.shutdown_asyncgens())
        EVENT_LOOP.stop()
        EVENT_LOOP.close()


if __name__ == "__main__":
    main()
