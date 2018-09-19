"""Worker module.
"""
import aiohttp
import asyncio
import json
import logging
import os
import sys

from logging.config import fileConfig

from lxml import etree
from typing import Iterable
from typing import Tuple
from urllib.parse import urlparse


fileConfig("logging_config.ini")
logger = logging.getLogger()


HTML_PARSER = etree.HTMLParser()

ONE_MINUTE_TIMEOUT = aiohttp.ClientTimeout(60)
TEN_SECONDS_TIMEOUT = aiohttp.ClientTimeout(10)

EVENT_LOOP = asyncio.get_event_loop()
STDOUT_WRITER_QUEUE = asyncio.Queue(loop=EVENT_LOOP)


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


async def write_to_stdout():
    """Chained coroutine, writes rows from out of queue.

    Args:
        csv_writer: Object retrieved from csv.writer(). Writes to the CSV.

    Side Effects:
        Writes a completed row to a CSV.
    """
    while True:
        row = await STDOUT_WRITER_QUEUE.get()
        if row is None:
            break
        else:
            logger.info(f"Success for {row}")
            sys.stdout.write(json.dumps(row))
            STDOUT_WRITER_QUEUE.task_done()  # Be polite.


async def get_row(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    rank: str,
    domain: str,
):
    """Coroutine/Task that puts successful attempts into the STDOUT_WRITER_QUEUE.

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
        await STDOUT_WRITER_QUEUE.put((url, rank, favicon_url))
        return None

    # Otherwise, we have to dive into the HTML.
    try:
        text_content = await _fetch_html(semaphore, session, url)
    except Exception as e:
        logger.exception(e)
        return None  # Failure.

    if not text_content:
        logger.debug(f"No text content for {url}.")
        return None  # Failure.

    favicon_url = get_favicon(text_content, url)

    if not favicon_url:
        logger.debug(f"No favicon found for {url}.")
        return None  # Failure.

    await STDOUT_WRITER_QUEUE.put((url, rank, favicon_url))  # Success.


async def consume(pairs: Iterable[Tuple[str, str]]):
    """Consumes pairs of ranks and domains.

    Args:
        pairs: Iterable of tuples of rank and domain strings.

    Side Effects:
        Upon completion of the entire fleet of requests, puts a None/"term"
        signal into the queue.
    """

    # TODO(rico): Figure out why these get finicky when put into global scope?
    semaphore = asyncio.Semaphore(75)

    async with aiohttp.ClientSession(
        loop=EVENT_LOOP, connector=aiohttp.TCPConnector(limit=100)
    ) as session:
        await asyncio.gather(
            *[get_row(semaphore, session, *pair) for pair in pairs]
        )

    await STDOUT_WRITER_QUEUE.put(None)
    STDOUT_WRITER_QUEUE.task_done()


def main(worker_number: int, pairs: list):
    """Main execution context.

    Args:
        worker_number: The number of this worker.
        pairs: From json.loads.

    Side Effects:
        Controls the event loop.
    """
    try:
        EVENT_LOOP.create_task(write_to_stdout())
        EVENT_LOOP.run_until_complete(consume(pairs))
    except Exception as e:
        # Note: I would never do bare exception handling in a real application,
        # This is just so we can see what this quick-and-dirty script does.
        STDOUT_WRITER_QUEUE.put_nowait(None)
        logger.error(f"***FATAL ERROR***: {e}")
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
    finally:
        # Shutting down and closing file descriptors after interrupt
        EVENT_LOOP.run_until_complete(EVENT_LOOP.shutdown_asyncgens())
        EVENT_LOOP.stop()
        EVENT_LOOP.close()


if __name__ == "__main__":
    payload = json.load(sys.stdin)
    main(0, payload)
