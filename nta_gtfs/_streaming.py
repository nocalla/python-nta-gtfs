"""Shared zip-download and CSV-streaming helpers for static GTFS clients.

Internal to the library — not part of the public API. Both
``StaticGtfsClient`` and ``StaticGtfsPickerClient`` download a static GTFS
zip the same way (streamed to an anonymous temp file with a size limit) and
stream CSVs out of it the same way; their parsing and lifecycle semantics
differ and stay separate.
"""

import asyncio
import csv
import io
import tempfile
import zipfile
from collections.abc import Iterator
from typing import IO

import aiohttp

from nta_gtfs.exceptions import StaticGtfsLoadError

_DOWNLOAD_CHUNK_BYTES = 1024 * 1024


async def download_zip_to_tempfile(
    url: str,
    session: aiohttp.ClientSession,
    max_download_bytes: int,
) -> IO[bytes]:
    """Stream a zip URL to an anonymous temporary file.

    Writes happen in a thread via ``asyncio.to_thread`` so the event loop is
    not blocked by disk I/O.

    Args:
        url: HTTPS URL of the zip to download.
        session: Caller-supplied aiohttp client session used for the request.
        max_download_bytes: Maximum permitted response body size in bytes.

    Returns:
        The open temporary file containing the downloaded bytes. The caller
        owns the file and is responsible for closing it, including on any
        error raised after this function returns.

    Raises:
        StaticGtfsLoadError: On a non-OK HTTP status, a Content-Length or
            streamed body exceeding ``max_download_bytes``, or an
            ``aiohttp.ClientError``.
    """
    tmp = await asyncio.to_thread(tempfile.TemporaryFile)
    try:
        try:
            async with session.get(url) as resp:
                if not resp.ok:
                    raise StaticGtfsLoadError(
                        f"Static GTFS download failed: HTTP {resp.status} from {url}"
                    )
                content_length = resp.content_length
                if content_length is not None and content_length > max_download_bytes:
                    raise StaticGtfsLoadError(
                        f"Static GTFS response too large: {content_length} bytes "
                        f"exceeds limit of {max_download_bytes} bytes"
                    )
                received = 0
                async for chunk in resp.content.iter_chunked(_DOWNLOAD_CHUNK_BYTES):
                    received += len(chunk)
                    if received > max_download_bytes:
                        raise StaticGtfsLoadError(
                            f"Static GTFS response too large: {received} bytes "
                            f"exceeds limit of {max_download_bytes} bytes"
                        )
                    await asyncio.to_thread(tmp.write, chunk)
        except aiohttp.ClientError as exc:
            raise StaticGtfsLoadError(
                f"Static GTFS download error for {url}: {exc}"
            ) from exc
    except Exception:
        await asyncio.to_thread(tmp.close)
        raise
    return tmp


def iter_csv(zf: zipfile.ZipFile, filename: str) -> Iterator[dict[str, str]]:
    """Stream a CSV file from an open zip one row dict at a time.

    Args:
        zf: Open zip archive to read from.
        filename: Name of the file inside the zip archive.

    Yields:
        Row dicts with string values; BOM-stripped headers.
    """
    with zf.open(filename) as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8-sig")
        yield from csv.DictReader(text)
