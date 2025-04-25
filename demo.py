# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "httpx",
#     "ktflow",
# ]
#
# [tool.uv.sources]
# ktflow = { path = "." }
# ///

import shutil
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

from liquid import Flow

lock = threading.Lock()


def withlock[**P, T](func: Callable[P, T]) -> Callable[P, T]:
    def wrapper(*args, **kwargs):
        with lock:
            return func(*args, **kwargs)

    return wrapper


def infer_name(url: str) -> str:
    content_disposition = httpx.head(url).raise_for_status().headers.get("Content-Disposition")
    if content_disposition:
        return content_disposition.split("filename=")[1].strip('"')
    return url.split("/")[-1]


def recover_infer_name(url: str, e: Exception) -> Iterator[str]:
    print(f"Error occurred while inferring name for {url}. Falling back to 'file'")
    yield "file"


@withlock
def create_temp_file(dest: Path) -> Path:
    i = 0
    name = dest.name
    tempfile = dest.parent / f"{name}.part"
    while dest.exists() or tempfile.exists():
        i += 1
        dest = dest.parent / f"{i}_{name}"
        tempfile = dest.parent / f"{i}_{name}.part"
    tempfile.touch()
    return tempfile


def rename_temp_file(temp_file: Path) -> Path | None:
    if not temp_file.exists():
        return None
    target = temp_file.with_name(temp_file.name[:-5])
    shutil.move(temp_file, target)
    return target


def download_one(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)

    with httpx.stream("GET", url) as response:
        response.raise_for_status()
        with dest.open("wb") as file:
            for chunk in response.iter_bytes(1024 * 1024):
                file.write(chunk)
    return dest


def recover_download_one(pair: tuple[str, Path], e: Exception) -> Iterator[Path]:
    url, dest = pair
    print(f"Error occurred while downloading {url}: {e}")
    raise StopIteration  # Drop the download that failed


urls = ["http://ipv4.download.thinkbroadband.com/1MB.zip" for _ in range(5)]

Path("downloads").mkdir(parents=True, exist_ok=True)

with ThreadPoolExecutor(5) as executor:
    file_creation_flow = (
        Flow(urls)
        .submit_map(executor, infer_name, recover_infer_name)
        .map(lambda name: Path.cwd() / "downloads" / name)
        .map(create_temp_file)
    )
    download_flow = (
        Flow(urls)
        .zip(file_creation_flow)
        .on_each(lambda pair: print(f"Downloading {pair[0]} to {pair[1]}"))
        .submit_map_as_completed(executor, lambda pair: download_one(*pair), recover_download_one)
        .on_each(lambda path: print(f"File saved to {path}"))
        .map(rename_temp_file)
    )

    download_flow.collect(lambda path: print(f"File renamed to {path}"))
