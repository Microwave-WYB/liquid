import shutil
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
from tqdm import tqdm

from ktflow.core import Flow

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


def create_temp_file(dest: Path) -> Path:
    i = 0
    name = dest.name
    tempfile = dest.parent / f"{name}.part"
    with lock:
        while dest.exists() or tempfile.exists():
            i += 1
            dest = dest.parent / f"{i}_{name}"
            tempfile = dest.parent / f"{i}_{name}.part"
    tempfile.touch()
    return tempfile


def rename_temp_file(temp_file: Path) -> Path:
    target = temp_file.with_name(temp_file.name[:-5])
    shutil.move(temp_file, target)
    return target


def download_one(url: str, dest: Path) -> Path:
    print(f"Downloading {url} to {dest}")
    dest.parent.mkdir(parents=True, exist_ok=True)

    with httpx.stream("GET", url) as response:
        response.raise_for_status()
        with dest.open("wb") as file:
            for chunk in response.iter_bytes(1024 * 1024):
                file.write(chunk)
    return dest


urls = ["http://ipv4.download.thinkbroadband.com/1MB.zip" for _ in range(10)]

Path("downloads").mkdir(parents=True, exist_ok=True)

with ThreadPoolExecutor(5) as executor:
    list(
        tqdm(
            Flow(urls)
            .zip(
                Flow(urls)
                .submit_map(executor, infer_name)
                .map(lambda name: Path.cwd() / "downloads" / name)
                .map(create_temp_file)
            )
            .submit(executor, lambda pair: download_one(*pair))
            .map(lambda path: path.result())
            .map(rename_temp_file),
            total=len(urls),
        )
    )
