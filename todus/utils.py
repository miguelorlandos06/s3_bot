from __future__ import annotations
import os, re, hashlib
from pathlib import Path
from typing import Optional, List, Dict, Iterable, Iterator
from urllib.parse import quote
from .constants import CONTENT_TYPES, CATEGORIES, guess_content_type, categorize
def format_size(size_bytes: int) -> str:
    if size_bytes is None or size_bytes < 0: return "—"
    if size_bytes == 0: return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]; size = float(size_bytes); idx = 0
    while size >= 1024 and idx < len(units) - 1: size /= 1024; idx += 1
    if idx == 0: return f"{int(size)} {units[idx]}"
    return f"{size:.2f} {units[idx]}"
def format_speed(bytes_per_sec: float) -> str:
    return f"{format_size(int(bytes_per_sec))}/s"
def format_date(iso_date: Optional[str]) -> str:
    if not iso_date: return "—"
    if iso_date.startswith(("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")):
        try:
            from email.utils import parsedate_to_datetime
            return parsedate_to_datetime(iso_date).strftime("%Y-%m-%d %H:%M")
        except Exception: return iso_date[:25]
    try:
        from datetime import datetime
        return datetime.fromisoformat(iso_date.replace("Z", "+00:00")[:19]).strftime("%Y-%m-%d %H:%M")
    except Exception: return iso_date[:19]
def format_duration(seconds: float) -> str:
    if seconds < 1: return f"{seconds*1000:.0f}ms"
    if seconds < 60: return f"{seconds:.1f}s"
    m, s = divmod(seconds, 60)
    if m < 60: return f"{int(m)}m {int(s)}s"
    h, m = divmod(m, 60); return f"{int(h)}h {int(m)}m"
def human_count(n: int) -> str:
    if n < 1000: return str(n)
    if n < 1_000_000: return f"{n/1000:.1f}K"
    return f"{n/1_000_000:.1f}M"
_INVALID_S3_CHARS = re.compile(r"[\x00-\x1f\x7f]")
_URL_UNSAFE = re.compile(r"[^\w\-._~/]")
def sanitize_filename(name: str, replacement: str = "_") -> str:
    if not name: return "unnamed"
    name = _INVALID_S3_CHARS.sub(replacement, name)
    name = re.sub(r"[^\x20-\x7E]", replacement, name)
    name = name.replace(" ", replacement)
    if len(name) > 255:
        stem, ext = os.path.splitext(name)
        name = stem[:255 - len(ext)] + ext
    return name or "unnamed"
def sanitize_path(path: str, replacement: str = "_") -> str:
    if not path: return ""
    parts = path.split("/")
    return "/".join(sanitize_filename(p, replacement) for p in parts if p)
def build_s3_key(namespace_prefix: str, path: Optional[str], filename: str, sanitize: bool = True) -> str:
    parts = [namespace_prefix.rstrip("/")]
    if path:
        path = sanitize_path(path) if sanitize else path.strip("/")
        if path: parts.append(path)
    parts.append(sanitize_filename(filename) if sanitize else filename)
    return "/".join(parts)
def normalize_key(key: str) -> str:
    while "//" in key: key = key.replace("//", "/")
    if key.startswith("./"): key = key[2:]
    return key.strip("/")
def url_encode_key(key: str) -> str:
    return quote(key, safe="/")
def content_disposition(filename: str, inline: bool = False) -> str:
    disposition = "inline" if inline else "attachment"
    try:
        filename.encode("ascii")
        return f'{disposition}; filename="{filename}"'
    except UnicodeEncodeError:
        encoded = quote(filename, safe="")
        return f'{disposition}; filename="{filename.encode("ascii", "replace").decode()}"; filename*=UTF-8\'\'{encoded}'
def md5_file(path: str, chunk_size: int = 64 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk: break
            h.update(chunk)
    return h.hexdigest()
def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()
def normalize_etag(etag: Optional[str]) -> str:
    if not etag: return ""
    return etag.strip().strip('"')
def parse_listing(xml_text: str) -> tuple:
    import xml.etree.ElementTree as ET
    NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    root = ET.fromstring(xml_text)
    items = []
    for contents in root.findall("s3:Contents", NS):
        key_elem = contents.find("s3:Key", NS)
        size_elem = contents.find("s3:Size", NS)
        date_elem = contents.find("s3:LastModified", NS)
        etag_elem = contents.find("s3:ETag", NS)
        items.append({
            "Key": key_elem.text if key_elem is not None else "",
            "Size": int(size_elem.text) if size_elem is not None and size_elem.text else 0,
            "LastModified": date_elem.text if date_elem is not None else None,
            "ETag": normalize_etag(etag_elem.text) if etag_elem is not None else None,
        })
    truncated_elem = root.find("s3:IsTruncated", NS)
    is_truncated = truncated_elem is not None and truncated_elem.text == "true"
    next_marker_elem = root.find("s3:NextMarker", NS)
    next_marker = next_marker_elem.text if next_marker_elem is not None else None
    if is_truncated and not next_marker and items: next_marker = items[-1]["Key"]
    return items, is_truncated, next_marker
def split_path_key(key: str) -> tuple:
    if "/" in key:
        dirname, basename = key.rsplit("/", 1)
        return dirname, basename
    return "", key
def join_path(*parts: str) -> str:
    cleaned = []
    for p in parts:
        if p: cleaned.append(p.strip("/"))
    return "/".join(cleaned)
def is_subpath(parent: str, child: str) -> bool:
    parent = parent.strip("/"); child = child.strip("/")
    if not parent: return True
    return child == parent or child.startswith(parent + "/")
def relative_path(parent: str, child: str) -> str:
    parent = parent.strip("/"); child = child.strip("/")
    if not parent: return child
    if child == parent: return ""
    if child.startswith(parent + "/"): return child[len(parent) + 1:]
    return child
def batched(iterable: Iterable, n: int) -> Iterator[List]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch; batch = []
    if batch: yield batch
