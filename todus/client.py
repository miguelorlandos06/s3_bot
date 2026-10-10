from __future__ import annotations
import os, time, shutil
from pathlib import Path
from typing import Optional, Iterator, Dict, List
import requests
from .constants import (
    DEFAULT_BASE_URL, DEFAULT_TIMEOUT, DEFAULT_MAX_RETRIES,
    DEFAULT_CHUNK_SIZE, MAX_KEYS_PER_PAGE, USER_AGENT, guess_content_type,
)
from .exceptions import (
    ToDusError, ToDusConnectionError, ToDusTimeoutError, ToDusServerError,
    ToDusNotFoundError,
)
from .utils import (
    parse_listing, normalize_etag, url_encode_key, md5_file,
    content_disposition, format_size,
)
from .progress import progress_bar
class S3Client:
    def __init__(self, base_url: str = DEFAULT_BASE_URL, timeout: int = DEFAULT_TIMEOUT,
                 max_retries: int = DEFAULT_MAX_RETRIES, chunk_size: int = DEFAULT_CHUNK_SIZE,
                 session: Optional[requests.Session] = None):
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max(1, max_retries)
        self.chunk_size = chunk_size
        self.session = session or requests.Session()
        if not session:
            adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
            self.session.headers["User-Agent"] = USER_AGENT
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.session.close()
    def _build_url(self, key: str) -> str:
        return f"{self.base}/{url_encode_key(key)}"
    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", self.timeout)
        last_exc = None
        for attempt in range(self.max_retries):
            try:
                r = self.session.request(method, url, **kwargs)
                if 500 <= r.status_code < 600 and attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt); continue
                return r
            except requests.Timeout as e:
                last_exc = ToDusTimeoutError(str(e))
                if attempt < self.max_retries - 1: time.sleep(2 ** attempt)
                else: raise last_exc
            except requests.ConnectionError as e:
                last_exc = ToDusConnectionError(str(e))
                if attempt < self.max_retries - 1: time.sleep(2 ** attempt)
                else: raise last_exc
        raise last_exc or ToDusError("Error desconocido")
    def put(self, local_path: str, key: str, content_type: Optional[str] = None,
            filename: Optional[str] = None, inline: bool = False,
            show_progress: bool = True, extra_headers: Optional[Dict[str, str]] = None) -> Dict:
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Archivo no encontrado: {local_path}")
        if content_type is None: content_type = guess_content_type(local_path)
        if filename is None: filename = os.path.basename(local_path)
        size = os.path.getsize(local_path)
        url = self._build_url(key)
        headers = {"Content-Type": content_type, "Content-Disposition": content_disposition(filename, inline=inline)}
        if extra_headers: headers.update(extra_headers)
        start = time.time()
        try:
            if size < 10 * 1024 * 1024:
                with open(local_path, "rb") as f: data = f.read()
                r = self._request("PUT", url, data=data, headers=headers)
            else:
                r = self._put_streaming(local_path, url, headers, size, show_progress)
        except ToDusError: raise
        duration = time.time() - start
        success = r.status_code == 200
        etag = normalize_etag(r.headers.get("ETag", "")) if success else None
        return {"success": success, "etag": etag, "size": size, "duration": duration,
                "status_code": r.status_code, "error": None if success else f"HTTP {r.status_code}: {r.text[:200]}"}
    def _put_streaming(self, local_path: str, url: str, headers: Dict[str, str],
                       size: int, show_progress: bool) -> requests.Response:
        chunk_size = self.chunk_size
        bar = progress_bar(total=size, desc=f"↑ {os.path.basename(local_path)}", color="green", enabled=show_progress) if show_progress else None
        class _StreamGen:
            def __init__(self, fileobj, bar):
                self._f = fileobj; self._bar = bar; self._chunk = chunk_size
            def read(self, size=-1):
                chunk = self._f.read(self._chunk if size == -1 else size)
                if chunk and self._bar: self._bar.update(len(chunk))
                return chunk
            def __iter__(self):
                while True:
                    chunk = self._f.read(self._chunk)
                    if not chunk: break
                    if self._bar: self._bar.update(len(chunk))
                    yield chunk
            def __getattr__(self, name): return getattr(self._f, name)
            def close(self): self._f.close()
        try:
            with open(local_path, "rb") as f:
                wrapper = _StreamGen(f, bar)
                r = self._request("PUT", url, data=wrapper, headers=headers)
        finally:
            if bar: bar.close()
        return r
    def head(self, key: str) -> Optional[Dict]:
        url = self._build_url(key)
        try: r = self._request("HEAD", url)
        except ToDusError: return None
        if r.status_code != 200: return None
        return {"size": int(r.headers.get("Content-Length", 0)),
                "content_type": r.headers.get("Content-Type"),
                "etag": normalize_etag(r.headers.get("ETag", "")),
                "last_modified": r.headers.get("Last-Modified"),
                "content_disposition": r.headers.get("Content-Disposition")}
    def exists(self, key: str) -> bool:
        try:
            r = self._request("HEAD", self._build_url(key))
            return r.status_code == 200
        except ToDusError: return False
    def build_url(self, key: str) -> str:
        return self._build_url(key)
