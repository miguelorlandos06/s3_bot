from __future__ import annotations
import sys, time
from typing import Optional
try:
    from tqdm import tqdm as _tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
class _TextProgressBar:
    def __init__(self, total: int, desc: str = "", width: int = 30, unit: str = "B", unit_scale: bool = True):
        self.total = total; self.desc = desc; self.width = width
        self.unit = unit; self.unit_scale = unit_scale
        self.n = 0; self._last_print = 0.0; self._start = time.time()
    def _format_size(self, n: int) -> str:
        if not self.unit_scale: return f"{n} {self.unit}"
        units = ["B", "KB", "MB", "GB", "TB"]; f = float(n); i = 0
        while f >= 1024 and i < len(units) - 1: f /= 1024; i += 1
        return f"{f:.1f}{units[i]}"
    def update(self, n: int = 1) -> None:
        self.n += n; now = time.time()
        if now - self._last_print < 0.2 and self.n < self.total: return
        self._last_print = now; self._render()
    def _render(self) -> None:
        if self.total <= 0: pct = 100; bar = "?" * self.width
        else:
            pct = min(100, int(self.n * 100 / self.total))
            filled = int(self.width * self.n / max(1, self.total))
            bar = "█" * filled + "░" * (self.width - filled)
        elapsed = time.time() - self._start
        speed = self.n / elapsed if elapsed > 0 else 0
        sys.stdout.write(f"\r{self.desc[:20]:<20} {bar} {pct:3d}% {self._format_size(self.n)}/{self._format_size(self.total)} {self._format_size(int(speed))}/s")
        sys.stdout.flush()
    def close(self) -> None:
        self._render(); sys.stdout.write("\n"); sys.stdout.flush()
    def __enter__(self): return self
    def __exit__(self, *exc): self.close()
class _DummyBar:
    def update(self, n: int = 1) -> None: pass
    def close(self) -> None: pass
    def __enter__(self): return self
    def __exit__(self, *exc): pass
def progress_bar(total: int, desc: str = "", unit: str = "B", unit_scale: bool = True, enabled: bool = True, color: Optional[str] = None):
    if not enabled: return _DummyBar()
    if HAS_TQDM:
        kwargs = dict(total=total, unit=unit, unit_scale=unit_scale, desc=desc, leave=True)
        if color: kwargs["colour"] = color
        return _tqdm(**kwargs)
    return _TextProgressBar(total=total, desc=desc, unit=unit, unit_scale=unit_scale)
