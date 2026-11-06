import re
from pathlib import Path


def download_subtitle(url, dest, session, headers):
    res = session.get(url, headers=headers, timeout=30)
    res.raise_for_status()
    text = res.text
    if text.lstrip().startswith('WEBVTT'):
        text = re.sub(r'^WEBVTT[^\n]*\n+', '', text)
        text = re.sub(r'(\d{2}:\d{2}:\d{2})\.(\d{3})', r'\1,\2', text)
    Path(dest).write_text(text, encoding='utf-8')
    return Path(dest)
