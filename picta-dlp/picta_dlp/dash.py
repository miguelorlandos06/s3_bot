import os
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

NS = '{urn:mpeg:dash:schema:mpd:2011}'


class DashError(Exception):
    pass


class Representation:
    def __init__(self, rep_id, bandwidth, width, height, codecs, mime, lang,
                 base_url, init_url, segment_urls):
        self.id = rep_id
        self.bandwidth = bandwidth or 0
        self.width = width
        self.height = height
        self.codecs = codecs
        self.mime = mime
        self.lang = lang
        self.base_url = base_url
        self.init_url = init_url
        self.segment_urls = segment_urls

    @property
    def is_video(self):
        return (self.mime or '').startswith('video/')

    @property
    def is_audio(self):
        return (self.mime or '').startswith('audio/')

    @property
    def is_subtitle(self):
        return (self.mime or '').startswith('text/') or (self.codecs or '') in ('wvtt', 'stpp')

    def __repr__(self):
        if self.is_subtitle:
            return f'subs {self.lang or "und"} [{self.codecs}]'
        if self.is_video:
            return f'video {self.width}x{self.height} @{self.bandwidth//1000}k [{self.codecs}]'
        return f'audio {self.lang or "und"} @{self.bandwidth//1000}k [{self.codecs}]'


def _resolve_url(base, url):
    return urllib.parse.urljoin(base, url) if url else base


def _prepare_template(template, rep_id, bandwidth):
    return (template.replace('$RepresentationID$', rep_id or '')
                    .replace('$Bandwidth$', str(bandwidth or 0)))


def _expand_segments(template, rep_id, bandwidth, start, count):
    return [template.replace('$Number$', str(n))
                    .replace('$RepresentationID$', rep_id or '')
                    .replace('$Bandwidth$', str(bandwidth or 0))
            for n in range(start, start + count)]


class MPDParser:
    def __init__(self, mpd_url, mpd_text):
        self.mpd_url = mpd_url
        self.mpd_base = mpd_url.rsplit('/', 1)[0] + '/'
        try:
            self.root = ET.fromstring(mpd_text)
        except ET.ParseError as e:
            raise DashError(f'MPD invalido: {e}')

    def representations(self):
        reps = []
        for period in self.root.findall(f'{NS}Period'):
            for aset in period.findall(f'{NS}AdaptationSet'):
                lang = aset.get('lang')
                mime = aset.get('mimeType')
                for rep in aset.findall(f'{NS}Representation'):
                    reps.append(self._parse_representation(rep, aset, lang, mime))
        return reps

    def _parse_representation(self, rep, aset, lang, mime):
        rep_id = rep.get('id')
        bandwidth = int(rep.get('bandwidth') or 0)
        width = int(rep.get('width') or aset.get('width') or 0) or None
        height = int(rep.get('height') or aset.get('height') or 0) or None
        codecs = rep.get('codecs') or aset.get('codecs')
        mime = rep.get('mimeType') or mime
        lang = rep.get('lang') or lang

        base = self.mpd_base
        for elem in (rep, aset):
            bu = elem.find(f'{NS}BaseURL')
            if bu is not None and bu.text:
                base = _resolve_url(base, bu.text.strip())
                break

        st = rep.find(f'{NS}SegmentTemplate') or aset.find(f'{NS}SegmentTemplate')
        init_url = None
        segment_urls = []

        if st is not None:
            init_tpl = st.get('initialization')
            media_tpl = st.get('media')
            start_number = int(st.get('startNumber') or 1)
            timescale = int(st.get('timescale') or 1)
            duration = int(st.get('duration') or 0)

            timeline = st.find(f'{NS}SegmentTimeline')
            if timeline is not None and media_tpl:
                segs = []
                for s in timeline.findall(f'{NS}S'):
                    d = int(s.get('d') or 0)
                    r = int(s.get('r') or 0)
                    for _ in range(r + 1):
                        segs.append(d)
                if init_tpl:
                    init_url = _resolve_url(base, _prepare_template(init_tpl, rep_id, bandwidth))
                segment_urls = [
                    _resolve_url(base, _prepare_template(media_tpl, rep_id, bandwidth)
                                 .replace('$Time$', str(i * (segs[0] if segs else 0))))
                    for i in range(len(segs))
                ]
            elif media_tpl and duration > 0:
                mpd_dur = self.root.get('mediaPresentationDuration', 'PT0S')
                m = re.search(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?', mpd_dur)
                total = 0
                if m:
                    total = (int(m.group(1) or 0) * 3600
                             + int(m.group(2) or 0) * 60
                             + float(m.group(3) or 0))
                if total > 0:
                    seg_dur = duration / timescale
                    count = max(1, int(total / seg_dur) + 1)
                    if init_tpl:
                        init_url = _resolve_url(base, _prepare_template(init_tpl, rep_id, bandwidth))
                    segment_urls = _expand_segments(media_tpl, rep_id, bandwidth,
                                                    start_number, int(count))
        else:
            sl = rep.find(f'{NS}SegmentList') or aset.find(f'{NS}SegmentList')
            if sl is not None:
                init = sl.find(f'{NS}Initialization')
                if init is not None and init.get('sourceURL'):
                    init_url = _resolve_url(base, init.get('sourceURL'))
                for seg in sl.findall(f'{NS}SegmentURL'):
                    if seg.get('media'):
                        segment_urls.append(_resolve_url(base, seg.get('media')))
            else:
                bu = rep.find(f'{NS}BaseURL')
                if bu is not None and bu.text:
                    segment_urls = [_resolve_url(base, bu.text.strip())]

        return Representation(rep_id, bandwidth, width, height, codecs, mime, lang,
                              base, init_url, segment_urls)


def _download(url, dest, session, headers, retries=3):
    if dest.exists() and dest.stat().st_size > 0:
        return
    for attempt in range(retries):
        try:
            r = session.get(url, headers=headers, stream=True, timeout=60)
            r.raise_for_status()
            tmp = dest.with_suffix(dest.suffix + '.part')
            with open(tmp, 'wb') as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
            tmp.replace(dest)
            return
        except (requests.RequestException, OSError) as e:
            if attempt == retries - 1:
                raise DashError(f'Fallo descarga {url}: {e}')
            time.sleep(1 + attempt)


def download_representation(rep, out_dir, session, headers, workers=4, verbose=True,
                            progress_cb=None):
    import sys
    from concurrent.futures import ThreadPoolExecutor, as_completed
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not rep.segment_urls:
        raise DashError(f'Representacion {rep.id} sin segmentos')

    init_file = out_dir / 'init.mp4'
    if rep.init_url:
        _download(rep.init_url, init_file, session, headers)
    else:
        init_file.touch()

    seg_files = []
    total = len(rep.segment_urls)
    if verbose:
        print(f'  Descargando {total} segmentos de {rep}...', flush=True)

    def fetch(i_url):
        i, url = i_url
        dest = out_dir / f'seg-{i:05d}.m4s'
        _download(url, dest, session, headers)
        return dest

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch, (i, u)) for i, u in enumerate(rep.segment_urls)]
        done = 0
        for fut in as_completed(futures):
            seg_files.append(fut.result())
            done += 1
            # Imprimir progreso en cada segmento para que el bot lo lea
            print(f'PROGRESO:{done}/{total}', flush=True)
            if progress_cb:
                progress_cb(done, total)

    if verbose:
        print(f'    {total}/{total} segmentos OK', flush=True)

    out_file = out_dir / f'stream-{rep.id}.mp4'
    with open(out_file, 'wb') as f:
        f.write(init_file.read_bytes())
        for sf in sorted(seg_files):
            f.write(sf.read_bytes())
    return out_file
