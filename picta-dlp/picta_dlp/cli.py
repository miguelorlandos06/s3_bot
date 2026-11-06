import argparse
import getpass
import json
import re
import sys
from pathlib import Path

from .api import PictaAPI, PictaAPIError
from .auth import PictaAuth, AuthError
from .dash import MPDParser, DashError, download_representation
from .muxer import MuxError, mux
from .subtitles import download_subtitle

URL_RE = re.compile(
    r'https?://(?:www\.)?picta\.cu/(?:medias|movie|embed|publicacion)/'
    r'(?:\?v=)?(?P<id>[\w-]+)(?:\?playlist=(?P<playlist>[\w-]+))?')


def _parse_url(url):
    m = URL_RE.search(url)
    if not m:
        raise SystemExit(f'URL no reconocida: {url}')
    return m.group('id'), m.group('playlist')


def _sanitize(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name).strip() or 'video'


def _load_reps(api, manifest_url):
    r = api.session.get(manifest_url, headers=api.auth.headers(json=False), timeout=60)
    r.raise_for_status()
    return MPDParser(manifest_url, r.text).representations()


def _fmt_duration(pub):
    d = pub.get('duracion')
    if isinstance(d, (int, float)) and d > 0:
        m, s = divmod(int(d), 60)
        return f'{m:02d}:{s:02d}'
    return pub.get('duracion') or '-'


def _build_sheet(pub, reps=None):
    cat = (pub.get('categoria') or {})
    tip = (cat.get('tipologia') or {})
    sheet = {
        'id': pub.get('id'),
        'slug_url': pub.get('slug_url'),
        'title': pub.get('nombre'),
        'description': pub.get('descripcion'),
        'uploader': pub.get('usuario').get('username') if isinstance(pub.get('usuario'), dict) else pub.get('usuario'),
        'fecha_creacion': pub.get('fecha_creacion'),
        'duration': _fmt_duration(pub),
        'categoria': cat.get('nombre'),
        'tipologia': tip.get('nombre'),
        'thumbnail': pub.get('url_imagen'),
        'manifest_url': pub.get('url_manifiesto'),
        'subtitle_url': pub.get('url_subtitulo'),
        'has_subtitles': bool(pub.get('url_subtitulo')),
    }
    if reps is not None:
        sheet['formatos'] = [
            {
                'id': rep.id,
                'tipo': 'subs' if rep.is_subtitle else ('video' if rep.is_video else 'audio'),
                'resolucion': f'{rep.width}x{rep.height}' if rep.width else None,
                'idioma': rep.lang,
                'bitrate_kbps': rep.bandwidth // 1000,
                'codecs': rep.codecs,
            }
            for rep in reps
        ]
    return sheet


def _cmd_info(api, target, args):
    pub = api.get_publicacion(target)
    reps = None
    if pub.get('url_manifiesto'):
        try:
            reps = _load_reps(api, pub['url_manifiesto'])
        except Exception as e:
            print(f'[aviso] No se pudieron leer formatos: {e}', file=sys.stderr)
    sheet = _build_sheet(pub, reps)
    if args.human:
        print(f'ID:          {sheet["id"]}')
        print(f'Slug:        {sheet["slug_url"]}')
        print(f'Titulo:      {sheet["title"]}')
        print(f'Uploader:    {sheet["uploader"]}')
        print(f'Fecha:       {sheet["fecha_creacion"]}')
        print(f'Duracion:    {sheet["duration"]}')
        print(f'Categoria:   {sheet["categoria"]} / {sheet["tipologia"]}')
        print(f'Thumbnail:   {sheet["thumbnail"]}')
        print(f'Manifest:    {sheet["manifest_url"]}')
        print(f'Subtitulos:  {"si" if sheet["has_subtitles"] else "no"}')
        if sheet.get('description'):
            print(f'\nDescripcion:\n{sheet["description"]}')
        if sheet.get('formatos'):
            print(f'\nFormatos:')
            for f in sheet['formatos']:
                res = f['resolucion'] or f['idioma'] or '-'
                print(f'  {f["id"]:<24} {f["tipo"]:<6} {res:<12} {f["bitrate_kbps"]:>6}k  {f["codecs"]}')
    else:
        print(json.dumps(sheet, indent=2, ensure_ascii=False))


def _cmd_list(api, target, args):
    pub = api.get_publicacion(target)
    if not pub.get('url_manifiesto'):
        raise PictaAPIError(f'Sin manifiesto para {target}')
    reps = _load_reps(api, pub['url_manifiesto'])
    
    cat = (pub.get('categoria') or {})
    tip = (cat.get('tipologia') or {})
    nombre_cat = cat.get('nombre') or tip.get('nombre') or '-'
    thumb = pub.get('url_imagen') or '-'
    desc = pub.get('descripcion') or ''
    
    print(f'\nTitulo: {pub.get("nombre")}  (id={pub.get("id")})')
    print(f'Duracion: {_fmt_duration(pub)}   Categoria: {nombre_cat}')
    print(f'Thumbnail: {thumb}')
    if desc:
        print(f'Descripcion: {desc}')
    print(f'Subtitulos: {"si" if pub.get("url_subtitulo") else "no"}\n')
    
    print(f'{"ID":<24} {"TIPO":<6} {"RESOLUCION/IDIOMA":<18} {"BITRATE":<9} {"CODECS"}')
    print('-' * 84)
    for r in sorted(reps, key=lambda x: (x.is_subtitle, x.is_audio, x.height or 0, x.bandwidth)):
        res = f'{r.width}x{r.height}' if r.width else (r.lang or '-')
        tipo = 'subs' if r.is_subtitle else ('audio' if r.is_audio else 'video')
        print(f'{r.id:<24} {tipo:<6} {res:<18} {r.bandwidth//1000:>6}k   {r.codecs}')
    if pub.get('url_subtitulo'):
        print('\nSubtitulos disponibles (via --subs):')
        print(f'  URL: {pub["url_subtitulo"]}')
    print()


def _pick(reps, fmt_id, audio_lang):
    videos = [r for r in reps if r.is_video]
    audios = [r for r in reps if r.is_audio]
    if fmt_id:
        chosen = [r for r in reps if r.id == fmt_id]
        if not chosen:
            raise DashError(f'Formato no encontrado: {fmt_id}')
        sel = chosen[0]
        if sel.is_video:
            return sel, (audios[0] if audios else None)
        return None, sel
    if audio_lang:
        audio_match = [r for r in audios if (r.lang or '').lower().startswith(audio_lang.lower())]
        audio = (audio_match or audios or [None])[0]
    else:
        audio = audios[0] if audios else None
    video = max(videos, key=lambda r: (r.height or 0, r.bandwidth)) if videos else None
    return video, audio


def _cmd_download(api, target, args):
    pub = api.get_publicacion(target)
    title = pub.get('nombre') or str(target)
    manifest_url = pub.get('url_manifiesto')
    if not manifest_url:
        raise PictaAPIError(f'Sin manifiesto para {target}')

    print(f'\n> {title} (id={pub.get("id")})')
    reps = _load_reps(api, manifest_url)
    video, audio = _pick(reps, args.format, args.audio_lang)
    if video:
        print(f'  Video: {video}')
    if audio:
        print(f'  Audio: {audio}')

    safe = _sanitize(title)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.subs_only:
        sub_url = pub.get('url_subtitulo')
        if not sub_url:
            raise PictaAPIError('Este video no tiene subtitulos')
        dest = out_dir / f'{safe} [{pub.get("id")}].srt'
        download_subtitle(sub_url, dest, api.session, api.auth.headers(json=False))
        print(f'  Subtitulos -> {dest}')
        return

    work = Path(args.tmpdir) / str(pub.get('id'))
    work.mkdir(parents=True, exist_ok=True)

    v_file = a_file = None
    if video:
        v_file = download_representation(video, work / 'video',
                                         api.session, api.auth.headers(json=False),
                                         workers=args.workers)
    if audio:
        a_file = download_representation(audio, work / 'audio',
                                         api.session, api.auth.headers(json=False),
                                         workers=args.workers)

    if args.audio_only:
        if not a_file:
            raise DashError('No hay pista de audio')
        out = out_dir / f'{safe} [{pub.get("id")}].m4a'
        a_file.replace(out)
    else:
        out = out_dir / f'{safe} [{pub.get("id")}].mp4'
        print(f'  Muxeando -> {out}')
        mux(v_file, a_file, out)

    if args.write_info_json:
        sheet = _build_sheet(pub, reps)
        info_path = out_dir / f'{safe} [{pub.get("id")}].info.json'
        info_path.write_text(json.dumps(sheet, indent=2, ensure_ascii=False), encoding='utf-8')
        print(f'  info.json -> {info_path}')

    if args.write_thumbnail and pub.get('url_imagen'):
        try:
            r = api.session.get(pub['url_imagen'], timeout=30)
            r.raise_for_status()
            ext = 'png' if '.png' in pub['url_imagen'] else 'jpg'
            thumb = out_dir / f'{safe} [{pub.get("id")}].{ext}'
            thumb.write_bytes(r.content)
            print(f'  thumbnail -> {thumb}')
        except Exception as e:
            print(f'  [aviso] thumbnail fallo: {e}', file=sys.stderr)

    # --- Descargar subtítulos si --subs está activo ---
    if args.subtitles and not args.audio_only and pub.get('url_subtitulo'):
        try:
            sub_out = out_dir / f'{safe} [{pub.get("id")}].srt'
            download_subtitle(pub['url_subtitulo'], sub_out,
                              api.session, api.auth.headers(json=False))
            print(f'  subtitulos -> {sub_out}')
        except Exception as e:
            print(f'  [aviso] subtitulos fallo: {e}', file=sys.stderr)

    if not args.keep_temp:
        import shutil
        shutil.rmtree(work, ignore_errors=True)

    if out.exists():
        mb = out.stat().st_size / (1024 * 1024)
        print(f'  OK: {out} ({mb:.1f} MB)')


def main(argv=None):
    p = argparse.ArgumentParser(
        prog='picta-dlp',
        description='Descargador autonomo para Picta (picta.cu)')
    p.add_argument('url', nargs='+', help='URL(s) de Picta')
    p.add_argument('-u', '--username', required=True)
    p.add_argument('-p', '--password')
    p.add_argument('-o', '--output', default='.')
    p.add_argument('-J', '--info', action='store_true',
                   help='Mostrar ficha tecnica (JSON) y salir')
    p.add_argument('--human', action='store_true',
                   help='Con -J, mostrar ficha en texto legible')
    p.add_argument('-F', '--list-formats', action='store_true',
                   help='Listar formatos y salir (incluye subtitulos si existen)')
    p.add_argument('-f', '--format', help='ID de formato a descargar (ver -F)')
    p.add_argument('--audio-lang', help='Idioma de audio preferido (ej: es, en)')
    p.add_argument('--audio-only', action='store_true', help='Solo audio (.m4a)')
    p.add_argument('--subs-only', action='store_true', help='Solo subtitulos (.srt)')
    p.add_argument('--subs', action='store_true', help='Descargar tambien los subtitulos')
    p.add_argument('--write-info-json', action='store_true')
    p.add_argument('--write-thumbnail', action='store_true')
    p.add_argument('--no-subs', dest='subtitles', action='store_false',
                   help='No descargar subtitulos (por defecto)')
    p.add_argument('--keep-temp', action='store_true')
    p.add_argument('-N', '--workers', type=int, default=4)
    p.add_argument('--tmpdir', default='/tmp/picta-dlp')
    p.add_argument('--device-id', default='1165077')
    p.add_argument('-q', '--quiet', action='store_true')
    args = p.parse_args(argv)

    # Por defecto, no descargar subtítulos automáticamente
    if not hasattr(args, 'subtitles') or args.subtitles is None:
        args.subtitles = False

    if not args.password and not (args.info or args.list_formats):
        args.password = getpass.getpass('Contrasena de Picta: ')

    try:
        auth = PictaAuth(args.username, args.password, args.device_id)
        api = PictaAPI(auth)

        targets = []
        for url in args.url:
            vid, pl = _parse_url(url)
            if pl:
                playlist = api.get_playlist(pl)
                entries = api.playlist_entries(playlist)
                print(f'Playlist: {playlist.get("nombre")} ({len(entries)} videos)')
                for e in entries:
                    slug = e.get('slug_url') or api.resolve_slug(e.get('id'))
                    if slug:
                        targets.append(slug)
            else:
                targets.append(vid)

        for i, t in enumerate(targets, 1):
            if len(targets) > 1:
                print(f'\n[{i}/{len(targets)}] {t}')
            try:
                if args.info:
                    _cmd_info(api, t, args)
                elif args.list_formats:
                    _cmd_list(api, t, args)
                else:
                    _cmd_download(api, t, args)
            except (PictaAPIError, DashError, MuxError) as e:
                print(f'  ERROR: {e}', file=sys.stderr)

    except AuthError as e:
        print(f'Error de autenticacion: {e}', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('\nInterrumpido', file=sys.stderr)
        return 130
    return 0


if __name__ == '__main__':
    sys.exit(main())
