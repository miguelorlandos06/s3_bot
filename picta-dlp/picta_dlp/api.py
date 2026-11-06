from .auth import PictaAuth

API_BASE_V2 = 'https://api.picta.cu/v2/'
API_BASE_V1 = 'https://api.picta.cu/v1/'


class PictaAPIError(Exception):
    pass


class PictaAPI:
    def __init__(self, auth: PictaAuth):
        self.auth = auth
        self.session = auth.session

    def _get(self, base_url, path, params):
        url = f'{base_url}{path}'

        # Forzar la obtención de un token fresco antes de cada petición
        # (Esto evita problemas de caché o tokens expirados)
        token = self.auth.get_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'X-Device-Id': self.auth.device_id,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:154.0) Gecko/20100101 Firefox/154.0',
            'Accept': 'application/json',
        }

        res = self.session.get(url, params=params, headers=headers, timeout=30)

        if res.status_code == 401:
            # Si el token falla, forzamos login de nuevo y reintentamos
            self.auth._token = None
            self.auth._token_exp = 0
            self.auth._login() # Login explícito
            token = self.auth.get_token()
            headers['Authorization'] = f'Bearer {token}'
            res = self.session.get(url, params=params, headers=headers, timeout=30)

        if res.status_code != 200:
            raise PictaAPIError(f'API {path} -> {res.status_code}: {res.text[:200]}')
        return res.json()

    def _results(self, base_url, path, params):
        data = self._get(base_url, path, params)
        results = data.get('results') or []
        return results[0] if results else None

    def _fix_manifest_url(self, pub):
        """Corrige la URL del manifiesto si viene con www.picta.cu en lugar de videos.picta.cu."""
        if pub and 'url_manifiesto' in pub and pub['url_manifiesto']:
            url = pub['url_manifiesto']
            # Si la URL apunta a www.picta.cu/videos, forzar el dominio correcto
            if 'www.picta.cu/videos/' in url:
                pub['url_manifiesto'] = url.replace('https://www.picta.cu/videos/', 'https://videos.picta.cu/videos/')
            elif url.startswith('/'):
                pub['url_manifiesto'] = 'https://videos.picta.cu' + url
            elif url.startswith('videos/'):
                pub['url_manifiesto'] = 'https://videos.picta.cu/' + url
        return pub

    def get_publicacion(self, slug_or_id):
        if str(slug_or_id).isdigit():
            params = {'format': 'json', 'id': slug_or_id}
        else:
            params = {'format': 'json', 'slug_url_raw': slug_or_id}

        try:
            pub = self._results(API_BASE_V2, 'publicacion/', params)
            if pub:
                return self._fix_manifest_url(pub)
        except PictaAPIError:
            pass

        try:
            pub = self._results(API_BASE_V1, 'publicacion/', params)
            if pub:
                return self._fix_manifest_url(pub)
        except PictaAPIError:
            pass

        raise PictaAPIError(f'Video no encontrado: {slug_or_id}')

    def get_playlist(self, playlist_id):
        for endpoint in ('lista_reproduccion_canal/', 'lista_reproduccion/'):
            pl = self._results(API_BASE_V2, endpoint, {'format': 'json', 'id': playlist_id})
            if pl:
                pl['_endpoint'] = endpoint
                return pl
        raise PictaAPIError(f'Playlist no encontrada: {playlist_id}')

    def playlist_entries(self, pl):
        key = ('publicaciones' if pl.get('_endpoint') == 'lista_reproduccion_canal/'
               else 'publicacion')
        return pl.get(key) or []

    def resolve_slug(self, video_id):
        try:
            return self.get_publicacion(str(video_id)).get('slug_url')
        except PictaAPIError:
            return None
