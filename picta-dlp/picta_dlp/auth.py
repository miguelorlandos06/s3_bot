import json
import os
import time
from pathlib import Path

import requests

CLIENT_ID = 'ebkU3YeFu3So9hesQHrS8AZjEa4v7TiYbS5QZIgO'
TOKEN_URL = 'https://api.picta.cu/o/token/'
CACHE_DIR = Path.home() / '.cache' / 'picta-dlp'


class AuthError(Exception):
    pass


class PictaAuth:
    def __init__(self, username, password=None, device_id='1165077'):
        self.username = username
        self.password = password
        self.device_id = device_id
        self.session = requests.Session()
        # Forzar User-Agent de navegador real para evitar bloqueos
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:154.0) Gecko/20100101 Firefox/154.0',
            'X-Device-Id': self.device_id,
        })
        self._token = None
        self._refresh_token = None
        self._token_exp = 0
        self._cache_file = CACHE_DIR / f'token-{username}.json'

    def _load_cache(self):
        if not self._cache_file.exists():
            return False
        try:
            data = json.loads(self._cache_file.read_text())
        except (OSError, json.JSONDecodeError):
            return False
        if data.get('exp', 0) > time.time():
            self._token = data['access']
            self._refresh_token = data.get('refresh')
            self._token_exp = data['exp']
            return True
        return False

    def _save_cache(self):
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._cache_file.write_text(json.dumps({
            'access': self._token,
            'refresh': self._refresh_token,
            'exp': self._token_exp,
        }))
        try:
            os.chmod(self._cache_file, 0o600)
        except OSError:
            pass

    def _post_token(self, data):
        files = {k: (None, v) for k, v in data.items()}
        res = self.session.post(TOKEN_URL, files=files, timeout=30)
        if res.status_code != 200:
            raise AuthError(f'Login fallo ({res.status_code}): {res.text[:200]}')
        return res.json()

    def _login(self):
        if not self.password:
            raise AuthError('Se requiere contrasena para login')
        payload = self._post_token({
            'grant_type': 'password',
            'client_id': CLIENT_ID,
            'username': self.username,
            'password': self.password,
        })
        self._apply(payload)

    def _refresh(self):
        if not self._refresh_token:
            return False
        try:
            payload = self._post_token({
                'grant_type': 'refresh_token',
                'client_id': CLIENT_ID,
                'refresh_token': self._refresh_token,
            })
        except AuthError:
            self._refresh_token = None
            return False
        self._apply(payload)
        return True

    def _apply(self, payload):
        self._token = payload['access_token']
        self._refresh_token = payload.get('refresh_token')
        self._token_exp = time.time() + int(payload.get('expires_in', 36000)) - 60
        self._save_cache()

    def get_token(self):
        if self._token and time.time() < self._token_exp:
            return self._token
        if self._refresh():
            return self._token
        if not self._load_cache():
            self._login()
        return self._token

    def headers(self, json=True):
        h = {
            'Authorization': f'Bearer {self.get_token()}',
            'X-Device-Id': self.device_id,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:154.0) Gecko/20100101 Firefox/154.0',
        }
        if json:
            h['Accept'] = 'application/json'
        return h
