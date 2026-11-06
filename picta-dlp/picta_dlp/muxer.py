import shutil
import subprocess
from pathlib import Path


class MuxError(Exception):
    pass


def find_ffmpeg():
    path = shutil.which('ffmpeg')
    if path:
        return path
    raise MuxError('ffmpeg no encontrado en PATH. Instala con "pkg install ffmpeg" en Termux')


def mux(video_path, audio_path, output_path):
    ffmpeg = find_ffmpeg()
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [ffmpeg, '-y', '-loglevel', 'error']
    if video_path:
        cmd += ['-i', str(video_path)]
    if audio_path:
        cmd += ['-i', str(audio_path)]
    cmd += ['-c', 'copy']
    if video_path and audio_path:
        cmd += ['-map', '0:v:0', '-map', '1:a:0']
    cmd += [str(output_path)]

    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise MuxError(f'ffmpeg fallo: {res.stderr[:500]}')
    return output_path
