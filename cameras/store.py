"""Persistência simples em JSON (data/cameras.json e data/config.json)."""
import json
import os
import threading
import uuid

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
_lock = threading.Lock()


def _path(name: str) -> str:
    return os.path.join(DATA_DIR, name)


def _load(name: str, default):
    try:
        with open(_path(name), encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _save(name: str, data):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = _path(name + '.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, _path(name))


def list_cameras() -> list[dict]:
    return _load('cameras.json', [])


def get_camera(cam_id: str) -> dict | None:
    return next((c for c in list_cameras() if c['id'] == cam_id), None)


def add_camera(name: str, rtsp_main: str, rtsp_sub: str = '', ip: str = '') -> dict:
    with _lock:
        cams = list_cameras()
        cam = {'id': uuid.uuid4().hex, 'name': name, 'ip': ip, 'rtsp_main': rtsp_main, 'rtsp_sub': rtsp_sub}
        cams.append(cam)
        _save('cameras.json', cams)
        return cam


def update_camera(cam_id: str, **fields) -> dict | None:
    with _lock:
        cams = list_cameras()
        for c in cams:
            if c['id'] == cam_id:
                c.update({k: v for k, v in fields.items() if k in ('name', 'rtsp_main', 'rtsp_sub')})
                _save('cameras.json', cams)
                return c
    return None


def delete_camera(cam_id: str) -> bool:
    with _lock:
        cams = list_cameras()
        rest = [c for c in cams if c['id'] != cam_id]
        _save('cameras.json', rest)
        return len(rest) != len(cams)


def get_config() -> dict:
    return {'user': 'admin', 'password': '', 'subnet': '', **_load('config.json', {})}


def set_config(**fields):
    with _lock:
        cfg = get_config()
        cfg.update(fields)
        _save('config.json', cfg)
