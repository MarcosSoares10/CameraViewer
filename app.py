"""CameraViewer: encontra câmeras IP (ONVIF/RTSP) na rede local e mostra o vídeo ao vivo no navegador."""
import logging
import os
import threading

from flask import Flask, Response, abort, jsonify, render_template, request

from cameras import store
from cameras.discovery import discover, local_networks, strip_credentials
from cameras.stream import StreamManager, test_url

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PORT = int(os.environ.get('PORT', 8092))
# Só esta máquina por padrão: o app expõe o vídeo e as senhas das câmeras.
# Para abrir no celular, rode com HOST=0.0.0.0.
HOST = os.environ.get('HOST', '127.0.0.1')
GRID_WIDTH = 960  # largura máxima dos quadros na grade (a tela cheia usa a resolução original)

app = Flask(__name__)
streams = StreamManager()

_scan = {'running': False, 'message': '', 'results': [], 'error': ''}
_scan_lock = threading.Lock()


def _public(cam: dict) -> dict:
    return {**cam, 'display_url': strip_credentials(cam['rtsp_main'])}


@app.get('/')
def index():
    return render_template('index.html')


# ------------------------------------------------------------ câmeras salvas

@app.get('/api/cameras')
def api_cameras():
    return jsonify([_public(c) for c in store.list_cameras()])


@app.post('/api/cameras')
def api_add_camera():
    data = request.get_json(force=True)
    url = (data.get('rtsp_main') or '').strip()
    if not url.lower().startswith(('rtsp://', 'rtsps://', 'http://', 'https://')):
        return jsonify(error='Informe uma URL rtsp:// ou http://'), 400
    cam = store.add_camera(name=(data.get('name') or 'Câmera').strip(), rtsp_main=url,
                           rtsp_sub=(data.get('rtsp_sub') or '').strip(), ip=data.get('ip', ''))
    return jsonify(_public(cam))


@app.patch('/api/cameras/<cam_id>')
def api_update_camera(cam_id):
    cam = store.update_camera(cam_id, **request.get_json(force=True))
    return jsonify(_public(cam)) if cam else (jsonify(error='Câmera não encontrada'), 404)


@app.delete('/api/cameras/<cam_id>')
def api_delete_camera(cam_id):
    return jsonify(ok=store.delete_camera(cam_id))


@app.post('/api/test')
def api_test():
    url = (request.get_json(force=True).get('url') or '').strip()
    return jsonify(test_url(url) if url else {'ok': False, 'error': 'URL vazia'})


# ------------------------------------------------------------ descoberta

@app.get('/api/config')
def api_config():
    cfg = store.get_config()
    return jsonify(user=cfg['user'], has_password=bool(cfg['password']), subnet=cfg['subnet'],
                   networks=[str(n) for n in local_networks()])


def _run_scan(user, password, subnet):
    def progress(msg):
        _scan['message'] = msg
    try:
        found = discover(user, password, subnet, progress)
        known = {strip_credentials(c['rtsp_main']) for c in store.list_cameras()}
        for c in found:
            c['saved'] = strip_credentials(c['rtsp_main']) in known
            c['display_url'] = strip_credentials(c['rtsp_main'])
        _scan.update(results=found, message=f'{len(found)} câmera(s) encontrada(s)')
    except Exception as e:  # noqa: BLE001 - mostra qualquer falha na interface
        logger.exception('Falha na busca')
        _scan.update(error=str(e), message='')
    finally:
        _scan['running'] = False


@app.post('/api/scan')
def api_scan_start():
    data = request.get_json(force=True)
    cfg = store.get_config()
    user = data.get('user', cfg['user']).strip()
    # senha em branco no formulário = reaproveita a salva
    password = data.get('password') or cfg['password']
    subnet = (data.get('subnet') or '').strip()
    store.set_config(user=user, password=password, subnet=subnet)
    with _scan_lock:
        if _scan['running']:
            return jsonify(error='Já existe uma busca em andamento'), 409
        _scan.update(running=True, message='Iniciando...', results=[], error='')
    threading.Thread(target=_run_scan, args=(user, password, subnet), daemon=True).start()
    return jsonify(ok=True)


@app.get('/api/scan')
def api_scan_status():
    return jsonify({k: _scan[k] for k in ('running', 'message', 'error', 'results')})


# ------------------------------------------------------------ vídeo

def _stream_for(cam_id: str, quality: str):
    cam = store.get_camera(cam_id) or abort(404)
    if quality == 'main':
        return streams.get(cam['rtsp_main'])
    # grade: usa o sub-stream (mais leve) quando existir
    return streams.get(cam.get('rtsp_sub') or cam['rtsp_main'], GRID_WIDTH)


@app.get('/video/<cam_id>')
def video(cam_id):
    stream = _stream_for(cam_id, request.args.get('q', 'grid'))
    return Response(stream.mjpeg(), mimetype='multipart/x-mixed-replace; boundary=frame',
                    headers={'Cache-Control': 'no-store'})


@app.get('/frame/<cam_id>')
def frame(cam_id):
    """Próximo quadro depois de `after` (long-poll). A página usa isto em vez do MJPEG para não
    esbarrar no limite de ~6 conexões simultâneas por servidor do navegador."""
    stream = _stream_for(cam_id, request.args.get('q', 'grid'))
    after = request.args.get('after', type=int, default=-1)
    stream.touch()
    with stream.cond:
        stream.cond.wait_for(lambda: stream.seq != after, timeout=2)
        seq, jpeg, status = stream.seq, stream.current_jpeg(), stream.status
    w, h = stream.size
    return Response(jpeg, mimetype='image/jpeg', headers={
        'Cache-Control': 'no-store', 'X-Seq': str(seq), 'X-Status': status.encode('ascii', 'ignore').decode(),
        'X-Size': f'{w}x{h}'})


@app.get('/snapshot/<cam_id>')
def snapshot(cam_id):
    stream = _stream_for(cam_id, 'main')
    stream.touch()
    with stream.cond:
        stream.cond.wait_for(lambda: stream.jpeg is not None, timeout=10)
    return Response(stream.current_jpeg(), mimetype='image/jpeg',
                    headers={'Content-Disposition': f'attachment; filename=snapshot_{cam_id[:8]}.jpg'})


if __name__ == '__main__':
    print(f'CameraViewer em http://{"localhost" if HOST == "127.0.0.1" else HOST}:{PORT}')
    app.run(host=HOST, port=PORT, threaded=True)
