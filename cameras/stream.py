"""Leitura dos streams RTSP com PyAV (FFmpeg) e reenvio como MJPEG para o navegador.

Cada URL tem um único leitor em segundo plano, compartilhado por todos os clientes que a
estiverem assistindo. O leitor reconecta sozinho se a câmera cair e para depois de alguns
segundos sem ninguém assistindo.
"""
import threading
import time

import av
import cv2
import numpy as np

IDLE_SECONDS = 15        # para o leitor quando ninguém assiste
MAX_FPS = 15             # limite de quadros codificados em JPEG
JPEG_QUALITY = 80
OPEN_TIMEOUT_MS = 8000
READ_TIMEOUT_MS = 8000

# Transporte RTSP que funcionou por último em cada URL ('tcp' ou 'udp').
_transport_ok: dict[str, str] = {}


def _placeholder(text: str, width: int = 640, height: int = 360) -> bytes:
    img = np.full((height, width, 3), 24, np.uint8)
    font = cv2.FONT_HERSHEY_SIMPLEX
    (tw, th), _ = cv2.getTextSize(text, font, 0.9, 2)
    cv2.putText(img, text, ((width - tw) // 2, (height + th) // 2), font, 0.9, (200, 200, 200), 2, cv2.LINE_AA)
    return cv2.imencode('.jpg', img)[1].tobytes()


_PLACEHOLDERS: dict[str, bytes] = {}


def placeholder(text: str) -> bytes:
    if text not in _PLACEHOLDERS:
        _PLACEHOLDERS[text] = _placeholder(text)
    return _PLACEHOLDERS[text]


CaptureError = (av.FFmpegError, OSError)


def _open(url: str, transport: str | None):
    opts = {'timeout': str(READ_TIMEOUT_MS * 1000)}  # microssegundos
    if transport:
        opts.update(rtsp_transport=transport, allowed_media_types='video')  # sem áudio
        if transport == 'udp':
            opts['buffer_size'] = str(4 << 20)  # evita perder pacotes de quadros grandes
    container = av.open(url, options=opts, timeout=(OPEN_TIMEOUT_MS / 1000, READ_TIMEOUT_MS / 1000))
    if not container.streams.video:
        container.close()
        raise av.InvalidDataError(0, 'O stream não tem vídeo')
    return container


def open_capture(url: str):
    """Abre a URL e devolve o container PyAV. Em RTSP tenta TCP (sem perda de pacotes) e cai
    para UDP: algumas câmeras (ex.: Yoosee, caminho /onvif1) respondem o SETUP por TCP sem o
    "/TCP" no cabeçalho e o FFmpeg recusa ("Nonmatching transport in server reply").
    O transporte que funcionou é lembrado e tentado primeiro na próxima vez."""
    if not url.lower().startswith(('rtsp://', 'rtsps://')):
        return _open(url, None)
    first = _transport_ok.get(url, 'tcp')
    error = None
    for transport in (first, 'udp' if first == 'tcp' else 'tcp'):
        try:
            container = _open(url, transport)
        except CaptureError as e:
            error = e
            continue
        _transport_ok[url] = transport
        return container
    raise error


def frames(container):
    """Quadros decodificados do primeiro stream de vídeo. Pacotes corrompidos (comuns em UDP)
    são pulados; erros de leitura (câmera caiu, timeout) são propagados."""
    stream = container.streams.video[0]
    stream.thread_type = 'AUTO'
    for packet in container.demux(stream):
        try:
            yield from packet.decode()
        except av.InvalidDataError:
            continue


def to_bgr(frame, max_width: int | None = None) -> np.ndarray:
    w, h = frame.width, frame.height
    if max_width and w > max_width:
        return frame.to_ndarray(width=max_width, height=h * max_width // w, format='bgr24', interpolation='AREA')
    return frame.to_ndarray(format='bgr24')


def test_url(url: str) -> dict:
    """Abre a URL e lê um quadro. Usado pelo botão "Testar"."""
    try:
        container = open_capture(url)
    except CaptureError:
        return {'ok': False, 'error': 'Não foi possível abrir o stream (URL, usuário ou senha incorretos?)'}
    try:
        for frame in frames(container):
            return {'ok': True, 'width': frame.width, 'height': frame.height, 'transport': _transport_ok.get(url, '')}
    except CaptureError:
        pass
    finally:
        container.close()
    return {'ok': False, 'error': 'Conectou, mas não recebeu vídeo'}


class CameraStream:
    def __init__(self, url: str, max_width: int | None = None):
        self.url = url
        self.max_width = max_width
        self.cond = threading.Condition()
        self.jpeg: bytes | None = None
        self.seq = 0
        self.status = 'Conectando...'
        self.size = (0, 0)
        self.last_access = time.time()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def touch(self):
        self.last_access = time.time()
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run, daemon=True, name=f'cam:{self.url[-30:]}')
                self._thread.start()

    def _idle(self) -> bool:
        return time.time() - self.last_access > IDLE_SECONDS

    def _publish(self, jpeg: bytes | None = None, status: str | None = None):
        with self.cond:
            if jpeg is not None:
                self.jpeg = jpeg
            if status is not None:
                self.status = status
            self.seq += 1
            self.cond.notify_all()

    def _run(self):
        self._publish(status='Conectando...')
        while not self._idle():
            try:
                container = open_capture(self.url)
            except CaptureError:
                self.jpeg = None
                self._publish(status='Sem sinal - tentando novamente')
                time.sleep(3)
                continue
            self._publish(status='Ao vivo')
            last = 0.0
            try:
                for frame in frames(container):
                    if self._idle():
                        break
                    now = time.time()
                    if now - last < 1 / MAX_FPS:
                        continue
                    last = now
                    self.size = (frame.width, frame.height)
                    ok, buf = cv2.imencode('.jpg', to_bgr(frame, self.max_width), [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
                    if ok:
                        self._publish(jpeg=buf.tobytes())
            except CaptureError:
                pass  # câmera caiu ou parou de mandar vídeo: reconecta
            finally:
                container.close()
            if not self._idle():
                self.jpeg = None
                self._publish(status='Reconectando...')
        self._publish(status='Pausado')

    def current_jpeg(self) -> bytes:
        return self.jpeg or placeholder('Conectando...' if self.status == 'Ao vivo' else self.status)

    def mjpeg(self):
        """Gerador multipart/x-mixed-replace. Reenvia o último quadro a cada 2 s mesmo sem novidade,
        para manter a conexão viva e perceber quando o navegador fecha."""
        seq = -1
        while True:
            self.touch()
            with self.cond:
                self.cond.wait_for(lambda: self.seq != seq, timeout=2)
                seq = self.seq
                jpeg = self.current_jpeg()
            yield (b'--frame\r\nContent-Type: image/jpeg\r\nContent-Length: '
                   + str(len(jpeg)).encode() + b'\r\n\r\n' + jpeg + b'\r\n')


class StreamManager:
    def __init__(self):
        self._streams: dict[tuple[str, int | None], CameraStream] = {}
        self._lock = threading.Lock()

    def get(self, url: str, max_width: int | None = None) -> CameraStream:
        key = (url, max_width)
        with self._lock:
            if key not in self._streams:
                self._streams[key] = CameraStream(url, max_width)
            return self._streams[key]
