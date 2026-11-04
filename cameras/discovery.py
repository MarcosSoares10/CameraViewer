"""Descoberta de câmeras IP na rede local.

Estratégia (evolução do ipcameras.py original):
  1. WS-Discovery: um "Probe" multicast ONVIF (UDP 239.255.255.250:3702) faz as
     câmeras se anunciarem sozinhas, em ~2 s, sem varrer IP por IP.
  2. Varredura da sub-rede em paralelo (portas RTSP/ONVIF comuns), para pegar
     câmeras que não respondem ao multicast.
  3. Para cada candidato, consulta ONVIF (SOAP puro, sem dependências) para obter
     fabricante/modelo e as URLs RTSP reais de cada perfil (principal e sub-stream).
  4. Se não houver ONVIF mas a porta 554 estiver aberta, testa caminhos RTSP comuns.
"""
import base64
import hashlib
import ipaddress
import logging
import os
import re
import socket
import urllib.error
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlsplit, urlunsplit

logger = logging.getLogger(__name__)

RTSP_PORT = 554
ONVIF_PORTS = (80, 5000, 8000, 8080, 8899, 2020, 8090)
SCAN_PORTS = (RTSP_PORT,) + ONVIF_PORTS
RTSP_PATHS = (
    '/onvif1', '/onvif2', '/live/ch0', '/live/ch00_0', '/h264', '/stream1', '/stream2',
    '/Streaming/Channels/101', '/cam/realmonitor?channel=1&subtype=0', '/11', '/12',
    '/live', '/videoMain', '/',
)

# ---------------------------------------------------------------- rede local

def local_networks() -> list[ipaddress.IPv4Network]:
    """Sub-redes /24 das interfaces IPv4 desta máquina."""
    ips = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass
    try:  # interface da rota padrão (não envia nada)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(('8.8.8.8', 80))
            ips.add(s.getsockname()[0])
    except OSError:
        pass
    nets = []
    for ip in ips:
        addr = ipaddress.IPv4Address(ip)
        if addr.is_loopback or addr.is_link_local:
            continue
        net = ipaddress.IPv4Network(f'{ip}/24', strict=False)
        if net not in nets:
            nets.append(net)
    return nets


def _interface_ips() -> list[str]:
    out = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            out.add(info[4][0])
    except OSError:
        pass
    return [ip for ip in out if not ip.startswith(('127.', '169.254.'))]


# ---------------------------------------------------------------- WS-Discovery

_PROBE = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<e:Envelope xmlns:e="http://www.w3.org/2003/05/soap-envelope" '
    'xmlns:w="http://schemas.xmlsoap.org/ws/2004/08/addressing" '
    'xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery" '
    'xmlns:dn="http://www.onvif.org/ver10/network/wsdl">'
    '<e:Header><w:MessageID>uuid:{id}</w:MessageID>'
    '<w:To e:mustUnderstand="true">urn:schemas-xmlsoap-org:ws:2005:04:discovery</w:To>'
    '<w:Action e:mustUnderstand="true">http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</w:Action>'
    '</e:Header><e:Body><d:Probe><d:Types>dn:NetworkVideoTransmitter</d:Types></d:Probe></e:Body></e:Envelope>'
)
_XADDRS_RE = re.compile(r'<[^>]*?XAddrs>([^<]+)<', re.S)


def ws_discovery(timeout: float = 2.5) -> dict[str, str]:
    """Envia o Probe ONVIF em cada interface e devolve {ip: xaddr do device_service}."""
    found: dict[str, str] = {}
    socks = []
    for iface in _interface_ips() or ['0.0.0.0']:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            if iface != '0.0.0.0':
                s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(iface))
            s.bind((iface, 0))
            s.settimeout(0.2)
            s.sendto(_PROBE.format(id=uuid.uuid4()).encode(), ('239.255.255.250', 3702))
            socks.append(s)
        except OSError as e:
            logger.debug('WS-Discovery falhou em %s: %s', iface, e)
    deadline = datetime.now() + timedelta(seconds=timeout)
    while socks and datetime.now() < deadline:
        for s in socks:
            try:
                data, (ip, _) = s.recvfrom(65535)
            except (socket.timeout, OSError):
                continue
            m = _XADDRS_RE.search(data.decode('utf-8', 'ignore'))
            if not m:
                continue
            addrs = m.group(1).split()
            # prefere o endereço IPv4 que bate com o remetente
            best = next((a for a in addrs if urlsplit(a).hostname == ip), addrs[0])
            found.setdefault(ip, best)
    for s in socks:
        s.close()
    return found


# ---------------------------------------------------------------- varredura

def _port_open(ip: str, port: int, timeout: float) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except OSError:
        return False


def scan_hosts(networks, ports=SCAN_PORTS, timeout: float = 0.4, progress=None) -> dict[str, list[int]]:
    """Testa todas as combinações IP x porta em paralelo. Devolve {ip: [portas abertas]}."""
    own = set(_interface_ips())
    jobs = [(str(ip), p) for net in networks for ip in net.hosts() if str(ip) not in own for p in ports]
    result: dict[str, list[int]] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=192) as pool:
        for (ip, port), ok in zip(jobs, pool.map(lambda j: _port_open(j[0], j[1], timeout), jobs)):
            done += 1
            if ok:
                result.setdefault(ip, []).append(port)
            if progress and done % 200 == 0:
                progress(f'Varrendo a rede... {100 * done // len(jobs)}%')
    return result


# ---------------------------------------------------------------- ONVIF (SOAP)

_WSSE = 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd'
_WSU = 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd'
_PWD_DIGEST = 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest'
_B64 = 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary'


class OnvifError(Exception):
    pass


def _esc(s: str) -> str:
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _find(root, name):
    for el in root.iter():
        if el.tag.rsplit('}', 1)[-1] == name:
            return el
    return None


def _findall(root, name):
    return [el for el in root.iter() if el.tag.rsplit('}', 1)[-1] == name]


def _text(root, name, default=''):
    el = _find(root, name) if root is not None else None
    return (el.text or '').strip() if el is not None and el.text else default


class Onvif:
    """Cliente ONVIF mínimo: só o necessário para identificar a câmera e achar o RTSP."""

    def __init__(self, xaddr: str, user: str = '', password: str = '', timeout: float = 4):
        self.xaddr = xaddr
        self.user = user
        self.password = password
        self.timeout = timeout
        self.clock_offset = timedelta(0)

    def _call(self, url: str, body: str, auth: bool = True):
        header = ''
        if auth and self.user:
            nonce = os.urandom(16)
            created = (datetime.now(timezone.utc) + self.clock_offset).strftime('%Y-%m-%dT%H:%M:%S.000Z')
            digest = base64.b64encode(
                hashlib.sha1(nonce + created.encode() + self.password.encode()).digest()).decode()
            header = (
                f'<s:Header><Security s:mustUnderstand="1" xmlns="{_WSSE}"><UsernameToken>'
                f'<Username>{_esc(self.user)}</Username>'
                f'<Password Type="{_PWD_DIGEST}">{digest}</Password>'
                f'<Nonce EncodingType="{_B64}">{base64.b64encode(nonce).decode()}</Nonce>'
                f'<Created xmlns="{_WSU}">{created}</Created>'
                f'</UsernameToken></Security></s:Header>'
            )
        env = ('<?xml version="1.0" encoding="UTF-8"?>'
               f'<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">{header}'
               f'<s:Body>{body}</s:Body></s:Envelope>')
        req = urllib.request.Request(url, data=env.encode(),
                                     headers={'Content-Type': 'application/soap+xml; charset=utf-8'})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return ET.fromstring(r.read())
        except urllib.error.HTTPError as e:
            raw = e.read()
            reason = ''
            try:
                reason = _text(ET.fromstring(raw), 'Text') or _text(ET.fromstring(raw), 'Value')
            except ET.ParseError:
                pass
            raise OnvifError(f'HTTP {e.code} {reason}'.strip()) from None
        except (urllib.error.URLError, OSError, ET.ParseError) as e:
            raise OnvifError(str(e)) from None

    def sync_clock(self):
        """Lê o relógio da câmera (sem autenticação) para o digest não ser rejeitado por diferença de hora."""
        root = self._call(self.xaddr, '<GetSystemDateAndTime xmlns="http://www.onvif.org/ver10/device/wsdl"/>',
                          auth=False)
        utc = _find(root, 'UTCDateTime')
        if utc is None:
            return
        try:
            cam = datetime(int(_text(utc, 'Year')), int(_text(utc, 'Month')), int(_text(utc, 'Day')),
                           int(_text(utc, 'Hour')), int(_text(utc, 'Minute')), int(_text(utc, 'Second')),
                           tzinfo=timezone.utc)
            self.clock_offset = cam - datetime.now(timezone.utc)
        except ValueError:
            pass

    def device_info(self) -> dict:
        root = self._call(self.xaddr, '<GetDeviceInformation xmlns="http://www.onvif.org/ver10/device/wsdl"/>')
        return {k: _text(root, k) for k in ('Manufacturer', 'Model', 'FirmwareVersion', 'SerialNumber')}

    def media_xaddr(self) -> str:
        try:
            root = self._call(self.xaddr, '<GetCapabilities xmlns="http://www.onvif.org/ver10/device/wsdl">'
                                          '<Category>Media</Category></GetCapabilities>')
            media = _find(root, 'Media')
            addr = _text(media, 'XAddr') if media is not None else ''
            if addr:
                # algumas câmeras devolvem o IP interno/errado: força o host que respondeu
                host = urlsplit(self.xaddr).netloc
                p = urlsplit(addr)
                return urlunsplit((p.scheme, host, p.path, p.query, p.fragment))
        except OnvifError:
            pass
        return self.xaddr

    def streams(self) -> list[dict]:
        """Lista de perfis com resolução e URL RTSP, do maior para o menor."""
        media = self.media_xaddr()
        root = self._call(media, '<GetProfiles xmlns="http://www.onvif.org/ver10/media/wsdl"/>')
        out = []
        for prof in _findall(root, 'Profiles'):
            token = prof.get('token')
            if not token:
                continue
            res = _find(prof, 'Resolution')
            w = int(_text(res, 'Width', '0') or 0) if res is not None else 0
            h = int(_text(res, 'Height', '0') or 0) if res is not None else 0
            try:
                r = self._call(media,
                               '<GetStreamUri xmlns="http://www.onvif.org/ver10/media/wsdl"><StreamSetup>'
                               '<Stream xmlns="http://www.onvif.org/ver10/schema">RTP-Unicast</Stream>'
                               '<Transport xmlns="http://www.onvif.org/ver10/schema"><Protocol>RTSP</Protocol>'
                               f'</Transport></StreamSetup><ProfileToken>{_esc(token)}</ProfileToken></GetStreamUri>')
            except OnvifError as e:
                logger.debug('GetStreamUri %s falhou: %s', token, e)
                continue
            uri = _text(r, 'Uri')
            if uri:
                out.append({'name': _text(prof, 'Name') or token, 'width': w, 'height': h, 'uri': uri})
        out.sort(key=lambda s: s['width'] * s['height'], reverse=True)
        return out


def with_credentials(url: str, user: str, password: str, host: str | None = None) -> str:
    """Insere usuário/senha na URL RTSP (e opcionalmente troca o host pelo IP que respondeu)."""
    p = urlsplit(url)
    hostport = host or p.hostname or ''
    if p.port:
        hostport += f':{p.port}'
    auth = f'{quote(user, safe="")}:{quote(password, safe="")}@' if user else ''
    return urlunsplit((p.scheme, auth + hostport, p.path, p.query, p.fragment))


def strip_credentials(url: str) -> str:
    p = urlsplit(url)
    netloc = p.netloc.rsplit('@', 1)[-1]
    return urlunsplit((p.scheme, netloc, p.path, p.query, p.fragment))


# ---------------------------------------------------------------- RTSP sem ONVIF

def _rtsp_status(ip: str, path: str, port: int = RTSP_PORT, timeout: float = 1.5) -> int:
    url = f'rtsp://{ip}:{port}{path}'
    try:
        with socket.create_connection((ip, port), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall(f'DESCRIBE {url} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n'.encode())
            line = s.recv(256).split(b'\r\n', 1)[0].decode('latin-1')
        return int(line.split()[1]) if line.startswith('RTSP/') else 0
    except (OSError, ValueError, IndexError):
        return 0


def guess_rtsp_path(ip: str) -> str:
    """Primeiro caminho que responde 200; senão o primeiro que não é 404 (pede senha); senão /onvif1."""
    fallback = None
    for path in RTSP_PATHS:
        code = _rtsp_status(ip, path)
        if code == 200:
            return path
        if code in (401, 403) and fallback is None:
            fallback = path
    return fallback or RTSP_PATHS[0]


# ---------------------------------------------------------------- orquestração

def probe_camera(ip: str, ports: list[int], xaddr: str | None, user: str, password: str) -> dict | None:
    cam = {'ip': ip, 'name': '', 'onvif': False, 'streams': [], 'rtsp_main': '', 'rtsp_sub': '', 'error': ''}
    candidates = [xaddr] if xaddr else []
    candidates += [f'http://{ip}:{p}/onvif/device_service' for p in ONVIF_PORTS if p in ports]
    for addr in candidates:
        cli = Onvif(addr, user, password)
        try:
            cli.sync_clock()
        except OnvifError:
            continue  # não é ONVIF nessa porta
        cam['onvif'] = True
        try:
            info = cli.device_info()
            cam['name'] = ' '.join(x for x in (info['Manufacturer'], info['Model']) if x)
        except OnvifError as e:
            cam['error'] = f'ONVIF recusou as credenciais ({e})'
        try:
            streams = cli.streams()
        except OnvifError as e:
            streams = []
            cam['error'] = cam['error'] or f'Não consegui listar os perfis ONVIF ({e})'
        if streams:
            for s in streams:
                s['uri'] = with_credentials(s['uri'], user, password, host=ip)
            cam['streams'] = streams
            cam['rtsp_main'] = streams[0]['uri']
            cam['rtsp_sub'] = streams[-1]['uri'] if len(streams) > 1 else ''
            cam['error'] = ''
        break
    if not cam['rtsp_main']:
        rtsp_open = RTSP_PORT in ports or _port_open(ip, RTSP_PORT, 0.5)
        if not rtsp_open and not cam['onvif']:
            return None  # nem ONVIF nem RTSP: roteador, impressora etc.
        path = guess_rtsp_path(ip) if rtsp_open else RTSP_PATHS[0]
        cam['rtsp_main'] = with_credentials(f'rtsp://{ip}:{RTSP_PORT}{path}', user, password)
    cam['name'] = cam['name'] or ('Câmera ONVIF' if cam['onvif'] else 'Câmera RTSP')
    return cam


def discover(user: str = '', password: str = '', subnet: str = '', progress=None) -> list[dict]:
    say = progress or (lambda msg: None)
    if subnet.strip():
        networks = [ipaddress.IPv4Network(subnet.strip(), strict=False)]
    else:
        networks = local_networks()
    if not networks:
        raise ValueError('Nenhuma rede local encontrada. Informe a sub-rede (ex.: 192.168.0.0/24).')

    say('Procurando câmeras ONVIF (WS-Discovery)...')
    announced = ws_discovery()
    announced = {ip: x for ip, x in announced.items()
                 if any(ipaddress.IPv4Address(ip) in n for n in networks)} if subnet.strip() else announced

    say(f'Varrendo {", ".join(map(str, networks))}...')
    hosts = scan_hosts(networks, progress=say)
    for ip in announced:
        hosts.setdefault(ip, [])

    say(f'Identificando {len(hosts)} dispositivo(s)...')
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda ip: probe_camera(ip, hosts[ip], announced.get(ip), user, password),
                                sorted(hosts, key=lambda s: tuple(map(int, s.split('.'))))))
    return [c for c in results if c]
