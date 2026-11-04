# CameraViewer

Encontra as câmeras IP da rede local (ONVIF/RTSP) e mostra o vídeo ao vivo no navegador.

Baseado no aprendizado do [IpCameraServer/ipcameras.py](https://github.com/MarcosSoares10/IpCameraServer/blob/main/ipcameras.py),
com estas mudanças:

| ipcameras.py | CameraViewer |
|---|---|
| Varre `192.168.0.1-254` um IP por vez | WS-Discovery multicast (as câmeras ONVIF se anunciam em ~2 s) + varredura paralela da sub-rede detectada automaticamente |
| `onvif-zeep` só para confirmar que é câmera | ONVIF em SOAP puro, sem dependência: fabricante/modelo e a URL RTSP real de cada perfil (`GetStreamUri`) |
| URL fixa `rtsp://user:password@ip:554/onvif1` | URL vinda da câmera; sem ONVIF, testa caminhos RTSP comuns (`/onvif1`, `/live/ch0`, `/Streaming/Channels/101`...) |
| Grava com `ffmpeg` externo | Mostra ao vivo com PyAV (FFmpeg já embutido, não precisa instalar); RTSP por TCP com recaída automática para UDP |
| Usuário/senha no código | Usuário/senha informados na tela e salvos em `data/` |

## Como usar

```bash
pip install -r requirements.txt
python app.py
```

Abra http://localhost:8092, clique em **Procurar câmeras**, informe o usuário/senha das
câmeras e adicione as encontradas. Também dá para adicionar uma URL RTSP manualmente.

- **Grade**: usa o sub-stream (resolução menor) quando a câmera tem um, para economizar CPU e rede.
- **Clique na imagem**: tela cheia com o stream principal.
- **Foto**: baixa um JPEG do quadro atual.
- `http://localhost:8092/video/<id>`: stream MJPEG da câmera, para abrir em outros programas.

### Acessar de outro aparelho (celular)

Por padrão o servidor só aceita conexões desta máquina, porque mostra o vídeo e as senhas das
câmeras sem login. Para liberar na rede local:

```powershell
$env:HOST = "0.0.0.0"; python app.py
```

## Estrutura

- `app.py`: servidor Flask (API, busca em segundo plano, envio dos quadros)
- `cameras/discovery.py`: WS-Discovery, varredura de portas, cliente ONVIF mínimo, sondagem RTSP
- `cameras/stream.py`: um leitor RTSP por câmera, compartilhado entre os clientes, com reconexão automática
- `cameras/store.py`: câmeras e configuração em `data/*.json`
- `templates/index.html`: interface

## Dicas

- Se a busca não achar nada, confira se o PC está na mesma rede das câmeras e informe a
  sub-rede manualmente (ex.: `192.168.0.0/24`).
- O Firewall do Windows pode bloquear as respostas do WS-Discovery (UDP 3702); a varredura de
  portas continua funcionando mesmo assim.
- Câmeras tipo Yoosee/"onvif1" respondem ONVIF na porta 5000 e RTSP na 554.
