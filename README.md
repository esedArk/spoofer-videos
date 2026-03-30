# freepik_ig_scrapper

Pipeline local con:
- Landing (`index.html`) para subir video original.
- API (`api_server.py`) para crear jobs y consultar estado.
- Worker (`worker.py`) que consume RabbitMQ y genera 4 variantes con FFmpeg.

## Requisitos

1. Python 3.11+.
2. FFmpeg instalado (o configurar `FFMPEG_BIN` / `FFPROBE_BIN`).
3. RabbitMQ corriendo.
4. Opcional: para procesar tambien URLs remotas, `yt-dlp` instalado (o configurar `YTDLP_BIN`).

## Configuracion

1. Crea `.env` desde `.env.example`.
2. Instala dependencias:

```bash
pip install -r requirements.txt
```

Ejemplo en Windows:

```env
FFMPEG_BIN=C:\ffmpeg\bin\ffmpeg.exe
FFPROBE_BIN=C:\ffmpeg\bin\ffprobe.exe
YTDLP_BIN=C:\yt-dlp\yt-dlp.exe
```

## Ejecutar

Terminal 1:

```bash
python api_server.py
```

Terminal 2:

```bash
python worker.py
```

Luego abre `http://localhost:8000`.

## Endpoints

- `POST /api/jobs` body: `{ "url": "https://..." }`
- `POST /api/jobs/upload` multipart form-data: `file=<video>`
- `GET /api/jobs/recent?limit=8`
- `GET /api/jobs/<id>`
