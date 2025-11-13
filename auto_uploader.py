"""
Auto Uploader — Fresh Full (single-file FastAPI app)

Features included:
- Web dashboard (upload, paste links, schedule, generate AI metadata)
- Background worker (queue, scheduling, uploads)
- YouTube uploader (YouTube Data API v3) using token.json OAuth
- Instagram Reels uploader (Instagram Graph API) using IG_ACCESS_TOKEN / IG_USER_ID
- AI metadata generation (optional; uses OPENAI_API_KEY if provided)
- Thumbnail generation (ffmpeg preferred; Pillow fallback)
- Link downloader (direct URLs + Google Drive file links)
- Google Drive folder sync (optional; set GD_FOLDER_ID)
- SQLite DB for persistent queue and metadata
- Hour-gap scheduling (per-file next_upload_ts)

IMPORTANT: You must run OAuth locally once to create token.json (instructions below).

ENV vars used:
- YT_CLIENT_SECRETS_JSON (default: client_secret.json)
- YT_TOKEN_FILE (default: token.json)
- IG_ACCESS_TOKEN, IG_USER_ID (for Instagram uploads)
- VIDEO_BASE_URL (public URL prefix to served uploads, required for Instagram)
- OPENAI_API_KEY (optional — for better metadata)
- GD_FOLDER_ID (optional — Drive folder to sync)
- GD_CHECK_INTERVAL_HOURS (optional, default 2)
- PROCESS_INTERVAL (seconds between queue checks, default 60)

Run locally for testing:
1) pip install -r requirements.txt
2) python fresh_uploader.py
3) open http://127.0.0.1:8000

"""

import os
import time
import asyncio
import shutil
import json
import re
import io
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import requests

# Google API imports
from googleapiclient.discovery import build as gdrive_build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build as youtube_build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# Image
from PIL import Image

# DB
import sqlite3

# Config & paths
BASE_DIR = Path.cwd()
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
DB_PATH = BASE_DIR / "uploader.db"

YT_CLIENT_SECRETS = os.getenv("YT_CLIENT_SECRETS_JSON", "client_secret.json")
YT_TOKEN_FILE = os.getenv("YT_TOKEN_FILE", "token.json")
IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
VIDEO_BASE_URL = os.getenv("VIDEO_BASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
GD_FOLDER_ID = os.getenv("GD_FOLDER_ID")
GD_CHECK_INTERVAL_HOURS = int(os.getenv("GD_CHECK_INTERVAL_HOURS", "2"))
PROCESS_INTERVAL = int(os.getenv("PROCESS_INTERVAL", "60"))

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

app = FastAPI(title="Auto Uploader — Fresh Full")
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")

# ---------------- DB ----------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS uploads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT UNIQUE,
        original_url TEXT,
        title TEXT,
        description TEXT,
        tags TEXT,
        hashtags TEXT,
        thumbnail TEXT,
        next_upload_ts INTEGER,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()

init_db()

# ------------ YouTube helpers -------------

def load_youtube_service():
    if not Path(YT_TOKEN_FILE).exists():
        raise FileNotFoundError(f"YouTube token file not found: {YT_TOKEN_FILE}. Create it locally first.")
    creds = Credentials.from_authorized_user_file(YT_TOKEN_FILE, SCOPES)
    service = youtube_build("youtube", "v3", credentials=creds)
    return service


def upload_to_youtube(file_path: str, title: str, description: str = "", tags: Optional[list] = None, privacy: str = "public"):
    service = load_youtube_service()
    body = {
        "snippet": {"title": title, "description": description, "tags": tags or [], "categoryId": "22"},
        "status": {"privacyStatus": privacy}
    }
    media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
    request = service.videos().insert(part=",".join(body.keys()), body=body, media_body=media)
    res = request.execute()
    return res.get('id')

# ------------ Instagram helpers -------------

def instagram_create_container(video_url: str, caption: str) -> str:
    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        raise EnvironmentError('Instagram credentials missing')
    url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
    params = {"video_url": video_url, "caption": caption, "access_token": IG_ACCESS_TOKEN}
    r = requests.post(url, params=params)
    data = r.json()
    if r.status_code != 200:
        raise RuntimeError(f"IG create failed: {data}")
    return data.get('id')


def instagram_publish_container(container_id: str) -> dict:
    url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish"
    params = {"creation_id": container_id, "access_token": IG_ACCESS_TOKEN}
    r = requests.post(url, params=params)
    data = r.json()
    if r.status_code != 200:
        raise RuntimeError(f"IG publish failed: {data}")
    return data

# ------------ AI metadata -------------

def call_openai(prompt: str) -> Optional[str]:
    if not OPENAI_API_KEY:
        return None
    try:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        body = {"model": OPENAI_MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": 350}
        r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=body, timeout=30)
        return r.json()['choices'][0]['message']['content']
    except Exception as e:
        print('OpenAI error', e)
        return None


def generate_ai_metadata(video_path: str):
    base = Path(video_path).stem.replace('_', ' ').title()
    if OPENAI_API_KEY:
        prompt = (f"You are a YouTube SEO expert. Given the video topic '{base}', produce a JSON with fields: title, description, tags (comma separated), hashtags (space separated). "
                  "Title <70 chars. Description 2-3 lines with CTA. Provide 5 tags and 5 hashtags. Output valid JSON only.")
        out = call_openai(prompt)
        if out:
            try:
                import re, json as _json
                m = re.search(r"\{[\s\S]*\}", out)
                if m:
                    parsed = _json.loads(m.group(0))
                    return {
                        'title': parsed.get('title'),
                        'description': parsed.get('description'),
                        'tags': [t.strip() for t in parsed.get('tags','').split(',') if t.strip()],
                        'hashtags': parsed.get('hashtags')
                    }
            except Exception as e:
                print('AI parse failed', e)
    # fallback
    return {'title': f"{base} | Quick Guide", 'description': f"Watch this short video about {base}. Like & Subscribe.", 'tags': [base.replace(' ','')], 'hashtags': '#shorts'}

# ------------ Thumbnail -------------

def generate_thumbnail(video_path: str, output_path: str):
    try:
        tmp = str(Path(output_path).with_suffix('.tmp.jpg'))
        import subprocess
        subprocess.run(["ffmpeg", "-y", "-i", video_path, '-ss', '00:00:00.500', '-vframes', '1', tmp], check=True)
        img = Image.open(tmp).convert('RGB')
        img.thumbnail((1280,720))
        img.save(output_path, 'JPEG')
        Path(tmp).unlink(missing_ok=True)
        return True
    except Exception as e:
        print('thumb failed', e)
        try:
            img = Image.new('RGB', (1280,720), color=(30,30,30))
            img.save(output_path, 'JPEG')
            return True
        except Exception as e2:
            print('thumb fallback failed', e2)
            return False

# ------------ Link downloader -------------

def download_from_url(url: str) -> str:
    # Google Drive direct file link handling
    gd = re.search(r"drive.google.com/file/d/([A-Za-z0-9_-]+)", url)
    if gd:
        file_id = gd.group(1)
        dl_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    else:
        dl_url = url
    r = requests.get(dl_url, stream=True)
    if r.status_code != 200:
        raise RuntimeError(f"download failed: {r.status_code}")
    cd = r.headers.get('content-disposition','')
    fname = None
    if 'filename=' in cd:
        fname = cd.split('filename=')[-1].strip('"')
    if not fname:
        fname = 'video_'+str(int(time.time()))+'.mp4'
    path = UPLOAD_DIR / fname
    with open(path, 'wb') as f:
        for chunk in r.iter_content(8192):
            if chunk:
                f.write(chunk)
    return fname

# ------------ Google Drive sync -------------

def load_drive_service():
    if not Path(YT_TOKEN_FILE).exists():
        raise FileNotFoundError('token.json missing for Drive auth')
    creds = Credentials.from_authorized_user_file(YT_TOKEN_FILE, SCOPES)
    return gdrive_build('drive', 'v3', credentials=creds)


def download_drive_file(file_id, filename):
    svc = load_drive_service()
    request = svc.files().get_media(fileId=file_id)
    fh = io.FileIO(UPLOAD_DIR / filename, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return filename


def sync_drive_folder_if_due():
    if not GD_FOLDER_ID:
        return
    last_file = BASE_DIR / '.gd_last'
    last_ts = 0
    if last_file.exists():
        try: last_ts = int(last_file.read_text())
        except: last_ts = 0
    if time.time() - last_ts < GD_CHECK_INTERVAL_HOURS*3600:
        return
    try:
        svc = load_drive_service()
        res = svc.files().list(q=f"'{GD_FOLDER_ID}' in parents and trashed=false", fields='files(id,name)').execute()
        files = res.get('files', [])
        for f in files:
            name = f.get('name')
            fid = f.get('id')
            if not name.lower().endswith(('.mp4','.mov','.mkv')): continue
            if (UPLOAD_DIR / name).exists(): continue
            print('[Drive] downloading', name)
            download_drive_file(fid, name)
        last_file.write_text(str(int(time.time())))
    except Exception as e:
        print('Drive sync error', e)

# ------------ DB helpers -------------

def save_to_db(filename, original_url=None, title=None, description=None, tags=None, hashtags=None, thumbnail=None, next_ts=None, status='ready'):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO uploads (filename, original_url, title, description, tags, hashtags, thumbnail, next_upload_ts, status) VALUES (?,?,?,?,?,?,?,?,?)',
              (filename, original_url, title, description, ','.join(tags) if tags else None, hashtags, thumbnail, next_ts, status))
    conn.commit(); conn.close()


def load_from_db(filename):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT filename,original_url,title,description,tags,hashtags,thumbnail,next_upload_ts,status FROM uploads WHERE filename=?',(filename,))
    row = c.fetchone(); conn.close()
    if not row: return None
    return {'filename':row[0],'original_url':row[1],'title':row[2],'description':row[3],'tags':row[4].split(',') if row[4] else [],'hashtags':row[5],'thumbnail':row[6],'next_upload_ts':row[7],'status':row[8]}


def list_uploads():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT filename,title,next_upload_ts,status FROM uploads ORDER BY created_at DESC')
    rows = c.fetchall(); conn.close()
    return [{'filename':r[0],'title':r[1],'next_upload_ts':r[2],'status':r[3]} for r in rows]

# ------------ Processing worker -------------

async def process_one(p: Path):
    fname = p.name
    meta = load_from_db(fname)
    if meta and meta.get('next_upload_ts'):
        if time.time() < meta['next_upload_ts']:
            # not yet time
            return
    # generate metadata if missing
    if not meta or not meta.get('title'):
        generated = generate_ai_metadata(str(p))
        thumb = str(UPLOAD_DIR / f"{p.stem}_thumb.jpg")
        generate_thumbnail(str(p), thumb)
        save_to_db(fname, original_url=None, title=generated.get('title'), description=generated.get('description'), tags=generated.get('tags'), hashtags=generated.get('hashtags'), thumbnail=thumb, next_ts=None, status='ready')
        meta = load_from_db(fname)
    # upload to youtube
    try:
        vid = upload_to_youtube(str(p), meta.get('title') or p.stem, description=meta.get('description') or '', tags=meta.get('tags') or [])
        # set thumbnail
        try:
            if meta.get('thumbnail') and Path(meta['thumbnail']).exists():
                svc = load_youtube_service()
                svc.thumbnails().set(videoId=vid, media_body=MediaFileUpload(meta['thumbnail'])).execute()
        except Exception as e:
            print('thumbnail attach fail', e)
    except Exception as e:
        print('youtube upload failed', e)
        return
    # instagram
    if VIDEO_BASE_URL and IG_ACCESS_TOKEN and IG_USER_ID:
        try:
            video_url = VIDEO_BASE_URL.rstrip('/') + '/' + fname
            caption = (meta.get('hashtags') or '') + '\n' + (meta.get('description') or '')
            cid = instagram_create_container(video_url, caption)
            instagram_publish_container(cid)
        except Exception as e:
            print('ig upload failed', e)
    # mark done
    processed = UPLOAD_DIR / 'processed'
    processed.mkdir(exist_ok=True)
    shutil.move(str(p), str(processed / fname))
    conn = sqlite3.connect(DB_PATH); c = conn.cursor(); c.execute('UPDATE uploads SET status=? WHERE filename=?',('done',fname)); conn.commit(); conn.close()
    print('Finished', fname)

async def worker_loop():
    print('Worker started. Interval', PROCESS_INTERVAL)
    while True:
        try:
            # drive sync
            sync_drive_folder_if_due()
            # process files in uploads
            files = sorted([p for p in UPLOAD_DIR.iterdir() if p.suffix.lower() in ('.mp4','.mov','.mkv')])
            for p in files:
                await process_one(p)
        except Exception as e:
            print('Worker error', e)
        await asyncio.sleep(PROCESS_INTERVAL)

# ------------ API Endpoints & UI -------------
@app.get('/', response_class=HTMLResponse)
async def homepage():
    return HTMLResponse("""
    <!doctype html>
    <html>
    <head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
    <title>Auto Uploader — Dashboard</title>
    <style>body{font-family:Inter,Arial;margin:18px auto;max-width:1100px}table{width:100%;border-collapse:collapse}th,td{padding:8px;border-bottom:1px solid #eee}select,button,input,textarea{padding:6px}</style>
    </head>
    <body>
    <h1>Auto Uploader — Dashboard</h1>
    <p>Paste links, upload files, generate AI metadata and schedule uploads by hour-gap.</p>
    <div style='display:flex;gap:8px;margin-bottom:10px'>
      <input id='links' placeholder='Paste multiple URLs (one per line)' style='flex:1'>
      <button onclick='addLinks()'>Add Links</button>
      <button onclick='refresh()'>Refresh</button>
    </div>
    <table id='tbl'><thead><tr><th>Filename</th><th>Title</th><th>Schedule</th><th>Next Upload</th><th>Status</th><th>Actions</th></tr></thead><tbody></tbody></table>
    <script>
    async function api(path, method='GET', body=null){const opts={method,headers:{}}; if(body){opts.headers['Content-Type']='application/json'; opts.body=JSON.stringify(body)}; const r=await fetch(path,opts); return r.json();}
    function fmt(ts){ if(!ts) return '-'; return new Date(ts*1000).toLocaleString(); }
    async function refresh(){ const data=await api('/api/uploads'); const tb=document.querySelector('#tbl tbody'); tb.innerHTML=''; data.uploads.forEach(u=>{ const tr=document.createElement('tr'); tr.innerHTML=`<td>${u.filename}</td><td>${u.title||''}</td><td><select id='sel_${u.filename}'><option value='0'>Immediate</option><option value='1'>1h</option><option value='2'>2h</option><option value='3'>3h</option><option value='6'>6h</option><option value='12'>12h</option></select></td><td>${u.next_upload_ts?fmt(u.next_upload_ts):'-'}</td><td>${u.status}</td><td><button onclick="generate('${u.filename}')">Generate AI</button> <button onclick="schedule('${u.filename}')">Schedule</button></td>`; tb.appendChild(tr); }); }
    async function addLinks(){ const raw=document.getElementById('links').value.trim(); if(!raw) return alert('Paste links'); const lines=raw.split(/\n|\r/).map(s=>s.trim()).filter(Boolean); for(const l of lines){ await api('/api/add_link','POST',{url:l}); } alert('Added'); document.getElementById('links').value=''; setTimeout(refresh,500);}    
    async function generate(filename){ const res=await api('/api/generate','POST',{file:filename}); alert(JSON.stringify(res)); refresh(); }
    async function schedule(filename){ const sel=document.getElementById('sel_'+filename); if(!sel) return alert('Select'); const hrs=parseInt(sel.value); await api('/api/schedule_hours','POST',{filename:filename,hours_gap:hrs}); alert('Scheduled in '+hrs+' hour(s)'); refresh(); }
    refresh(); setInterval(refresh,45000);
    </script>
    </body></html>
    """)

@app.post('/upload')
async def upload_endpoint(file: UploadFile = File(...), title: Optional[str] = Form(None), description: Optional[str] = Form(None)):
    dest = UPLOAD_DIR / file.filename
    with dest.open('wb') as f:
        f.write(await file.read())
    if title or description:
        save_to_db(file.filename, title=title, description=description, status='ready')
    return JSONResponse({'status':'ok','filename':file.filename})

@app.post('/api/add_link')
async def api_add_link(body: dict):
    url = body.get('url')
    if not url:
        return JSONResponse({'error':'missing url'}, status_code=400)
    try:
        fname = download_from_url(url)
        save_to_db(fname, original_url=url, status='ready')
        return JSONResponse({'status':'ok','filename':fname})
    except Exception as e:
        return JSONResponse({'error':str(e)}, status_code=500)

@app.post('/api/generate')
async def api_generate(body: dict):
    file = body.get('file')
    path = UPLOAD_DIR / file
    if not path.exists():
        return JSONResponse({'error':'file not found'}, status_code=404)
    meta = generate_ai_metadata(str(path))
    thumb = str(UPLOAD_DIR / f"{path.stem}_thumb.jpg")
    generate_thumbnail(str(path), thumb)
    save_to_db(file, title=meta.get('title'), description=meta.get('description'), tags=meta.get('tags'), hashtags=meta.get('hashtags'), thumbnail=thumb, status='ready')
    return JSONResponse({'status':'ok','meta':meta,'thumbnail':thumb})

@app.post('/api/schedule_hours')
async def api_schedule_hours(body: dict):
    filename = body.get('filename')
    hours = int(body.get('hours_gap',0))
    if hours<0: hours=0
    next_ts = int(time.time() + hours*3600) if hours>0 else int(time.time())
    conn = sqlite3.connect(DB_PATH); c = conn.cursor(); c.execute('UPDATE uploads SET next_upload_ts=?, status=? WHERE filename=?',(next_ts,'scheduled',filename)); conn.commit(); conn.close()
    return JSONResponse({'status':'ok','next_upload_ts':next_ts})

@app.get('/api/uploads')
async def api_uploads():
    return JSONResponse({'uploads': list_uploads()})

# startup
@app.on_event('startup')
async def startup_tasks():
    asyncio.create_task(worker_loop())

# README endpoint
@app.get('/readme', response_class=HTMLResponse)
async def readme():
    return HTMLResponse('<pre>' + (Path(__file__).read_text() if Path(__file__).exists() else 'README: place your client_secret.json and token.json and run app') + '</pre>')

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv('PORT','8000')))
