import os, uuid, asyncio
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from . import storage
from .crawler import crawl, controller
from .scheduler import scheduler, schedule_first_monday

API_TOKEN = os.getenv('AUTH_TOKEN','CHANGE_ME')
WP_ORIGIN = os.getenv('WP_ORIGIN','*')

app = FastAPI(title="RM SEO Crawler")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[WP_ORIGIN] if WP_ORIGIN!='*' else ['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

async def auth(req: Request):
    auth = req.headers.get('authorization','')
    if not auth.startswith('Bearer '):
        raise HTTPException(401,'no token')
    token = auth.split(' ',1)[1]
    if token != API_TOKEN:
        raise HTTPException(403,'bad token')

@app.on_event('startup')
async def _startup():
    storage.init_db()
    if not scheduler.running:
        scheduler.start()

@app.get('/healthz')
async def healthz():
    return {"ok": True}

@app.post('/crawl/start')
async def crawl_start(payload: dict, _=Depends(auth)):
    domain = payload.get('domain','')
    if not domain: raise HTTPException(400,'domain required')
    crawl_id = str(uuid.uuid4())
    storage.write_summary(crawl_id, domain, 'running', 0, {'metrics':{}, 'started_at':'now'})
    asyncio.create_task(crawl(domain, crawl_id))
    return {"crawl_id": crawl_id}

@app.post('/crawl/stop')
async def crawl_stop(crawl_id: str, _=Depends(auth)):
    controller.stop(crawl_id)
    return {"ok": True}

@app.get('/crawl/status')
async def crawl_status(crawl_id: str, _=Depends(auth)):
    st = controller.status(crawl_id) or {"progress_0_100":0}
    return st

@app.get('/crawl/latest')
async def crawl_latest(domain: str, _=Depends(auth)):
    row = storage.query_latest(domain)
    if not row:
        return JSONResponse({"message":"no data"}, status_code=404)
    return row

@app.post('/schedule/monthly')
async def schedule_monthly(payload: dict, _=Depends(auth)):
    domain = payload.get('domain','')
    if not domain: raise HTTPException(400,'domain required')
    schedule_first_monday(domain)
    return {"ok": True}

@app.delete('/schedule/monthly')
async def unschedule_monthly(domain: str, _=Depends(auth)):
    from .scheduler import scheduler
    job_id = f"monthly::{domain}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    return {"ok": True}

@app.get('/data/{crawl_id}/{dataset}.csv')
async def csv_endpoint(crawl_id: str, dataset: str, _=Depends(auth)):
    gen = storage.stream_csv(crawl_id, dataset)
    if not gen:
        raise HTTPException(404,'dataset not found')
    return StreamingResponse(gen, media_type='text/csv; charset=utf-8')
