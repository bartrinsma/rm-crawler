from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz, uuid, asyncio
from .crawler import crawl

TZ = pytz.timezone('Europe/Amsterdam')
scheduler = BackgroundScheduler(timezone=TZ)

async def _run(domain:str, crawl_id:str):
    await crawl(domain, crawl_id)

def schedule_first_monday(domain:str):
    trig = CronTrigger(day='1-7', day_of_week='mon', hour=9, minute=0, timezone=TZ)
    job_id = f"monthly::{domain}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    scheduler.add_job(lambda: asyncio.run(_run(domain, str(uuid.uuid4()))), trigger=trig, id=job_id, replace_existing=True)
