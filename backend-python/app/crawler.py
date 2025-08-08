import asyncio, httpx, json
from selectolax.parser import HTMLParser
from urllib import robotparser
from urllib.parse import urljoin, urlsplit
from collections import deque, defaultdict
from .utils import normalize_url, same_host, is_http, classify_title_len, classify_meta_len
from . import storage
from datetime import datetime, timezone

GOOGLE_MOBILE_UA = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
FALLBACK_UA = "RM-Crawler/1.0"

class CrawlController:
    def __init__(self):
        self._stops = set()
        self._statuses = {}

    def stop(self, crawl_id:str):
        self._stops.add(crawl_id)

    def status(self, crawl_id:str):
        return self._statuses.get(crawl_id)

    def set_status(self, crawl_id:str, s:dict):
        self._statuses[crawl_id] = s

controller = CrawlController()

async def fetch(client, url, allow_redirects=True):
    try:
        r = await client.get(url, follow_redirects=allow_redirects, timeout=20.0)
        return r
    except Exception:
        return None

def parse_meta(html:HTMLParser):
    title = (html.css_first('title').text() if html.css_first('title') else '')[:512]
    md = ''
    for m in html.css('meta'):
        n = (m.attributes.get('name') or m.attributes.get('property') or '').lower()
        if n == 'description' and 'content' in m.attributes:
            md = (m.attributes.get('content') or '')[:1024]
    h1 = html.css_first('h1')
    h1_text = h1.text().strip()[:512] if h1 else ''
    robots = ''
    for m in html.css('meta'):
        n = (m.attributes.get('name') or '').lower()
        if n == 'robots':
            robots = (m.attributes.get('content') or '')
            break
    canonical = ''
    link = html.css_first('link[rel="canonical"]')
    if link and 'href' in link.attributes:
        canonical = link.attributes['href']
    return title, md, h1_text, robots, canonical

async def head_or_get_status(client, url):
    try:
        r = await client.head(url, follow_redirects=False, timeout=15.0)
        return r.status_code
    except Exception:
        try:
            r = await client.get(url, follow_redirects=False, timeout=20.0)
            return r.status_code
        except Exception:
            return 0

def detect_sd_types(html:HTMLParser):
    types = set()
    errors = 0
    for s in html.css('script[type="application/ld+json"]'):
        try:
            data = json.loads(s.text())
            def walk(x):
                if isinstance(x, dict):
                    t = x.get('@type')
                    if isinstance(t, list):
                        for it in t: types.add(str(it))
                    elif isinstance(t, str):
                        types.add(t)
                    for v in x.values(): walk(v)
                elif isinstance(x, list):
                    for v in x: walk(v)
            walk(data)
        except Exception:
            errors += 1
    for el in html.css('[itemtype]'):
        t = el.attributes.get('itemtype','')
        if t and '/' in t:
            t = t.split('/')[-1]
        if t:
            types.add(t)
    wanted = ["Product","BreadcrumbList","Article","FAQPage","WebSite","Organization","Offer","AggregateRating"]
    out = {f"sd_{w}": 1 if w in types else 0 for w in wanted}
    out['parse_errors'] = errors
    return out

async def crawl(domain:str, crawl_id:str, max_depth:int=6, max_urls:int=50000):
    start_url = domain.rstrip('/') + '/'
    rp = robotparser.RobotFileParser()
    rp.set_url(start_url.split('/',3)[0]+'//'+urlsplit(start_url).netloc+'/robots.txt')
    try:
        rp.read()
    except Exception:
        pass

    headers = {"User-Agent": GOOGLE_MOBILE_UA}
    if not rp.can_fetch(GOOGLE_MOBILE_UA, start_url):
        headers = {"User-Agent": FALLBACK_UA}

    limits = asyncio.Semaphore(2)
    interval = 0.5

    visited = set()
    q = deque([(start_url,0)])

    urls_rows = []
    redirects_rows = []
    broken_rows = []
    images_rows = []
    sd_rows = []

    title_index = defaultdict(list)
    meta_index = defaultdict(list)

    started_at = datetime.now(timezone.utc).isoformat()

    async with httpx.AsyncClient(headers=headers) as client:
        while q and len(visited) < max_urls:
            if crawl_id in controller._stops: break
            url, depth = q.popleft()
            if depth>max_depth: continue
            nurl = normalize_url(url)
            if nurl in visited: continue
            if not is_http(nurl): continue
            if not same_host(nurl, start_url): continue
            if any(p in nurl for p in ["/wp-admin/","/cart/","/checkout/"]) : continue
            if any(k in nurl for k in ["?add-to-cart=","?orderby=","?s="]): continue
            if not rp.can_fetch(headers["User-Agent"], nurl): continue
            visited.add(nurl)

            controller.set_status(crawl_id, {"progress_0_100": int(100*len(visited)/max_urls), "current_url": nurl})

            async with limits:
                await asyncio.sleep(interval)
                r = await fetch(client, nurl)
            status = r.status_code if r else 0
            html = HTMLParser(r.text) if (r and r.text) else HTMLParser("")

            title, meta_desc, h1_text, robots_meta, canonical = parse_meta(html)
            tlen = len(title or '')
            mlen = len(meta_desc or '')

            canonical_status = 'missing'
            if canonical:
                from urllib.parse import urljoin as _u
                cnorm = normalize_url(_u(nurl, canonical))
                canonical_status = 'self' if cnorm == nurl else ('cross' if same_host(cnorm, start_url) else 'conflict')
            
            urls_rows.append({
                "crawl_id": crawl_id,
                "url": nurl,
                "status_code": status,
                "crawl_depth": depth,
                "canonical": canonical or '',
                "canonical_status": canonical_status,
                "robots": robots_meta,
                "title": title or '',
                "title_length": tlen,
                "title_status": classify_title_len(tlen),
                "meta_description": meta_desc or '',
                "meta_length": mlen,
                "meta_status": classify_meta_len(mlen),
                "h1_present": 1 if h1_text else 0,
                "h1_text": h1_text
            })

            if title:
                key = title.strip().lower()
                title_index[key].append(nurl)
            if meta_desc:
                key = meta_desc.strip().lower()
                meta_index[key].append(nurl)

            anchors = html.css('a[href]')
            for a in anchors:
                href = a.attributes.get('href','')
                if not href: continue
                absu = urljoin(nurl, href)
                if absu.startswith('mailto:') or absu.startswith('tel:'): continue
                if not is_http(absu): continue

                direction = 'internal' if same_host(absu, start_url) else 'external'
                code = await head_or_get_status(client, absu)
                if code>=400:
                    broken_rows.append({
                        "crawl_id": crawl_id,
                        "source_url": nurl,
                        "anchor_text": (a.text() or '').strip(),
                        "direction": direction,
                        "target_url": absu,
                        "target_status": code,
                        "html_context": 'a[href]',
                        "first_seen": started_at,
                        "last_seen": started_at
                    })
                elif 300<=code<400 and direction=='internal':
                    chain = [absu]; types = []
                    nextu = absu; hops=0; loop=False
                    while hops<5:
                        hops+=1
                        try:
                            rr = await client.get(nextu, follow_redirects=False, timeout=15.0)
                            if rr.status_code in (301,302,303,307,308) and 'location' in rr.headers:
                                types.append(str(rr.status_code))
                                from urllib.parse import urljoin as _j
                                loc = _j(nextu, rr.headers['location'])
                                if loc in chain: loop=True; break
                                chain.append(loc); nextu = loc
                            else:
                                break
                        except Exception:
                            break
                    redirects_rows.append({
                        "crawl_id": crawl_id,
                        "source_url": nurl,
                        "chain_length": len(chain)-1,
                        "chain": ";".join(chain),
                        "final_url": chain[-1],
                        "redirect_types": ";".join(types),
                        "internal_only": 1 if all(same_host(u,start_url) for u in chain) else 0,
                        "loop": 1 if loop else 0
                    })

                if direction=='internal' and (absu not in visited) and depth+1<=max_depth:
                    q.append((absu, depth+1))

            imgs = html.css('img[src]')
            img_count = len(imgs); legacy = 0; modern = 0
            for im in imgs:
                src = im.attributes.get('src','').lower()
                ext = src.split('?')[0].split('#')[0]
                if ext.endswith('.webp') or ext.endswith('.avif'): modern+=1
                elif any(ext.endswith(e) for e in ('.jpg','.jpeg','.png','.gif')): legacy+=1
            images_rows.append({
                "crawl_id": crawl_id,
                "url": nurl,
                "img_count": img_count,
                "legacy_img_count": legacy,
                "webp_avif_count": modern
            })

            sd = detect_sd_types(html)
            sd_rows.append({"crawl_id":crawl_id, "url":nurl, **sd})

    dtitle_rows=[]; dmeta_rows=[]
    for k,urls in title_index.items():
        if len(urls)>1:
            dtitle_rows.append({"crawl_id": crawl_id,"title_hash": str(abs(hash(k))),"title_sample": k[:180],"url_count": len(urls),"urls_sample": ", ".join(urls[:10])})
    for k,urls in meta_index.items():
        if len(urls)>1:
            dmeta_rows.append({"crawl_id": crawl_id,"meta_hash": str(abs(hash(k))),"meta_sample": k[:180],"url_count": len(urls),"urls_sample": ", ".join(urls[:10])})

    storage.insert_many('urls', urls_rows)
    storage.insert_many('redirects', redirects_rows)
    storage.insert_many('broken_links', broken_rows)
    storage.insert_many('images', images_rows)
    storage.insert_many('structured_data', sd_rows)
    storage.insert_many('duplicates_titles', dtitle_rows)
    storage.insert_many('duplicates_meta', dmeta_rows)

    total = len(urls_rows)
    missing_h1 = sum(1 for r in urls_rows if not r['h1_present'])
    summary = {
        'started_at': started_at,
        'finished_at': datetime.now(timezone.utc).isoformat(),
        'metrics': {
            'urls': total,
            'broken_links': len(broken_rows),
            'redirects': len(redirects_rows),
            'missing_h1_pct': round(100.0*missing_h1/max(total,1),1),
            'urls': total,
            'broken_links': len(broken_rows),
            'redirects': len(redirects_rows),
            'duplicates_titles': len(dtitle_rows),
            'duplicates_meta': len(dmeta_rows),
            'images': len(images_rows),
            'structured_data': len(sd_rows)
        }
    }
    storage.write_summary(crawl_id, start_url, 'finished', total, summary)
    controller.set_status(crawl_id, {"progress_0_100":100, "current_url":"done", "finished":True})
