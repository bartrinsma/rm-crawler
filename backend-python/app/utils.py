import urllib.parse as up

STRIP_PARAMS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","gclid","fbclid","_ga","_gl"}
SESSION_KEYS = {"phpsessid","sessionid","sid"}

def normalize_url(url: str) -> str:
    u = up.urlsplit(url)
    q = up.parse_qsl(u.query, keep_blank_values=True)
    q = [(k,v) for (k,v) in q if k.lower() not in STRIP_PARAMS and k.lower() not in SESSION_KEYS]
    query = up.urlencode(q, doseq=True)
    path = u.path or "/"
    # unify trailing slash except for files
    if not path.split("/")[-1].count('.') and not path.endswith('/'):
        path += '/'
    return up.urlunsplit((u.scheme.lower(), u.netloc.lower(), path, query, ""))

def same_host(a:str,b:str)->bool:
    return up.urlsplit(a).netloc.lower()==up.urlsplit(b).netloc.lower()

def is_http(url:str)->bool:
    return url.startswith("http://") or url.startswith("https://")

TITLE_SHORT, TITLE_LONG = 30, 65
META_SHORT, META_LONG = 70, 160

def classify_title_len(n:int)->str:
    if n==0: return "missing"
    if n < TITLE_SHORT: return "short"
    if n > TITLE_LONG: return "long"
    return "ok"

def classify_meta_len(n:int)->str:
    if n==0: return "missing"
    if n < META_SHORT: return "short"
    if n > META_LONG: return "long"
    return "ok"
