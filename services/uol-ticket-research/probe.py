import datetime, html.parser, re, threading, time
import urllib.request, urllib.error, urllib.parse

ORIGIN = 'https://clube.uol.com.br'
CATALOG_URLS = [ORIGIN+'/?order=new', ORIGIN+'/?categoria=ingressosexclusivos&order=new', ORIGIN+'/?order=new&order=&offset=48', ORIGIN+'/?order=new&order=&offset=96']
MAX_BYTES = 1024 * 1024
STOP = threading.Event()
LOCK = threading.Lock()
REQUESTS = 0
MAX_REQUESTS = 650
DEADLINE = 0
BLOCKED_REASON = ''

def configure(max_requests=650, deadline_seconds=480):
    global REQUESTS, MAX_REQUESTS, DEADLINE, BLOCKED_REASON
    with LOCK:
        REQUESTS = 0
        MAX_REQUESTS = min(650, max(1, int(max_requests)))
        DEADLINE = time.monotonic() + min(480, max(1, int(deadline_seconds)))
        BLOCKED_REASON = ''
        STOP.clear()

def request_count():
    with LOCK: return REQUESTS

def blocked_reason():
    with LOCK: return BLOCKED_REASON

class Node:
    def __init__(self, tag='', attrs=None, parent=None):
        self.tag, self.attrs, self.parent = tag, dict(attrs or []), parent
        self.children = []
    def text(self):
        if self.tag in {'script','style','noscript','template'}: return ''
        return ' '.join(x.text() if isinstance(x, Node) else x for x in self.children)
    def nodes(self):
        yield self
        for x in self.children:
            if isinstance(x, Node): yield from x.nodes()
    def has_class(self, name): return name in self.attrs.get('class', '').split()

class Parser(html.parser.HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = Node(); self.current = self.root
    def handle_starttag(self, tag, attrs):
        n = Node(tag, attrs, self.current); self.current.children.append(n)
        if tag not in {'area','base','br','col','embed','hr','img','input','link','meta','param','source','track','wbr'}: self.current = n
    def handle_startendtag(self, tag, attrs): self.current.children.append(Node(tag, attrs, self.current))
    def handle_endtag(self, tag):
        n = self.current
        while n.parent:
            if n.tag == tag: self.current = n.parent; return
            n = n.parent
    def handle_data(self, data): self.current.children.append(data)

def clean(s): return re.sub(r'\s+', ' ', s).strip()
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def allowed(url, code, full=False, legacy=False):
    raw = str(url or '').strip()
    if re.search(r'[\s\\%?#]', raw): return ''
    if any(part in {'.','..'} for part in raw.split('/')): return ''
    try:
        u = urllib.parse.urlsplit(urllib.parse.urljoin(ORIGIN, raw))
        if u.scheme != 'https' or u.username or u.password or u.port: return ''
    except ValueError: return ''
    if u.hostname not in ({'clube.uol.com.br','clubeuol.clubeben.com.br'} if legacy else {'clube.uol.com.br'}): return ''
    m = re.fullmatch(r'/campanhasdeingresso/(p[A-Za-z0-9]{2,5})(-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*)?', u.path)
    if not m or m[1] != code or (full and not m[2]): return ''
    return ORIGIN + u.path

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl): return None

def get(url):
    global REQUESTS, BLOCKED_REASON
    try:
        u = urllib.parse.urlsplit(url)
        code = re.fullmatch(r'/campanhasdeingresso/(p[A-Za-z0-9]{2,5})(?:-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*)?',u.path)
        safe = url in CATALOG_URLS or (code and allowed(url,code[1])==url)
    except ValueError: safe = False
    if not safe: raise ValueError('public_read_url_blocked')
    with LOCK:
        if not STOP.is_set() and (REQUESTS >= MAX_REQUESTS or (DEADLINE and time.monotonic() >= DEADLINE)):
            BLOCKED_REASON = 'request_budget' if REQUESTS >= MAX_REQUESTS else 'run_deadline'
            STOP.set()
        if STOP.is_set(): return (None, {}, '', BLOCKED_REASON or 'stopped_after_block')
        REQUESTS += 1
    req = urllib.request.Request(url, headers={'Accept':'text/html','Cache-Control':'no-cache','User-Agent':'UOLTelegramCloudflare/1.0'}, method='GET')
    try:
        with urllib.request.build_opener(NoRedirect).open(req, timeout=10) as r:
            headers = dict(r.headers.items()); body = r.read(MAX_BYTES+1)
            if len(body) > MAX_BYTES: return (r.status, headers, '', 'body_too_large')
            return r.status, headers, body.decode('utf-8', errors='replace'), ''
    except urllib.error.HTTPError as e:
        try:
            if e.code in (403,429):
                with LOCK:
                    BLOCKED_REASON = 'http_'+str(e.code)
                    STOP.set()
            return e.code, dict(e.headers.items()), '', ''
        finally: e.close()
    except Exception as e: return None, {}, '', type(e).__name__
    finally: time.sleep(0.4)

def detail(url, code):
    status, headers, text, error = get(url)
    meta = {'httpStatus':status}
    if error: return {'status':'unknown','reason':error,**meta}
    if status in (301,302,303,307,308):
        location = headers.get('Location', headers.get('location',''))
        if not location: return {'status':'unknown','reason':'missing_redirect_location',**meta}
        dest = urllib.parse.urlsplit(urllib.parse.urljoin(ORIGIN, location))
        return {'status':'absent' if dest.hostname=='clube.uol.com.br' and dest.path in ('/','/index.html') else 'unknown', 'reason':'home_redirect' if dest.hostname=='clube.uol.com.br' and dest.path in ('/','/index.html') else 'unexpected_redirect', **meta}
    if status in (404,410): return {'status':'absent','reason':'not_found',**meta}
    if status != 200: return {'status':'unknown','reason':'rate_limited' if status==429 else 'http_error',**meta}
    if not headers.get('Content-Type',headers.get('content-type','')).lower().startswith('text/html'): return {'status':'unknown','reason':'non_html',**meta}
    p=Parser(); p.feed(text); allnodes=list(p.root.nodes())
    containers=[n for n in allnodes if n.attrs.get('id')=='beneficio']
    canon=[n.attrs.get('href','') for n in allnodes if n.tag=='link' and n.attrs.get('rel')=='canonical']
    og=[n.attrs.get('content','') for n in allnodes if n.tag=='meta' and n.attrs.get('property')=='og:url']
    if len(containers)!=1 or len(canon)!=1 or len(og)!=1: return {'status':'unknown','reason':'invalid_structure',**meta}
    if any(allowed(v,code)!=url for v in canon+og): return {'status':'unknown','reason':'identity_mismatch',**meta}
    nodes=list(containers[0].nodes()); titles=[n for n in nodes if n.tag=='h2']; desc=[n for n in nodes if n.has_class('info-beneficio')]; cta=[n for n in nodes if n.attrs.get('id')=='rescue_button']
    if len(titles)!=1 or len(desc)!=1 or len(cta)>1: return {'status':'unknown','reason':'invalid_structure',**meta}
    title=clean(titles[0].text()); description=clean(desc[0].text())
    if len(title)<4 or len(description)<30: return {'status':'unknown','reason':'incomplete_detail',**meta}
    # The CTA is observed only as metadata; it is never followed. Its presence
    # does not establish stock or eligibility, and absence need not hide a page.
    aliases=set()
    for n in allnodes:
        if n.has_class('fb-like'):
            a=allowed(n.attrs.get('data-href',''),code,True,True)
            if a: aliases.add(a)
    for v in canon+og:
        a=allowed(v,code,True)
        if a: aliases.add(a)
    if len(aliases)!=1: return {'status':'unknown','reason':'ambiguous_or_missing_alias',**meta}
    link=next(iter(aliases))
    if allowed(url,code,True) and link!=url: return {'status':'unknown','reason':'identity_mismatch',**meta}
    validity=[]
    for n in allnodes:
        if n.tag=='p':
            t=clean(n.text())
            if t.startswith('Benefício válido de'): validity.append(t)
    explicit_sold_out=any(n.has_class('esgotado') or n.has_class('sold-out') for n in nodes) or bool(cta and re.search(r'esgotad[oa]',clean(cta[0].text()),re.I))
    return {'status':'found','reason':'','title':title,'link':link,'description':description,'validity':list(dict.fromkeys(validity)), 'stock':'explicit_sold_out' if explicit_sold_out else 'unknown',**meta}

def probe_code(code):
    if not re.fullmatch(r'p[A-Za-z0-9]{2,5}',str(code)):
        return {'code':code,'status':'unknown','reason':'invalid_code','observedAt':now(),'requests':0,'httpStatuses':[]}
    first=detail(ORIGIN+'/campanhasdeingresso/'+code,code)
    statuses=[first.get('httpStatus')]; count=1
    result=first
    if first['status']=='found':
        result=detail(first['link'],code); statuses.append(result.get('httpStatus')); count=2
    return {'code':code,**result,'observedAt':now(),'requests':count,'httpStatuses':statuses,'blocked':any(s in (403,429) for s in statuses)}

def fetch_catalogs():
    urls=CATALOG_URLS
    pages=[]; cards={}
    for url in urls:
        status,headers,body,error=get(url)
        entry={'url':url,'status':status,'error':error or None,'unique_cards':0}
        if status==200 and body:
            p=Parser();p.feed(body); unique=set()
            for n in p.root.nodes():
                if n.tag!='div' or not n.has_class('beneficio'): continue
                anchors=[a for a in n.nodes() if a.tag=='a' and a.attrs.get('href')]
                titles=[a for a in n.nodes() if a.tag=='p' and a.has_class('titulo')]
                if not anchors or not titles: continue
                link=urllib.parse.urljoin(ORIGIN,anchors[0].attrs['href']); u=urllib.parse.urlsplit(link)
                code=re.search(r'^/[^/]+/(p[A-Za-z0-9]{2,5})-',u.path)
                if u.hostname!='clube.uol.com.br' or not code: continue
                key=(code[1],link);unique.add(key)
                cards.setdefault(key,{'code':code[1],'url':link,'title':clean(titles[0].text()),'category':n.attrs.get('data-categoria',''),'catalogs':[]})['catalogs'].append(url)
            entry['unique_cards']=len(unique)
        pages.append(entry)
    return {'checkedAt':now(),'pages':pages,'offers':list(cards.values()),'complete':all(p['status']==200 and not p['error'] and p['unique_cards']>0 for p in pages)}
