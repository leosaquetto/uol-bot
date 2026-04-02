import json

import os

import re

from datetime import datetime, timezone

from pathlib import Path

from typing import Any, Dict, List, Optional

from zoneinfo import ZoneInfo





import certifi

import requests

import urllib3

from bs4 import BeautifulSoup

from requests.exceptions import HTTPError, RequestException, SSLError





BASE\_URL = "https\://clube.uol.com.br"

LIST\_URL = f"{BASE\_URL}/?order=new"

FALLBACK\_LIST\_URL = f"{BASE\_URL}/"





HISTORY\_FILE = "historico\_leouol.json"

PENDING\_FILE = "pending\_offers.json"

DAILY\_LOG\_FILE = "daily\_log.json"

STATUS\_RUNTIME\_FILE = "status\_runtime.json"





SNAPSHOT\_DIR = "snapshots"

SNAPSHOT\_CONTROL\_FILE = "snapshots\_control.json"





REQUEST\_TIMEOUT = 30

MAX\_DASHBOARD\_LENGTH = 3900





TELEGRAM\_TOKEN = os.environ.get("TELEGRAM\_TOKEN")

GRUPO\_COMENTARIO\_ID = os.environ.get("GRUPO\_COMENTARIO\_ID")





USER\_AGENT = (

&#x20;   "Mozilla/5.0 (iPhone; CPU iPhone OS 17\_0 like Mac OS X) "

&#x20;   "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"

)





urllib3.disable\_warnings(urllib3.exceptions.InsecureRequestWarning)





BR\_TZ = ZoneInfo("America/Sao\_Paulo")







def now\_br() -> datetime:

&#x20;   return datetime.now(BR\_TZ)







def log(msg: str) -> None:

&#x20;   print(f"[{now\_br().strftime('%H:%M:%S')}] {msg}", flush=True)







def now\_br\_date() -> str:

&#x20;   return now\_br().strftime("%d/%m/%Y")







def now\_br\_time() -> str:

&#x20;   return now\_br().strftime("%H:%M")







def now\_br\_datetime() -> str:

&#x20;   return now\_br().strftime("%d/%m/%Y às %H:%M")







def load\_json(path: str, default: Any) -> Any:

&#x20;   if not os.path.exists(path):

&#x20;       return default

&#x20;   try:

&#x20;       with open(path, "r", encoding="utf-8") as f:

&#x20;           return json.load(f)

&#x20;   except Exception:

&#x20;       return default







def save\_json(path: str, data: Any) -> None:

&#x20;   with open(path, "w", encoding="utf-8") as f:

&#x20;       json.dump(data, f, indent=2, ensure\_ascii=False)







def load\_snapshot\_control() -> Dict[str, Any]:

&#x20;   return load\_json(SNAPSHOT\_CONTROL\_FILE, {"processed\_snapshot\_ids": []})





def save\_snapshot\_control(data: Dict[str, Any]) -> None:

&#x20;   save\_json(SNAPSHOT\_CONTROL\_FILE, data)





def list\_snapshot\_ids() -> List[str]:

&#x20;   if not os.path.exists(SNAPSHOT\_DIR):

&#x20;       return []



&#x20;   ids = []

&#x20;   for name in os.listdir(SNAPSHOT\_DIR):

&#x20;       if name.startswith("snapshot\_") and name.endswith(".json"):

&#x20;           snapshot\_id = name[len("snapshot\_"):-len(".json")]

&#x20;           ids.append(snapshot\_id)



&#x20;   ids.sort()

&#x20;   return ids





def load\_snapshot(snapshot\_id: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:

&#x20;   meta\_path = os.path.join(SNAPSHOT\_DIR, f"snapshot\_{snapshot\_id}.json")



&#x20;   if not os.path.exists(meta\_path):

&#x20;       return None, None



&#x20;   meta = load\_json(meta\_path, None)

&#x20;   if not isinstance(meta, dict):

&#x20;       return None, None



&#x20;   html\_path = str(meta.get("html\_path") or "").strip()

&#x20;   if not html\_path or not os.path.exists(html\_path):

&#x20;       return meta, None



&#x20;   try:

&#x20;       with open(html\_path, "r", encoding="utf-8") as f:

&#x20;           html = f.read()

&#x20;       return meta, html

&#x20;   except Exception:

&#x20;       return meta, None





def get\_unprocessed\_snapshot\_ids() -> tuple[List[str], Dict[str, Any]]:

&#x20;   control = load\_snapshot\_control()

&#x20;   processed = set(control.get("processed\_snapshot\_ids", []))

&#x20;   all\_ids = list\_snapshot\_ids()

&#x20;   pending\_ids = [snapshot\_id for snapshot\_id in all\_ids if snapshot\_id not in processed]

&#x20;   return pending\_ids, control





def mark\_snapshot\_processed(snapshot\_id: str, control: Dict[str, Any]) -> None:

&#x20;   processed = control.get("processed\_snapshot\_ids", [])

&#x20;   if not isinstance(processed, list):

&#x20;       processed = []



&#x20;   if snapshot\_id not in processed:

&#x20;       processed.append(snapshot\_id)



&#x20;   control["processed\_snapshot\_ids"] = processed[-500:]

&#x20;   save\_snapshot\_control(control)





def clean\_text(text: Optional[str]) -> str:

&#x20;   if not text:

&#x20;       return ""

&#x20;   text = str(text)

&#x20;   text = text.replace("\r\n", "\n").replace("\r", "\n")

&#x20;   text = re.sub(r"[ \t]+", " ", text)

&#x20;   text = re.sub(r"\n\s\*\n+", "\n\n", text)

&#x20;   text = re.sub(r"^ +| +\$", "", text, flags=re.MULTILINE)

&#x20;   return text.strip()







def html\_to\_text(html: str) -> str:

&#x20;   if not html:

&#x20;       return ""

&#x20;   text = html

&#x20;   text = re.sub(r"\<br\s\*/?>", "\n", text, flags=re.I)

&#x20;   text = re.sub(r"\</p>", "\n\n", text, flags=re.I)

&#x20;   text = re.sub(r"\</div>", "\n", text, flags=re.I)

&#x20;   text = re.sub(r"\<li[^>]\*>", "\n• ", text, flags=re.I)

&#x20;   text = re.sub(r"\</li>", "", text, flags=re.I)

&#x20;   text = re.sub(r"<[^>]+>", " ", text)

&#x20;   return clean\_text(text)







def escape\_html(text: str) -> str:

&#x20;   if not text:

&#x20;       return ""

&#x20;   return (

&#x20;       text.replace("&", "&amp;")

&#x20;       .replace("<", "&lt;")

&#x20;       .replace(">", "&gt;")

&#x20;       .replace('"', "&quot;")

&#x20;       .replace("'", "&#39;")

&#x20;   )







def parse\_br\_datetime(value: str) -> Optional[datetime]:

&#x20;   raw = str(value or "").strip()

&#x20;   if not raw or raw == "—":

&#x20;       return None

&#x20;   for fmt in ("%d/%m/%Y às %H:%M", "%d/%m/%Y %H:%M"):

&#x20;       try:

&#x20;           return datetime.strptime(raw, fmt).replace(tzinfo=BR\_TZ)

&#x20;       except Exception:

&#x20;           continue

&#x20;   for fmt in ("%d/%m às %H:%M", "%d/%m %H:%M"):

&#x20;       try:

&#x20;           partial = datetime.strptime(raw, fmt)

&#x20;           return partial.replace(year=now\_br().year, tzinfo=BR\_TZ)

&#x20;       except Exception:

&#x20;           continue

&#x20;   return None







def format\_relative\_time(value: str) -> str:

&#x20;   dt = parse\_br\_datetime(value)

&#x20;   if not dt:

&#x20;       return "Sem dados"

&#x20;   delta = now\_br() - dt

&#x20;   seconds = max(int(delta.total\_seconds()), 0)

&#x20;   if seconds < 60:

&#x20;       return "Agora"

&#x20;   minutes = seconds // 60

&#x20;   if minutes < 60:

&#x20;       return f"Há {minutes}min"

&#x20;   hours = minutes // 60

&#x20;   rem\_minutes = minutes % 60

&#x20;   if hours < 24:

&#x20;       return f"Há {hours}h{rem\_minutes:02d}min" if rem\_minutes else f"Há {hours}h"

&#x20;   days = hours // 24

&#x20;   rem\_hours = hours % 24

&#x20;   return f"Há {days}d{rem\_hours:02d}h" if rem\_hours else f"Há {days}d"







def truncate\_text(text: str, max\_len: int) -> str:

&#x20;   return text if len(text) <= max\_len else text[:max\_len]







def load\_daily\_log() -> Dict:

&#x20;   path = Path(DAILY\_LOG\_FILE)

&#x20;   default = {

&#x20;       "date": "",

&#x20;       "message\_id": None,

&#x20;       "last\_success\_check": "",

&#x20;       "last\_new\_offer\_at": "",

&#x20;       "pending\_count": 0,

&#x20;       "last\_consumer\_run": "",

&#x20;       "last\_rendered\_text": "",

&#x20;       "lines": [],

&#x20;   }

&#x20;   if not path.exists():

&#x20;       return default

&#x20;   try:

&#x20;       data = json.loads(path.read\_text(encoding="utf-8"))

&#x20;   except Exception:

&#x20;       data = default

&#x20;   lines = data.get("lines", [])

&#x20;   if not isinstance(lines, list):

&#x20;       lines = []

&#x20;   return {

&#x20;       "date": str(data.get("date") or ""),

&#x20;       "message\_id": data.get("message\_id"),

&#x20;       "last\_success\_check": str(data.get("last\_success\_check") or ""),

&#x20;       "last\_new\_offer\_at": str(data.get("last\_new\_offer\_at") or ""),

&#x20;       "pending\_count": int(data.get("pending\_count") or 0),

&#x20;       "last\_consumer\_run": str(data.get("last\_consumer\_run") or ""),

&#x20;       "last\_rendered\_text": str(data.get("last\_rendered\_text") or ""),

&#x20;       "lines": [str(x) for x in lines][-30:],

&#x20;   }







def save\_daily\_log(data: Dict) -> None:

&#x20;   Path(DAILY\_LOG\_FILE).write\_text(

&#x20;       json.dumps(data, ensure\_ascii=False, indent=2),

&#x20;       encoding="utf-8",

&#x20;   )







def load\_status\_runtime() -> Dict:

&#x20;   path = Path(STATUS\_RUNTIME\_FILE)

&#x20;   default = {

&#x20;       "scriptable": {

&#x20;           "last\_started\_at": "",

&#x20;           "last\_finished\_at": "",

&#x20;           "status": "",

&#x20;           "summary": "",

&#x20;           "offers\_seen": 0,

&#x20;           "new\_offers": 0,

&#x20;           "pending\_count": 0,

&#x20;           "last\_error": "",

&#x20;       },

&#x20;       "scraper": {

&#x20;           "last\_started\_at": "",

&#x20;           "last\_finished\_at": "",

&#x20;           "last\_success\_at": "",

&#x20;           "status": "",

&#x20;           "summary": "",

&#x20;           "offers\_seen": 0,

&#x20;           "new\_offers": 0,

&#x20;           "pending\_count": 0,

&#x20;           "last\_error": "",

&#x20;       },

&#x20;       "consumer": {

&#x20;           "last\_started\_at": "",

&#x20;           "last\_finished\_at": "",

&#x20;           "last\_success\_at": "",

&#x20;           "status": "",

&#x20;           "summary": "",

&#x20;           "processed": 0,

&#x20;           "sent": 0,

&#x20;           "failed": 0,

&#x20;           "pending\_count": 0,

&#x20;           "last\_error": "",

&#x20;       },

&#x20;       "global": {

&#x20;           "last\_offer\_title": "",

&#x20;           "last\_offer\_at": "",

&#x20;           "last\_offer\_id": "",

&#x20;       },

&#x20;   }

&#x20;   if not path.exists():

&#x20;       return default

&#x20;   try:

&#x20;       data = json.loads(path.read\_text(encoding="utf-8"))

&#x20;   except Exception:

&#x20;       data = default

&#x20;   for key, value in default.items():

&#x20;       if key not in data or not isinstance(data[key], dict):

&#x20;           data[key] = value

&#x20;   if "last\_success\_at" not in data["scraper"]:

&#x20;       data["scraper"]["last\_success\_at"] = ""

&#x20;   if "last\_success\_at" not in data["consumer"]:

&#x20;       data["consumer"]["last\_success\_at"] = ""

&#x20;   return data







def save\_status\_runtime(data: Dict) -> None:

&#x20;   Path(STATUS\_RUNTIME\_FILE).write\_text(

&#x20;       json.dumps(data, ensure\_ascii=False, indent=2),

&#x20;       encoding="utf-8",

&#x20;   )







def status\_scraper\_start() -> None:

&#x20;   status = load\_status\_runtime()

&#x20;   prev = status.get("scraper", {})

&#x20;   status["scraper"] = {

&#x20;       "last\_started\_at": now\_br\_datetime(),

&#x20;       "last\_finished\_at": prev.get("last\_finished\_at", ""),

&#x20;       "last\_success\_at": prev.get("last\_success\_at", ""),

&#x20;       "status": "running",

&#x20;       "summary": "scraper iniciado",

&#x20;       "offers\_seen": 0,

&#x20;       "new\_offers": 0,

&#x20;       "pending\_count": prev.get("pending\_count", 0),

&#x20;       "last\_error": "",

&#x20;   }

&#x20;   save\_status\_runtime(status)







def status\_scraper\_finish(

&#x20;   summary: str,

&#x20;   status\_value: str,

&#x20;   offers\_seen: int,

&#x20;   new\_offers: int,

&#x20;   pending\_count: int,

&#x20;   last\_error: str = "",

) -> None:

&#x20;   status = load\_status\_runtime()

&#x20;   prev = status.get("scraper", {})

&#x20;   last\_success\_at = prev.get("last\_success\_at", "")

&#x20;   if status\_value in {"ok", "sem\_novidade"} and not last\_error:

&#x20;       last\_success\_at = now\_br\_datetime()

&#x20;   status["scraper"] = {

&#x20;       "last\_started\_at": prev.get("last\_started\_at", ""),

&#x20;       "last\_finished\_at": now\_br\_datetime(),

&#x20;       "last\_success\_at": last\_success\_at,

&#x20;       "status": status\_value,

&#x20;       "summary": summary,

&#x20;       "offers\_seen": offers\_seen,

&#x20;       "new\_offers": new\_offers,

&#x20;       "pending\_count": pending\_count,

&#x20;       "last\_error": last\_error,

&#x20;   }

&#x20;   save\_status\_runtime(status)







def telegram\_api(method: str) -> str:

&#x20;   return f"https\://api.telegram.org/bot{TELEGRAM\_TOKEN}/{method}"







def map\_operation\_status(source: str, status\_block: Dict, fallback\_detail: str) -> tuple[str, str, str]:

&#x20;   status\_value = str(status\_block.get("status") or "").strip().lower()

&#x20;   detail = str(status\_block.get("summary") or fallback\_detail or "Sem atualização registrada.").strip()

&#x20;   started\_at = str(status\_block.get("last\_started\_at") or "")

&#x20;   finished\_at = str(status\_block.get("last\_finished\_at") or "")

&#x20;   started\_dt = parse\_br\_datetime(started\_at)

&#x20;   finished\_dt = parse\_br\_datetime(finished\_at)

&#x20;   stale\_running = status\_value == "running" and started\_dt and (not finished\_dt or finished\_dt < started\_dt)



&#x20;   if source == "scriptable":

&#x20;       if stale\_running:

&#x20;           return ("🟡 Instável", "última execução ainda não consolidada", started\_at or finished\_at)

&#x20;       if status\_value in {"ok", "running", "sem\_novidade"}:

&#x20;           return ("🟢 Online", detail, finished\_at or started\_at)

&#x20;       if status\_value == "erro":

&#x20;           err = str(status\_block.get("last\_error") or detail or "Erro")

&#x20;           return ("🔴 Erro", err, finished\_at or started\_at)

&#x20;       return ("⚪ Sem dados", detail, finished\_at or started\_at)



&#x20;   if source == "scraper":

&#x20;       last\_success = str(status\_block.get("last\_success\_at") or "").strip()

&#x20;       if stale\_running:

&#x20;           return ("🟡 Instável", "rodada iniciada sem fechamento consistente", started\_at or finished\_at or last\_success)

&#x20;       if status\_value == "ok":

&#x20;           return ("🟢 Online", detail, finished\_at or started\_at or last\_success)

&#x20;       if status\_value == "sem\_novidade":

&#x20;           return ("⚪ Ocioso", detail, finished\_at or started\_at or last\_success)

&#x20;       if status\_value == "erro":

&#x20;           extra = f"Último sucesso às {last\_success.split(' às ')[-1]}" if last\_success else "Sem sucesso recente"

&#x20;           return ("🟡 Bloqueado", f"{extra} (check cloudflare)", finished\_at or started\_at or last\_success)

&#x20;       return ("⚪ Sem dados", detail, finished\_at or started\_at or last\_success)



&#x20;   if source == "consumer":

&#x20;       if stale\_running:

&#x20;           return ("🟡 Instável", "processamento iniciou mas não fechou corretamente", started\_at or finished\_at)

&#x20;       if status\_value == "running":

&#x20;           return ("🔵 Ativo", detail, started\_at or finished\_at)

&#x20;       if status\_value == "ok":

&#x20;           return ("✅ Concluído", detail, finished\_at or started\_at or str(status\_block.get("last\_success\_at") or ""))

&#x20;       if status\_value == "sem\_novidade":

&#x20;           return ("⚪ Ocioso", detail, finished\_at or started\_at or str(status\_block.get("last\_success\_at") or ""))

&#x20;       if status\_value == "parcial":

&#x20;           return ("🟡 Parcial", detail, finished\_at or started\_at or str(status\_block.get("last\_success\_at") or ""))

&#x20;       if status\_value == "erro":

&#x20;           err = str(status\_block.get("last\_error") or detail or "Erro")

&#x20;           return ("🔴 Erro", err, finished\_at or started\_at)

&#x20;       return ("⚪ Sem dados", detail, finished\_at or started\_at)



&#x20;   return ("⚪ Sem dados", detail, finished\_at or started\_at)







def get\_last\_offer\_snapshot(status: Dict) -> tuple[str, str]:

&#x20;   global\_block = status.get("global", {}) or {}

&#x20;   title = str(global\_block.get("last\_offer\_title") or "").strip()

&#x20;   detected\_at = str(global\_block.get("last\_offer\_at") or "").strip()

&#x20;   if title and detected\_at:

&#x20;       return title, detected\_at



&#x20;   latest\_data = load\_json("latest\_offers.json", {"offers": []})

&#x20;   latest\_offers = latest\_data.get("offers", []) if isinstance(latest\_data, dict) else []

&#x20;   if isinstance(latest\_offers, list) and latest\_offers:

&#x20;       last\_offer = latest\_offers[-1] or {}

&#x20;       latest\_title = str(last\_offer.get("title") or last\_offer.get("preview\_title") or "").strip()

&#x20;       latest\_detected = str(last\_offer.get("scraped\_at") or "").strip()



&#x20;       if latest\_title:

&#x20;           if latest\_detected:

&#x20;               try:

&#x20;                   dt = datetime.fromisoformat(latest\_detected.replace("Z", "+00:00")).astimezone(BR\_TZ)

&#x20;                   return latest\_title, dt.strftime("%d/%m às %H:%M")

&#x20;               except Exception:

&#x20;                   pass

&#x20;           return latest\_title, detected\_at or "—"



&#x20;   history\_data = load\_json(HISTORY\_FILE, {"ids": []})

&#x20;   history\_ids = history\_data.get("ids", []) if isinstance(history\_data, dict) else []

&#x20;   if isinstance(history\_ids, list) and history\_ids:

&#x20;       return str(history\_ids[-1]).strip(), detected\_at or "—"



&#x20;   return "Não disponível", "—"







def format\_elapsed\_since(value: str) -> str:

&#x20;   dt = parse\_br\_datetime(value)

&#x20;   if not dt:

&#x20;       return "sem oferta nova recente"

&#x20;   delta = now\_br() - dt

&#x20;   seconds = max(int(delta.total\_seconds()), 0)

&#x20;   if seconds < 60:

&#x20;       return f"{seconds}s sem oferta nova"

&#x20;   minutes = seconds // 60

&#x20;   if minutes < 60:

&#x20;       return f"{minutes}min sem oferta nova"

&#x20;   hours = minutes // 60

&#x20;   rem\_minutes = minutes % 60

&#x20;   if hours < 24:

&#x20;       return f"{hours}h{rem\_minutes:02d}m sem oferta nova"

&#x20;   days = hours // 24

&#x20;   rem\_hours = hours % 24

&#x20;   return f"{days}d{rem\_hours:02d}h sem oferta nova"







def format\_monitor\_dashboard(state: Dict, status: Dict) -> str:

&#x20;   st = status.get("scriptable", {})

&#x20;   sc = status.get("scraper", {})

&#x20;   co = status.get("consumer", {})



&#x20;   s\_status, \_s\_detail, s\_dt = map\_operation\_status("scriptable", st, str(st.get("summary") or "Sem atualização registrada."))

&#x20;   sc\_status, \_sc\_detail, sc\_dt = map\_operation\_status("scraper", sc, str(sc.get("summary") or "Sem atualização registrada."))

&#x20;   c\_status, \_c\_detail, c\_dt = map\_operation\_status("consumer", co, str(co.get("summary") or "Sem atualização registrada."))



&#x20;   def fmt(dt\_str: str) -> str:

&#x20;       rel = format\_relative\_time(dt\_str)

&#x20;       dt = parse\_br\_datetime(dt\_str)

&#x20;       if not dt:

&#x20;           return str(rel).lower() if rel != "Sem dados" else rel

&#x20;       rel\_txt = "agora" if str(rel).lower() == "agora" else str(rel).lower()

&#x20;       return f"{rel\_txt} às {dt.strftime('%H:%M')}"



&#x20;   last\_title, last\_at = get\_last\_offer\_snapshot(status)

&#x20;   pending\_count = state.get("pending\_count", 0)

&#x20;   scraper\_line\_status = ("🟠 Sob restrição" if "Bloqueado" in sc\_status else sc\_status).replace("⚪ Ocioso", "⚪ Em espera")

&#x20;   consumer\_line\_status = "✅ Pronto" if pending\_count == 0 and ("Ocioso" in c\_status or "Concluído" in c\_status or "sem\_novidade" in str(co.get("status", "")).lower()) else c\_status



&#x20;   dash = [

&#x20;       f"📊 \<b>Monitor Clube Uol\</b> ({escape\_html(now\_br\_time())})",

&#x20;       "",

&#x20;       f"📱 \<b>Scriptable\</b> {escape\_html(s\_status)} \<i>({escape\_html(fmt(s\_dt))})\</i>",

&#x20;       f"🤖 \<b>Scraper\</b> {escape\_html(scraper\_line\_status)} \<i>({escape\_html(fmt(sc\_dt))})\</i>",

&#x20;       f"📦 \<b>Consumer\</b> {escape\_html(consumer\_line\_status)} \<i>({escape\_html(fmt(c\_dt))})\</i>",

&#x20;       "",

&#x20;       f"🎯 \<b>Última captura\</b> 🕒 {escape\_html(last\_at)}",

&#x20;       f"↳ \<code>{escape\_html(last\_title)}\</code>",

&#x20;       f"⏳ \<i>{escape\_html(format\_elapsed\_since(last\_at))}\</i>",

&#x20;       "",

&#x20;       f"📦 \<b>Fila de processamento:\</b> {('🚀 ' + str(pending\_count) + ' ofertas aguardando') if pending\_count > 0 else '📭 Limpa'}",

&#x20;       "",

&#x20;       f"🌤️ \<b>Humor do sistema:\</b> {'Atenção no scraper' if 'Bloqueado' in sc\_status or 'restrição' in scraper\_line\_status.lower() or 'Erro' in sc\_status else ('Fila aquecida' if pending\_count > 0 or 'Ativo' in consumer\_line\_status else 'Tudo calmo')}",

&#x20;       f"🧭 \<b>Leitura do ambiente:\</b> {'Parcial' if 'Bloqueado' in sc\_status or 'restrição' in scraper\_line\_status.lower() else ('Alta' if 'Online' in s\_status else 'Moderada')}",

&#x20;   ]

&#x20;   return truncate\_text("\n".join(dash), MAX\_DASHBOARD\_LENGTH)







def sync\_daily\_dashboard(state: Dict) -> None:

&#x20;   if not TELEGRAM\_TOKEN or not GRUPO\_COMENTARIO\_ID:

&#x20;       return

&#x20;   status = load\_status\_runtime()

&#x20;   text = format\_monitor\_dashboard(state, status)

&#x20;   current\_text = str(state.get("last\_rendered\_text") or "")

&#x20;   if current\_text == text:

&#x20;       save\_daily\_log(state)

&#x20;       return

&#x20;   if state["date"] != now\_br\_date() or not state["message\_id"]:

&#x20;       state["date"] = now\_br\_date()

&#x20;       state["message\_id"] = None

&#x20;       state["lines"] = state.get("lines", [])[-12:]

&#x20;       text = format\_monitor\_dashboard(state, load\_status\_runtime())

&#x20;       try:

&#x20;           resp = requests.post(

&#x20;               telegram\_api("sendMessage"),

&#x20;               data={

&#x20;                   "chat\_id": GRUPO\_COMENTARIO\_ID,

&#x20;                   "text": text,

&#x20;                   "parse\_mode": "HTML",

&#x20;                   "disable\_notification": "true",

&#x20;                   "disable\_web\_page\_preview": "true",

&#x20;               },

&#x20;               timeout=REQUEST\_TIMEOUT,

&#x20;           )

&#x20;           if resp.ok:

&#x20;               data = resp.json()

&#x20;               if data.get("ok"):

&#x20;                   state["message\_id"] = data.get("result", {}).get("message\_id")

&#x20;                   state["last\_rendered\_text"] = text

&#x20;                   save\_daily\_log(state)

&#x20;           else:

&#x20;               log(f"falha ao criar dashboard diário: {resp.text}")

&#x20;       except Exception as e:

&#x20;           log(f"falha ao criar dashboard diário: {e}")

&#x20;       return

&#x20;   try:

&#x20;       resp = requests.post(

&#x20;           telegram\_api("editMessageText"),

&#x20;           data={

&#x20;               "chat\_id": GRUPO\_COMENTARIO\_ID,

&#x20;               "message\_id": state["message\_id"],

&#x20;               "text": text,

&#x20;               "parse\_mode": "HTML",

&#x20;               "disable\_web\_page\_preview": "true",

&#x20;           },

&#x20;           timeout=REQUEST\_TIMEOUT,

&#x20;       )

&#x20;       if resp.ok:

&#x20;           state["last\_rendered\_text"] = text

&#x20;           save\_daily\_log(state)

&#x20;       else:

&#x20;           try:

&#x20;               error\_data = resp.json()

&#x20;           except Exception:

&#x20;               error\_data = {}

&#x20;           description = str(error\_data.get("description") or "")

&#x20;           if "message is not modified" in description.lower():

&#x20;               state["last\_rendered\_text"] = text

&#x20;               save\_daily\_log(state)

&#x20;               return

&#x20;           log(f"falha ao editar dashboard diário: {resp.text}")

&#x20;   except Exception as e:

&#x20;       log(f"falha ao editar dashboard diário: {e}")







def append\_dashboard\_line(source: str, status\_line: str) -> None:

&#x20;   state = load\_daily\_log()

&#x20;   if state["date"] != now\_br\_date():

&#x20;       state = {

&#x20;           "date": now\_br\_date(),

&#x20;           "message\_id": None,

&#x20;           "last\_success\_check": "",

&#x20;           "last\_new\_offer\_at": state.get("last\_new\_offer\_at", ""),

&#x20;           "pending\_count": 0,

&#x20;           "last\_consumer\_run": state.get("last\_consumer\_run", ""),

&#x20;           "last\_rendered\_text": "",

&#x20;           "lines": [],

&#x20;       }

&#x20;   line = f"[{now\_br\_time()}] {source}: {status\_line}"

&#x20;   filtered = [l for l in state.get("lines", []) if f"] {source}:" not in l]

&#x20;   filtered.append(line)

&#x20;   state["lines"] = filtered[-12:]

&#x20;   sync\_daily\_dashboard(state)







def set\_dashboard\_success\_check() -> None:

&#x20;   state = load\_daily\_log()

&#x20;   if state["date"] != now\_br\_date():

&#x20;       state["date"] = now\_br\_date()

&#x20;       state["message\_id"] = None

&#x20;       state["lines"] = []

&#x20;       state["last\_rendered\_text"] = ""

&#x20;   state["last\_success\_check"] = now\_br\_datetime()

&#x20;   sync\_daily\_dashboard(state)







def set\_dashboard\_last\_new\_offer() -> None:

&#x20;   state = load\_daily\_log()

&#x20;   if state["date"] != now\_br\_date():

&#x20;       state["date"] = now\_br\_date()

&#x20;       state["message\_id"] = None

&#x20;       state["lines"] = []

&#x20;       state["last\_rendered\_text"] = ""

&#x20;   state["last\_new\_offer\_at"] = now\_br\_datetime()

&#x20;   sync\_daily\_dashboard(state)







def set\_dashboard\_pending\_count(count: int) -> None:

&#x20;   state = load\_daily\_log()

&#x20;   if state["date"] != now\_br\_date():

&#x20;       state["date"] = now\_br\_date()

&#x20;       state["message\_id"] = None

&#x20;       state["lines"] = []

&#x20;       state["last\_rendered\_text"] = ""

&#x20;   state["pending\_count"] = count

&#x20;   sync\_daily\_dashboard(state)







def absolutize\_url(url: Optional[str]) -> str:

&#x20;   if not url:

&#x20;       return ""

&#x20;   url = str(url).strip()

&#x20;   if url.startswith("http\://") or url.startswith("https\://"):

&#x20;       return url

&#x20;   if url.startswith("//"):

&#x20;       return "https:" + url

&#x20;   if url.startswith("/"):

&#x20;       return BASE\_URL + url

&#x20;   return f"{BASE\_URL}/{url}"







def get\_offer\_id(link: str) -> str:

&#x20;   try:

&#x20;       clean\_link = str(link).split("?")[0].rstrip("/")

&#x20;       return clean\_link.split("/")[-1]

&#x20;   except Exception:

&#x20;       return str(link or "").strip()







def normalize\_text\_key(value: Optional[str]) -> str:

&#x20;   raw = str(value or "").strip().lower()

&#x20;   if not raw:

&#x20;       return ""

&#x20;   replacements = {

&#x20;       "á": "a", "à": "a", "ã": "a", "â": "a",

&#x20;       "é": "e", "ê": "e",

&#x20;       "í": "i",

&#x20;       "ó": "o", "ô": "o", "õ": "o",

&#x20;       "ú": "u",

&#x20;       "ç": "c",

&#x20;   }

&#x20;   for src, dst in replacements.items():

&#x20;       raw = raw\.replace(src, dst)

&#x20;   raw = re.sub(r"https?://", "", raw)

&#x20;   raw = re.sub(r"[^a-z0-9]+", "-", raw)

&#x20;   raw = re.sub(r"-{2,}", "-", raw)

&#x20;   raw = raw\.strip("-")

&#x20;   return raw







def normalize\_offer\_key(value: str) -> str:

&#x20;   raw = str(value or "").strip().lower()

&#x20;   if not raw:

&#x20;       return ""

&#x20;   if raw\.startswith("http\://") or raw\.startswith("https\://"):

&#x20;       raw = get\_offer\_id(raw)

&#x20;   return normalize\_text\_key(raw)







def pick\_description\_anchor(description: str) -> str:

&#x20;   if not description:

&#x20;       return ""

&#x20;   lines = [clean\_text(x) for x in str(description).splitlines()]

&#x20;   filtered = []

&#x20;   blacklist\_starts = (

&#x20;       "beneficio valido",

&#x20;       "válido até",

&#x20;       "local",

&#x20;       "quando",

&#x20;       "importante",

&#x20;       "regras de resgate",

&#x20;       "atencao",

&#x20;       "atenção",

&#x20;       "enviar cupons por e-mail",

&#x20;       "preencha os campos abaixo",

&#x20;       "e-mail",

&#x20;       "mensagem",

&#x20;       "enviar",

&#x20;   )

&#x20;   for line in lines:

&#x20;       low = normalize\_text\_key(line)

&#x20;       if not low or len(low) < 12:

&#x20;           continue

&#x20;       if any(low\.startswith(normalize\_text\_key(x)) for x in blacklist\_starts):

&#x20;           continue

&#x20;       filtered.append(low)

&#x20;   return filtered[0][:160] if filtered else ""







def build\_dedupe\_key(title: str, validity: Optional[str], description: str) -> str:

&#x20;   title\_key = normalize\_text\_key(title)

&#x20;   validity\_key = normalize\_text\_key(validity or "")

&#x20;   desc\_key = pick\_description\_anchor(description)

&#x20;   parts = [x for x in [title\_key, validity\_key, desc\_key] if x]

&#x20;   return "|".join(parts)







def uniq\_by(items: List[Dict[str, Any]], key\_fn) -> List[Dict[str, Any]]:

&#x20;   out = []

&#x20;   seen = set()

&#x20;   for item in items:

&#x20;       key = key\_fn(item)

&#x20;       if not key or key in seen:

&#x20;           continue

&#x20;       seen.add(key)

&#x20;       out.append(item)

&#x20;   return out







def is\_bad\_banner\_url(url: Optional[str]) -> bool:

&#x20;   u = str(url or "").lower()

&#x20;   if not u:

&#x20;       return True

&#x20;   return (

&#x20;       "loader.gif" in u

&#x20;       or "/static/images/loader.gif" in u

&#x20;       or "/parceiros/" in u

&#x20;       or "/rodape/" in u

&#x20;       or "icon-instagram" in u

&#x20;       or "icon-facebook" in u

&#x20;       or "icon-twitter" in u

&#x20;       or "icon-youtube" in u

&#x20;       or "instagram.png" in u

&#x20;       or "facebook.png" in u

&#x20;       or "twitter.png" in u

&#x20;       or "youtube.png" in u

&#x20;       or "share-" in u

&#x20;       or "social" in u

&#x20;       or "logo-uol" in u

&#x20;       or "logo\_uol" in u

&#x20;   )







def is\_likely\_benefit\_banner(url: Optional[str]) -> bool:

&#x20;   u = str(url or "").lower()

&#x20;   if not u or is\_bad\_banner\_url(u):

&#x20;       return False

&#x20;   return (

&#x20;       "/beneficios/" in u

&#x20;       or "/campanhasdeingresso/" in u

&#x20;       or "cloudfront.net" in u

&#x20;   )







def build\_headers(referer: Optional[str] = None) -> Dict[str, str]:

&#x20;   return {

&#x20;       "User-Agent": USER\_AGENT,

&#x20;       "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,\*/\*;q=0.8",

&#x20;       "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",

&#x20;       "Referer": referer or (BASE\_URL + "/"),

&#x20;       "Connection": "keep-alive",

&#x20;       "Upgrade-Insecure-Requests": "1",

&#x20;       "Cache-Control": "no-cache",

&#x20;       "Pragma": "no-cache",

&#x20;   }







def fetch\_once(session: requests.Session, url: str, referer: Optional[str], verify\_value) -> requests.Response:

&#x20;   headers = build\_headers(referer)

&#x20;   response = session.get(

&#x20;       url,

&#x20;       headers=headers,

&#x20;       timeout=REQUEST\_TIMEOUT,

&#x20;       verify=verify\_value,

&#x20;       allow\_redirects=True,

&#x20;   )

&#x20;   return response







def fetch\_with\_fallback(session: requests.Session, url: str, referer: Optional[str] = None) -> Optional[str]:

&#x20;   try:

&#x20;       r = fetch\_once(session, url, referer, certifi.where())

&#x20;       r.raise\_for\_status()

&#x20;       return r.text

&#x20;   except SSLError as e:

&#x20;       log(f"ssl falhou com verificação padrão, tentando fallback sem verify: {e}")

&#x20;       try:

&#x20;           r = fetch\_once(session, url, referer, False)

&#x20;           r.raise\_for\_status()

&#x20;           return r.text

&#x20;       except HTTPError as http\_e:

&#x20;           status\_code = getattr(http\_e.response, "status\_code", None)

&#x20;           log(f"fallback sem verify retornou http {status\_code} para {url}")

&#x20;           return None

&#x20;       except RequestException as req\_e:

&#x20;           log(f"fallback sem verify falhou para {url}: {req\_e}")

&#x20;           return None

&#x20;   except HTTPError as e:

&#x20;       status\_code = getattr(e.response, "status\_code", None)

&#x20;       log(f"http {status\_code} ao buscar {url}")

&#x20;       return None

&#x20;   except RequestException as e:

&#x20;       log(f"erro de rede ao buscar {url}: {e}")

&#x20;       return None







def get\_html(url: str) -> Optional[str]:

&#x20;   session = requests.Session()

&#x20;   candidates = [(url, BASE\_URL + "/")]

&#x20;   if url == LIST\_URL:

&#x20;       candidates.append((FALLBACK\_LIST\_URL, BASE\_URL + "/"))

&#x20;   for candidate\_url, referer in candidates:

&#x20;       html = fetch\_with\_fallback(session, candidate\_url, referer)

&#x20;       if html:

&#x20;           return html

&#x20;   return None







def extract\_all\_img\_meta(block) -> List[Dict[str, Any]]:

&#x20;   imgs: List[Dict[str, Any]] = []

&#x20;   for img in block.select("img"):

&#x20;       src = (

&#x20;           img.get("data-src")

&#x20;           or img.get("data-original")

&#x20;           or img.get("data-lazy")

&#x20;           or img.get("src")

&#x20;           or ""

&#x20;       ).strip()

&#x20;       if not src or src.startswith("data\:image"):

&#x20;           continue

&#x20;       full\_src = absolutize\_url(src)

&#x20;       class\_names = " ".join(img.get("class", [])).lower()

&#x20;       title = (img.get("title") or "").strip().lower()

&#x20;       alt = (img.get("alt") or "").strip().lower()

&#x20;       try:

&#x20;           width = int(img.get("width") or 0)

&#x20;       except Exception:

&#x20;           width = 0

&#x20;       try:

&#x20;           height = int(img.get("height") or 0)

&#x20;       except Exception:

&#x20;           height = 0

&#x20;       imgs.append(

&#x20;           {

&#x20;               "src": full\_src,

&#x20;               "title": title,

&#x20;               "alt": alt,

&#x20;               "class\_name": class\_names,

&#x20;               "width": width,

&#x20;               "height": height,

&#x20;               "is\_partner\_path": "/parceiros/" in full\_src,

&#x20;               "is\_partner\_like": (

&#x20;                   "/parceiros/" in full\_src

&#x20;                   or "logo" in class\_names

&#x20;                   or "brand" in class\_names

&#x20;                   or "parceiro" in class\_names

&#x20;                   or "logo" in alt

&#x20;                   or bool(title)

&#x20;                   or (0 < width <= 220)

&#x20;                   or (0 < height <= 120)

&#x20;               ),

&#x20;           }

&#x20;       )

&#x20;   return uniq\_by(imgs, lambda x: x["src"])







def choose\_images\_from\_block(block) -> Dict[str, str]:

&#x20;   all\_imgs = extract\_all\_img\_meta(block)

&#x20;   partner\_img\_url = ""

&#x20;   img\_url = ""

&#x20;   partner\_candidates = [img for img in all\_imgs if img["is\_partner\_like"] or img["is\_partner\_path"]]

&#x20;   if partner\_candidates:

&#x20;       partner\_img\_url = partner\_candidates[0]["src"]

&#x20;   banner\_candidates = [

&#x20;       img for img in all\_imgs

&#x20;       if (not partner\_img\_url or img["src"] != partner\_img\_url) and is\_likely\_benefit\_banner(img["src"])

&#x20;   ]

&#x20;   if banner\_candidates:

&#x20;       img\_url = banner\_candidates[-1]["src"]

&#x20;   if not img\_url:

&#x20;       fallback\_candidates = [

&#x20;           img for img in all\_imgs

&#x20;           if (not partner\_img\_url or img["src"] != partner\_img\_url) and not is\_bad\_banner\_url(img["src"])

&#x20;       ]

&#x20;       if fallback\_candidates:

&#x20;           img\_url = fallback\_candidates[-1]["src"]

&#x20;   if not partner\_img\_url and len(all\_imgs) >= 2:

&#x20;       for img in all\_imgs:

&#x20;           if img["src"] != img\_url:

&#x20;               partner\_img\_url = img["src"]

&#x20;               break

&#x20;   return {"img\_url": img\_url, "partner\_img\_url": partner\_img\_url}







def parse\_offers(html: str) -> List[Dict[str, Any]]:

&#x20;   soup = BeautifulSoup(html, "lxml")

&#x20;   offers: List[Dict[str, Any]] = []

&#x20;   blocks = soup.select('[data-categoria="Ingressos Exclusivos"]')

&#x20;   if not blocks:

&#x20;       log("fallback: buscando blocos com menção a ingresso")

&#x20;       candidate\_blocks = soup.select("[data-categoria], .beneficio, .item-oferta, .oferta")

&#x20;       filtered = []

&#x20;       for block in candidate\_blocks:

&#x20;           low = block.get\_text(" ", strip=True).lower()

&#x20;           hrefs = " ".join(a.get("href", "") for a in block.select("a[href]")).lower()

&#x20;           if "ingresso" in low or "ingressos" in low or "campanhasdeingresso" in hrefs:

&#x20;               filtered.append(block)

&#x20;       blocks = filtered

&#x20;   log(f"blocos candidatos: {len(blocks)}")

&#x20;   for block in blocks:

&#x20;       try:

&#x20;           title\_el = block.select\_one(".titulo") or block.select\_one("h3") or block.select\_one("h2")

&#x20;           link\_el = block.select\_one("a[href]")

&#x20;           if not title\_el or not link\_el:

&#x20;               continue

&#x20;           title = clean\_text(title\_el.get\_text(" ", strip=True))

&#x20;           link = absolutize\_url(link\_el.get("href"))

&#x20;           images = choose\_images\_from\_block(block)

&#x20;           offer\_id = get\_offer\_id(link)

&#x20;           log(f"     main url: {images['img\_url'] or 'vazia'}")

&#x20;           log(f"     partner url: {images['partner\_img\_url'] or 'vazia'}")

&#x20;           offers.append({

&#x20;               "id": offer\_id,

&#x20;               "original\_link": link,

&#x20;               "preview\_title": title,

&#x20;               "title": title,

&#x20;               "link": link,

&#x20;               "img\_url": images["img\_url"],

&#x20;               "partner\_img\_url": images["partner\_img\_url"],

&#x20;           })

&#x20;           log(f"extraído: {title[:60]}")

&#x20;       except Exception as e:

&#x20;           log(f"erro ao parsear bloco: {e}")

&#x20;   return uniq\_by(offers, lambda o: normalize\_offer\_key(o.get("id") or o.get("link")))







def extract\_offer\_details(url: str, preview\_title: str) -> Dict[str, Any]:

&#x20;   full\_url = absolutize\_url(url)

&#x20;   log(f"acessando detalhes: {preview\_title[:50]}...")

&#x20;   try:

&#x20;       html = get\_html(full\_url)

&#x20;       if not html:

&#x20;           return {"title": preview\_title, "validity": None, "description": "descrição não disponível.", "detail\_img\_url": ""}

&#x20;       page\_title = preview\_title

&#x20;       for regex in [re.compile(r"\<h2[^>]\*>([\s\S]\*?)\</h2>", re.I), re.compile(r"\<h1[^>]\*>([\s\S]\*?)\</h1>", re.I)]:

&#x20;           m = regex.search(html)

&#x20;           if m:

&#x20;               candidate\_title = clean\_text(re.sub(r"<[^>]+>", " ", m.group(1)))

&#x20;               if candidate\_title:

&#x20;                   page\_title = candidate\_title

&#x20;                   break

&#x20;       all\_imgs = []

&#x20;       for m in re.finditer(r'\<img[^>]+(?\:data-src|data-original|data-lazy|src)=["\\']\([^"\\']+)["\\']', html, re.I):

&#x20;           src = absolutize\_url(m.group(1))

&#x20;           if src and not src.startswith("data\:image"):

&#x20;               all\_imgs.append(src)

&#x20;       detail\_img\_url = ""

&#x20;       detail\_candidates = [src for src in all\_imgs if is\_likely\_benefit\_banner(src)]

&#x20;       if detail\_candidates:

&#x20;           detail\_img\_url = detail\_candidates[-1]

&#x20;       else:

&#x20;           fallback\_detail = [src for src in all\_imgs if not is\_bad\_banner\_url(src)]

&#x20;           if fallback\_detail:

&#x20;               detail\_img\_url = fallback\_detail[-1]

&#x20;       validity = None

&#x20;       for regex in [

&#x20;           re.compile(r"[Bb]enefício válido de[^.!?\n]\*[.!?]?", re.I),

&#x20;           re.compile(r"[Vv]álido até[^.!?\n]\*[.!?]?", re.I),

&#x20;           re.compile(r"\d{2}/\d{2}/\d{4}[\s\S]{0,80}\d{2}/\d{2}/\d{4}", re.I),

&#x20;       ]:

&#x20;           m = regex.search(html)

&#x20;           if m:

&#x20;               validity = clean\_text(re.sub(r"<[^>]+>", " ", m.group(0)))

&#x20;               break

&#x20;       description = ""

&#x20;       for regex in [

&#x20;           re.compile(r'class=["\\'][^"\\']\*info-beneficio[^"\\']\*["\\'][^>]\*>([\s\S]\*?)(?:\<script|\<footer|class=["\\'][^"\\']\*box-compartilhar)', re.I),

&#x20;           re.compile(r'id=["\\']beneficio["\\'][^>]\*>([\s\S]\*?)(?:\<script|\<footer)', re.I),

&#x20;       ]:

&#x20;           m = regex.search(html)

&#x20;           if m:

&#x20;               description = html\_to\_text(m.group(1))

&#x20;               if len(description) >= 20:

&#x20;                   break

&#x20;       if not description or len(description) < 20:

&#x20;           description = "descrição detalhada não disponível."

&#x20;       return {

&#x20;           "title": page\_title,

&#x20;           "validity": validity,

&#x20;           "description": description[:4000],

&#x20;           "detail\_img\_url": detail\_img\_url,

&#x20;       }

&#x20;   except Exception as e:

&#x20;       log(f"erro ao extrair detalhes: {e}")

&#x20;       return {"title": preview\_title, "validity": None, "description": "descrição não disponível.", "detail\_img\_url": ""}







def extract\_history\_sets(history\_data: Dict[str, Any]) -> tuple[set, set]:

&#x20;   ids = history\_data.get("ids", [])

&#x20;   dedupe\_keys = history\_data.get("dedupe\_keys", [])

&#x20;   if not isinstance(ids, list):

&#x20;       ids = []

&#x20;   if not isinstance(dedupe\_keys, list):

&#x20;       dedupe\_keys = []

&#x20;   id\_set = {normalize\_offer\_key(x) for x in ids if normalize\_offer\_key(x)}

&#x20;   dedupe\_set = {str(x).strip() for x in dedupe\_keys if str(x).strip()}

&#x20;   return id\_set, dedupe\_set







def extract\_pending\_sets(pending\_data: Dict[str, Any]) -> tuple[set, set]:

&#x20;   offers = pending\_data.get("offers", [])

&#x20;   if not isinstance(offers, list):

&#x20;       offers = []

&#x20;   id\_set = set()

&#x20;   dedupe\_set = set()

&#x20;   for o in offers:

&#x20;       offer\_key = normalize\_offer\_key(o.get("id") or o.get("link"))

&#x20;       if offer\_key:

&#x20;           id\_set.add(offer\_key)

&#x20;       dedupe\_key = str(o.get("dedupe\_key") or "").strip()

&#x20;       if not dedupe\_key:

&#x20;           dedupe\_key = build\_dedupe\_key(

&#x20;               title=o.get("title") or o.get("preview\_title") or "",

&#x20;               validity=o.get("validity"),

&#x20;               description=o.get("description") or "",

&#x20;           )

&#x20;       if dedupe\_key:

&#x20;           dedupe\_set.add(dedupe\_key)

&#x20;   return id\_set, dedupe\_set







def main() -> None:

&#x20;   log("iniciando scraper")

&#x20;   status\_scraper\_start()



&#x20;   historico = load\_json(HISTORY\_FILE, {"ids": [], "dedupe\_keys": []})

&#x20;   pending = load\_json(PENDING\_FILE, {"last\_update": None, "offers": []})

&#x20;   if not isinstance(pending.get("offers"), list):

&#x20;       pending["offers"] = []



&#x20;   historico\_keys, historico\_dedupe = extract\_history\_sets(historico)

&#x20;   pending\_keys, pending\_dedupe = extract\_pending\_sets(pending)



&#x20;   snapshot\_ids, snapshot\_control = get\_unprocessed\_snapshot\_ids()



&#x20;   if snapshot\_ids:

&#x20;       log(f"snapshots pendentes encontrados: {len(snapshot\_ids)}")

&#x20;   else:

&#x20;       log("nenhum snapshot pendente; tentando buscar html direto")

&#x20;       html = get\_html(LIST\_URL)

&#x20;       if not html:

&#x20;           log("não foi possível obter html da lista nesta rodada; encerrando sem alterações")

&#x20;           append\_dashboard\_line("scraper", "⚠️ html indisponível / 405 / ssl")

&#x20;           status\_scraper\_finish(

&#x20;               summary="html indisponível / 405 / ssl",

&#x20;               status\_value="erro",

&#x20;               offers\_seen=0,

&#x20;               new\_offers=0,

&#x20;               pending\_count=len(pending.get("offers", [])),

&#x20;               last\_error="falha ao obter html da lista",

&#x20;           )

&#x20;           return

&#x20;       snapshot\_ids = ["\_\_live\_fetch\_\_"]



&#x20;   all\_offers = []

&#x20;   loaded\_snapshot\_ids = []





&#x20;   for snapshot\_id in snapshot\_ids:

&#x20;       if snapshot\_id == "\_\_live\_fetch\_\_":

&#x20;           html = get\_html(LIST\_URL)

&#x20;           if not html:

&#x20;               continue

&#x20;           source\_label = "live\_fetch"

&#x20;       else:

&#x20;           meta, html = load\_snapshot(snapshot\_id)

&#x20;           source\_label = snapshot\_id



&#x20;           if not html:

&#x20;               log(f"snapshot inválido ou sem html: {snapshot\_id}")

&#x20;               mark\_snapshot\_processed(snapshot\_id, snapshot\_control)

&#x20;               continue



&#x20;       set\_dashboard\_success\_check()

&#x20;       offers = parse\_offers(html)

&#x20;       log(f"total encontradas em {source\_label}: {len(offers)}")



&#x20;       all\_offers.extend(offers)



&#x20;       if snapshot\_id != "\_\_live\_fetch\_\_":

&#x20;           loaded\_snapshot\_ids.append(snapshot\_id)



&#x20;   offers = uniq\_by(all\_offers, lambda o: normalize\_offer\_key(o.get("id") or o.get("link")))

&#x20;   log(f"total consolidado após unir snapshots: {len(offers)}")



&#x20;   candidates = []

&#x20;   seen\_new\_offer\_keys = set()

&#x20;   seen\_new\_dedupe\_keys = set()



&#x20;   for offer in offers:

&#x20;       details = extract\_offer\_details(offer["link"], offer["preview\_title"])

&#x20;       final\_title = details["title"] or offer["title"]

&#x20;       final\_partner = absolutize\_url(offer.get("partner\_img\_url") or "")

&#x20;       final\_img = absolutize\_url(details["detail\_img\_url"] or "")

&#x20;       if not final\_img or is\_bad\_banner\_url(final\_img) or final\_img == final\_partner:

&#x20;           fallback\_img = absolutize\_url(offer.get("img\_url") or "")

&#x20;           if fallback\_img and not is\_bad\_banner\_url(fallback\_img) and fallback\_img != final\_partner:

&#x20;               final\_img = fallback\_img

&#x20;       if not final\_img or is\_bad\_banner\_url(final\_img) or final\_img == final\_partner:

&#x20;           final\_img = ""

&#x20;       offer\_key = normalize\_offer\_key(offer.get("id") or offer.get("link"))

&#x20;       dedupe\_key = build\_dedupe\_key(title=final\_title, validity=details["validity"], description=details["description"])

&#x20;       if not offer\_key and not dedupe\_key:

&#x20;           continue

&#x20;       if offer\_key and (offer\_key in historico\_keys or offer\_key in pending\_keys or offer\_key in seen\_new\_offer\_keys):

&#x20;           continue

&#x20;       if dedupe\_key and (dedupe\_key in historico\_dedupe or dedupe\_key in pending\_dedupe or dedupe\_key in seen\_new\_dedupe\_keys):

&#x20;           continue

&#x20;       if offer\_key:

&#x20;           seen\_new\_offer\_keys.add(offer\_key)

&#x20;       if dedupe\_key:

&#x20;           seen\_new\_dedupe\_keys.add(dedupe\_key)

&#x20;       candidates.append({

&#x20;           "id": offer["id"],

&#x20;           "original\_link": offer["original\_link"],

&#x20;           "preview\_title": offer["preview\_title"] or final\_title,

&#x20;           "title": final\_title,

&#x20;           "link": offer["link"],

&#x20;           "img\_url": final\_img,

&#x20;           "partner\_img\_url": final\_partner,

&#x20;           "validity": details["validity"],

&#x20;           "description": details["description"],

&#x20;           "dedupe\_key": dedupe\_key,

&#x20;           "scraped\_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),

&#x20;       })



&#x20;   log(f"novas fora de histórico/pending: {len(candidates)}")

&#x20;   if not candidates:

&#x20;       log("nenhuma oferta nova para adicionar")



&#x20;       for snapshot\_id in loaded\_snapshot\_ids:

&#x20;           mark\_snapshot\_processed(snapshot\_id, snapshot\_control)



&#x20;       set\_dashboard\_pending\_count(len(pending.get("offers", [])))

&#x20;       append\_dashboard\_line("scraper", "💤 sem ofertas novas")

&#x20;       status\_scraper\_finish(

&#x20;           summary="sem ofertas novas",

&#x20;           status\_value="sem\_novidade",

&#x20;           offers\_seen=len(offers),

&#x20;           new\_offers=0,

&#x20;           pending\_count=len(pending.get("offers", [])),

&#x20;           last\_error="",

&#x20;       )

&#x20;       return



&#x20;   pending["offers"].extend(candidates)

&#x20;   pending["offers"] = uniq\_by(

&#x20;       pending["offers"],

&#x20;       lambda o: str(o.get("dedupe\_key") or "").strip() or normalize\_offer\_key(o.get("id") or o.get("link"))

&#x20;   )

&#x20;   pending["last\_update"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

&#x20;   save\_json(PENDING\_FILE, pending)



&#x20;   for snapshot\_id in loaded\_snapshot\_ids:

&#x20;       mark\_snapshot\_processed(snapshot\_id, snapshot\_control)



&#x20;   set\_dashboard\_last\_new\_offer()

&#x20;   set\_dashboard\_pending\_count(len(pending["offers"]))

&#x20;   append\_dashboard\_line("scraper", f"✅ novas no pending: {len(candidates)}")

&#x20;   status\_scraper\_finish(

&#x20;       summary=f"novas no pending: {len(candidates)}",

&#x20;       status\_value="ok",

&#x20;       offers\_seen=len(offers),

&#x20;       new\_offers=len(candidates),

&#x20;       pending\_count=len(pending["offers"]),

&#x20;       last\_error="",

&#x20;   )

&#x20;   log(f"adicionadas ao pending: {len(candidates)}")

&#x20;   log("finalizado")





if \_\_name\_\_ == "\_\_main\_\_":

&#x20;   main()
