# bot_leouol.py - Versão CORRIGIDA (mesmo método do Scriptable com Fallback de Imagem)

import sys
import requests
import json
import os
import time
import re
import random
import urllib3
from datetime import datetime
from pathlib import Path

# Suprime os avisos de conexão insegura no terminal por usar verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==============================================
# CONFIGURAÇÕES
# ==============================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIOS_ID = os.environ.get("GRUPO_COMENTARIO_ID", "-1003802235343")

TARGET_URL = "https://clube.uol.com.br/?order=new"

HISTORY_FILE = "historico_leouol.json"
MAX_OFFERS_PER_RUN = 10
MAX_HISTORY_SIZE = 200
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096

HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://clube.uol.com.br/"
}

# ==============================================
# FUNÇÕES UTILITÁRIAS
# ==============================================
def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def clean_text(text: str) -> str:
    if not text: return ""
    cleaned = re.sub(r'[ \t]+', ' ', text)
    cleaned = re.sub(r'\n\s*\n+', '\n\n', cleaned)
    cleaned = re.sub(r'^ +| +$', '', cleaned, flags=re.MULTILINE)
    return cleaned.strip()

def escape_html(text: str) -> str:
    if not text: return ""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&#39;")

def get_offer_id(link: str) -> str:
    try:
        link_clean = link.split('?')[0]
        return link_clean.rstrip('/').split('/')[-1]
    except:
        return link

def load_history():
    path = Path(HISTORY_FILE)
    if not path.exists(): return {"ids": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        ids = data.get("ids", [])
        return {"ids": list(dict.fromkeys([get_offer_id(str(x)) for x in ids]))[-MAX_HISTORY_SIZE:]}
    except Exception:
        return {"ids": []}

def save_history(history):
    try:
        ids = list(dict.fromkeys([get_offer_id(str(x)) for x in history.get("ids", [])]))[-MAX_HISTORY_SIZE:]
        Path(HISTORY_FILE).write_text(json.dumps({"ids": ids}, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"✅ Histórico salvo: {len(ids)} IDs")
        return True
    except:
        return False

def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    if len(text) <= max_len: return text
    return text[:max_len - len(suffix)] + suffix

# ==============================================
# SCRAPER - EXATAMENTE IGUAL AO SCRIPTABLE
# ==============================================
def fetch_offers():
    offers = []
    seen_links = set()
    
    log(f"\n🌐 Buscando ofertas em: {TARGET_URL}")
    
    try:
        res = requests.get(TARGET_URL, headers=HEADERS, timeout=15, verify=False)
        html = res.text
        log(f"✅ Página baixada: {len(html)} caracteres")
        
        blocks = re.split(r'<div[^>]*data-categoria=[\'"]', html, flags=re.IGNORECASE)
        log(f"📦 Blocos encontrados: {len(blocks) - 1}")
        
        for block in blocks[1:]:  # Pula o primeiro (cabeçalho)
            try:
                # 1. LINK
                link_match = re.search(r'<a[^>]*href="([^"]+)"', block, re.IGNORECASE)
                if not link_match: continue
                link = link_match.group(1)
                if link == "#" or "javascript" in link or "minhas-recompensas" in link: continue
                if link.startswith("/"): link = "https://clube.uol.com.br" + link
                
                if link in seen_links: continue
                seen_links.add(link)
                
                # 2. TÍTULO
                title = ""
                title_match = re.search(r'class="[^"]*titulo[^"]*"[^>]*>([\s\S]*?)</', block, re.IGNORECASE)
                if title_match:
                    title = clean_text(re.sub(r'<[^>]+>', '', title_match.group(1)))
                else:
                    btn_match = re.search(r'<a[^>]*class="[^"]*btn[^"]*"[^>]*>([\s\S]*?)</', block, re.IGNORECASE)
                    if btn_match: title = clean_text(re.sub(r'<[^>]+>', '', btn_match.group(1)))
                
                if not title: continue
                
                # 3. IMAGEM (Regex otimizado para aspas simples e duplas)
                img_url = ""
                img_matches = re.finditer(r'<img[^>]*data-src=[\'"]([^\'"]+)[\'"]', block, re.IGNORECASE)
                srcs = [m.group(1) for m in img_matches if "data:image" not in m.group(1) and "/parceiros/" not in m.group(1)]
                
                if srcs:
                    img_url = srcs[0]
                else:
                    src_matches = re.finditer(r'<img[^>]*src=[\'"]([^\'"]+)[\'"]', block, re.IGNORECASE)
                    srcs = [m.group(1) for m in src_matches if "data:image" not in m.group(1) and "/parceiros/" not in m.group(1)]
                    if srcs: img_url = srcs[0]
                
                if img_url and img_url.startswith("/"):
                    img_url = "https://clube.uol.com.br" + img_url
                
                offers.append({
                    "id": get_offer_id(link),
                    "preview_title": title,
                    "link": link,
                    "img_url": img_url
                })
                log(f"  🎫 Encontrado: {title[:40]}...")
                
            except Exception as e:
                continue
                
    except Exception as e:
        log(f"❌ Erro: {e}")
        
    return offers[:MAX_OFFERS_PER_RUN]

def process_offer_details(offer):
    log(f"   🔍 Acessando detalhes: {offer['preview_title'][:40]}...")
    try:
        res = requests.get(offer['link'], headers=HEADERS, timeout=15, verify=False)
        html = res.text
        
        # TÍTULO
        page_title = offer['preview_title']
        h2_match = re.search(r'<h2[^>]*>([\s\S]*?)</h2>', html, re.IGNORECASE)
        if h2_match:
            page_title = clean_text(re.sub(r'<[^>]+>', '', h2_match.group(1)))
            
        # IMAGEM DETALHE (Plano B igual ao Scriptable)
        detail_img_url = ""
        img_detail_match = re.search(r'<img[^>]*class="[^"]*responsive[^"]*"[^>]*src=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE)
        if img_detail_match:
            detail_img_url = img_detail_match.group(1)
        else:
            data_src_match = re.search(r'<img[^>]*class="[^"]*responsive[^"]*"[^>]*data-src=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE)
            if data_src_match:
                detail_img_url = data_src_match.group(1)
        
        if detail_img_url and detail_img_url.startswith("/"):
            detail_img_url = "https://clube.uol.com.br" + detail_img_url

        # VALIDADE
        validity = None
        for pattern in [r"[Bb]enefício válido de[^.!?\n]*[.!?]?", r"[Vv]álido até[^.!?\n]*[.!?]?", r"\d{2}/\d{2}/\d{4}.*?\d{2}/\d{2}/\d{4}"]:
            match = re.search(pattern, html)
            if match:
                validity = clean_text(re.sub(r'<[^>]+>', '', match.group(0)))
                break
                
        # DESCRIÇÃO
        full_desc = "Descrição detalhada não disponível."
        info_match = re.search(r'class="[^"]*info-beneficio[^"]*"[^>]*>([\s\S]*?)(?:<script|<footer|class="[^"]*box-compartilhar)', html, re.IGNORECASE)
        
        if info_match:
            raw = info_match.group(1)
            raw = re.sub(r'<br\s*/?>', '\n', raw, flags=re.IGNORECASE)
            raw = re.sub(r'</p>', '\n\n', raw, flags=re.IGNORECASE)
            raw = re.sub(r'</div>', '\n', raw, flags=re.IGNORECASE)
            raw = re.sub(r'<li[^>]*>', '\n• ', raw, flags=re.IGNORECASE)
            
            raw = re.sub(r'<[^>]+>', ' ', raw)
            raw = clean_text(raw)
            raw = re.sub(r'•\s+', '• ', raw)
            
            lixo_idx = raw.find("Enviar cupons por e-mail")
            if lixo_idx != -1:
                raw = raw[:lixo_idx].strip()
                
            if len(raw) > 20:
                full_desc = raw
                
        return page_title, validity, full_desc, detail_img_url
    except Exception as e:
        log(f"   ⚠️ Erro nos detalhes: {e}")
        return offer['preview_title'], None, "Descrição não disponível", ""

# ==============================================
# FUNÇÕES DE TELEGRAM
# ==============================================
def download_image(img_url: str) -> str:
    try:
        res = requests.get(img_url, headers=HEADERS, timeout=10, verify=False)
        if res.ok:
            path = f"/tmp/leouol_{int(time.time())}.jpg"
            Path(path).write_bytes(res.content)
            return path
    except: pass
    return None

def build_caption(title, validity, link):
    parts = [f"<b>{escape_html(title)}</b>"]
    if validity: parts.append(f"📅 {escape_html(validity)}")
    parts.append(f"🔗 <a href='{escape_html(link)}'>Acessar oferta</a>")
    parts.append(f"💬 Veja os detalhes completos nos comentários abaixo")
    return truncate_text("\n\n".join(parts), MAX_CAPTION_LENGTH)

def send_photo_to_channel(img_path, caption):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        with open(img_path, 'rb') as photo:
            res = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': caption, 'parse_mode': 'HTML'}, files={'photo': photo}, timeout=30)
        return res.json().get("result", {}).get("message_id") if res.ok else None
    except: return None

def send_description_comment(desc, link, channel_msg_id):
    group_msg_id = None
    for _ in range(3):
        time.sleep(3)
        try:
            updates = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", timeout=10).json()
            for update in reversed(updates.get("result", [])):
                msg = update.get("message", {})
                if str(msg.get("chat", {}).get("id")) == str(GRUPO_COMENTARIOS_ID):
                    origin_id = msg.get("forward_origin", {}).get("message_id") or msg.get("forward_from_message_id")
                    if origin_id == channel_msg_id:
                        group_msg_id = msg.get("message_id")
                        break
        except: pass
        if group_msg_id: break

    text = f"📋 <b>DESCRIÇÃO COMPLETA</b>\n\n{desc}\n\n🔗 <a href='{escape_html(link)}'>Link original</a>"
    data = {"chat_id": GRUPO_COMENTARIOS_ID, "text": truncate_text(text, MAX_COMMENT_LENGTH), "parse_mode": "HTML", "disable_web_page_preview": True}
    if group_msg_id: data["reply_to_message_id"] = group_msg_id
    
    try:
        return requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data=data, timeout=30).ok
    except: return False

# ==============================================
# FLUXOS PRINCIPAIS
# ==============================================
def run_scraper():
    log("=" * 70)
    log("🤖 BOT LEOUOL - Otimizado (Requests Puro + Regex + Fallback Imagem)")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 70)
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    
    history = load_history()
    seen_ids = set(history.get("ids", []))
    
    offers = fetch_offers()
    new_offers = [o for o in offers if o["id"] not in seen_ids]
    
    if not new_offers:
        log("📭 Nenhuma oferta nova.")
        return
        
    log(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")
    processed_ids, success_count = set(seen_ids), 0
    
    for idx, offer in enumerate(new_offers, 1):
        log(f"\n{'=' * 50}\n📦 Oferta {idx}/{len(new_offers)}: {offer['preview_title']}")
        
        # 1. Abre os detalhes da oferta primeiro!
        page_title, validity, full_desc, detail_img_url = process_offer_details(offer)
        
        # 2. Tenta a imagem dos detalhes, se falhar usa a da página inicial
        final_img_url = detail_img_url or offer.get("img_url")
        
        if not final_img_url:
            log("⚠️ Sem imagem na página inicial nem nos detalhes, pulando...")
            processed_ids.add(offer["id"])
            continue
            
        img_path = download_image(final_img_url)
        if not img_path: 
            log("⚠️ Falha ao baixar a imagem final, pulando...")
            processed_ids.add(offer["id"])
            continue
        
        caption = build_caption(page_title, validity, offer["link"])
        message_id = send_photo_to_channel(img_path, caption)
        
        if message_id and send_description_comment(full_desc, offer["link"], message_id):
            success_count += 1
            log(f"✅ Oferta enviada com sucesso!")
        else:
            log(f"❌ Falha ao enviar")
            
        processed_ids.add(offer["id"])
        try: Path(img_path).unlink(missing_ok=True)
        except: pass
        
    history["ids"] = list(processed_ids)
    save_history(history)
    log(f"\n✅ Fim. {success_count}/{len(new_offers)} enviadas.")

def run_consumer():
    log("=" * 70)
    log("🤖 BOT LEOUOL - Consumer (Processando pendentes do Scriptable)")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 70)
    
    history = load_history()
    seen_ids = set(history.get("ids", []))
    pending_file = Path("pending_offers.json")
    
    if not pending_file.exists(): return
    with open(pending_file, 'r') as f: data = json.load(f)
    offers = data.get("offers", [])
    if not offers: return
    
    log(f"🎉 {len(offers)} ofertas pendentes encontradas!")
    processed_ids, success_count, failed_ids = set(seen_ids), 0, []
    
    for idx, offer in enumerate(offers, 1):
        if offer["id"] in seen_ids or not offer.get("img_url"):
            processed_ids.add(offer["id"])
            continue
            
        img_path = download_image(offer["img_url"])
        if not img_path: continue
        
        page_title = offer.get("title", offer.get("preview_title", "Oferta"))
        caption = build_caption(page_title, offer.get("validity"), offer["link"])
        message_id = send_photo_to_channel(img_path, caption)
        
        if message_id and send_description_comment(offer.get("description", ""), offer["link"], message_id):
            success_count += 1
            processed_ids.add(offer["id"])
        else:
            failed_ids.append(offer["id"])
            
        try: Path(img_path).unlink(missing_ok=True)
        except: pass
        time.sleep(2)
        
    history["ids"] = list(processed_ids)
    save_history(history)
    
    remaining_offers = [o for o in offers if o["id"] in failed_ids]
    with open(pending_file, 'w') as f:
        json.dump({"last_update": datetime.now().isoformat(), "offers": remaining_offers}, f, indent=2)
    log(f"\n✅ Consumer Finalizado: {success_count}/{len(offers)} enviadas.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--pending":
        run_consumer()
    else:
        run_scraper()
}

# ==============================================
# FUNÇÕES UTILITÁRIAS
# ==============================================
def log(msg: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def clean_text(text: str) -> str:
    if not text: return ""
    cleaned = re.sub(r'[ \t]+', ' ', text)
    cleaned = re.sub(r'\n\s*\n+', '\n\n', cleaned)
    cleaned = re.sub(r'^ +| +$', '', cleaned, flags=re.MULTILINE)
    return cleaned.strip()

def escape_html(text: str) -> str:
    if not text: return ""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&#39;")

def get_offer_id(link: str) -> str:
    try:
        link_clean = link.split('?')[0]
        return link_clean.rstrip('/').split('/')[-1]
    except:
        return link

def load_history():
    path = Path(HISTORY_FILE)
    if not path.exists(): return {"ids": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        ids = data.get("ids", [])
        return {"ids": list(dict.fromkeys([get_offer_id(str(x)) for x in ids]))[-MAX_HISTORY_SIZE:]}
    except Exception:
        return {"ids": []}

def save_history(history):
    try:
        ids = list(dict.fromkeys([get_offer_id(str(x)) for x in history.get("ids", [])]))[-MAX_HISTORY_SIZE:]
        Path(HISTORY_FILE).write_text(json.dumps({"ids": ids}, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"✅ Histórico salvo: {len(ids)} IDs")
        return True
    except:
        return False

def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    if len(text) <= max_len: return text
    return text[:max_len - len(suffix)] + suffix

# ==============================================
# SCRAPER - EXATAMENTE IGUAL AO SCRIPTABLE
# ==============================================
def fetch_offers():
    offers = []
    seen_links = set()
    
    log(f"\n🌐 Buscando ofertas em: {TARGET_URL}")
    
    try:
        res = requests.get(TARGET_URL, headers=HEADERS, timeout=15, verify=False)
        html = res.text
        log(f"✅ Página baixada: {len(html)} caracteres")
        
        blocks = re.split(r'<div[^>]*data-categoria=[\'"]', html, flags=re.IGNORECASE)
        log(f"📦 Blocos encontrados: {len(blocks) - 1}")
        
        for block in blocks[1:]:  # Pula o primeiro (cabeçalho)
            try:
                # 1. LINK
                link_match = re.search(r'<a[^>]*href="([^"]+)"', block, re.IGNORECASE)
                if not link_match: continue
                link = link_match.group(1)
                if link == "#" or "javascript" in link or "minhas-recompensas" in link: continue
                if link.startswith("/"): link = "https://clube.uol.com.br" + link
                
                if link in seen_links: continue
                seen_links.add(link)
                
                # 2. TÍTULO
                title = ""
                title_match = re.search(r'class="[^"]*titulo[^"]*"[^>]*>([\s\S]*?)</', block, re.IGNORECASE)
                if title_match:
                    title = clean_text(re.sub(r'<[^>]+>', '', title_match.group(1)))
                else:
                    btn_match = re.search(r'<a[^>]*class="[^"]*btn[^"]*"[^>]*>([\s\S]*?)</', block, re.IGNORECASE)
                    if btn_match: title = clean_text(re.sub(r'<[^>]+>', '', btn_match.group(1)))
                
                if not title: continue
                
                # 3. IMAGEM (Regex otimizado para aspas simples e duplas)
                img_url = ""
                img_matches = re.finditer(r'<img[^>]*data-src=[\'"]([^\'"]+)[\'"]', block, re.IGNORECASE)
                srcs = [m.group(1) for m in img_matches if "data:image" not in m.group(1) and "/parceiros/" not in m.group(1)]
                
                if srcs:
                    img_url = srcs[0]
                else:
                    src_matches = re.finditer(r'<img[^>]*src=[\'"]([^\'"]+)[\'"]', block, re.IGNORECASE)
                    srcs = [m.group(1) for m in src_matches if "data:image" not in m.group(1) and "/parceiros/" not in m.group(1)]
                    if srcs: img_url = srcs[0]
                
                if img_url and img_url.startswith("/"):
                    img_url = "https://clube.uol.com.br" + img_url
                
                offers.append({
                    "id": get_offer_id(link),
                    "preview_title": title,
                    "link": link,
                    "img_url": img_url
                })
                log(f"  🎫 Encontrado: {title[:40]}...")
                
            except Exception as e:
                continue
                
    except Exception as e:
        log(f"❌ Erro: {e}")
        
    return offers[:MAX_OFFERS_PER_RUN]

def process_offer_details(offer):
    log(f"   🔍 Acessando detalhes: {offer['preview_title'][:40]}...")
    try:
        res = requests.get(offer['link'], headers=HEADERS, timeout=15, verify=False)
        html = res.text
        
        # TÍTULO
        page_title = offer['preview_title']
        h2_match = re.search(r'<h2[^>]*>([\s\S]*?)</h2>', html, re.IGNORECASE)
        if h2_match:
            page_title = clean_text(re.sub(r'<[^>]+>', '', h2_match.group(1)))
            
        # IMAGEM DETALHE (Plano B igual ao Scriptable)
        detail_img_url = ""
        img_detail_match = re.search(r'<img[^>]*class="[^"]*responsive[^"]*"[^>]*src=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE)
        if img_detail_match:
            detail_img_url = img_detail_match.group(1)
        else:
            data_src_match = re.search(r'<img[^>]*class="[^"]*responsive[^"]*"[^>]*data-src=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE)
            if data_src_match:
                detail_img_url = data_src_match.group(1)
        
        if detail_img_url and detail_img_url.startswith("/"):
            detail_img_url = "https://clube.uol.com.br" + detail_img_url

        # VALIDADE
        validity = None
        for pattern in [r"[Bb]enefício válido de[^.!?\n]*[.!?]?", r"[Vv]álido até[^.!?\n]*[.!?]?", r"\d{2}/\d{2}/\d{4}.*?\d{2}/\d{2}/\d{4}"]:
            match = re.search(pattern, html)
            if match:
                validity = clean_text(re.sub(r'<[^>]+>', '', match.group(0)))
                break
                
        # DESCRIÇÃO
        full_desc = "Descrição detalhada não disponível."
        info_match = re.search(r'class="[^"]*info-beneficio[^"]*"[^>]*>([\s\S]*?)(?:<script|<footer|class="[^"]*box-compartilhar)', html, re.IGNORECASE)
        
        if info_match:
            raw = info_match.group(1)
            raw = re.sub(r'<br\s*/?>', '\n', raw, flags=re.IGNORECASE)
            raw = re.sub(r'</p>', '\n\n', raw, flags=re.IGNORECASE)
            raw = re.sub(r'</div>', '\n', raw, flags=re.IGNORECASE)
            raw = re.sub(r'<li[^>]*>', '\n• ', raw, flags=re.IGNORECASE)
            
            raw = re.sub(r'<[^>]+>', ' ', raw)
            raw = clean_text(raw)
            raw = re.sub(r'•\s+', '• ', raw)
            
            lixo_idx = raw.find("Enviar cupons por e-mail")
            if lixo_idx != -1:
                raw = raw[:lixo_idx].strip()
                
            if len(raw) > 20:
                full_desc = raw
                
        return page_title, validity, full_desc, detail_img_url
    except Exception as e:
        log(f"   ⚠️ Erro nos detalhes: {e}")
        return offer['preview_title'], None, "Descrição não disponível", ""

# ==============================================
# FUNÇÕES DE TELEGRAM
# ==============================================
def download_image(img_url: str) -> str:
    try:
        res = requests.get(img_url, headers=HEADERS, timeout=10, verify=False)
        if res.ok:
            path = f"/tmp/leouol_{int(time.time())}.jpg"
            Path(path).write_bytes(res.content)
            return path
    except: pass
    return None

def build_caption(title, validity, link):
    parts = [f"<b>{escape_html(title)}</b>"]
    if validity: parts.append(f"📅 {escape_html(validity)}")
    parts.append(f"🔗 <a href='{escape_html(link)}'>Acessar oferta</a>")
    parts.append(f"💬 Veja os detalhes completos nos comentários abaixo")
    return truncate_text("\n\n".join(parts), MAX_CAPTION_LENGTH)

def send_photo_to_channel(img_path, caption):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        with open(img_path, 'rb') as photo:
            res = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': caption, 'parse_mode': 'HTML'}, files={'photo': photo}, timeout=30)
        return res.json().get("result", {}).get("message_id") if res.ok else None
    except: return None

def send_description_comment(desc, link, channel_msg_id):
    group_msg_id = None
    for _ in range(3):
        time.sleep(3)
        try:
            updates = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", timeout=10).json()
            for update in reversed(updates.get("result", [])):
                msg = update.get("message", {})
                if str(msg.get("chat", {}).get("id")) == str(GRUPO_COMENTARIOS_ID):
                    origin_id = msg.get("forward_origin", {}).get("message_id") or msg.get("forward_from_message_id")
                    if origin_id == channel_msg_id:
                        group_msg_id = msg.get("message_id")
                        break
        except: pass
        if group_msg_id: break

    text = f"📋 <b>DESCRIÇÃO COMPLETA</b>\n\n{desc}\n\n🔗 <a href='{escape_html(link)}'>Link original</a>"
    data = {"chat_id": GRUPO_COMENTARIOS_ID, "text": truncate_text(text, MAX_COMMENT_LENGTH), "parse_mode": "HTML", "disable_web_page_preview": True}
    if group_msg_id: data["reply_to_message_id"] = group_msg_id
    
    try:
        return requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data=data, timeout=30).ok
    except: return False

# ==============================================
# FLUXOS PRINCIPAIS
# ==============================================
def run_scraper():
    log("=" * 70)
    log("🤖 BOT LEOUOL - Otimizado (Requests Puro + Regex + Fallback Imagem)")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 70)
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    
    history = load_history()
    seen_ids = set(history.get("ids", []))
    
    offers = fetch_offers()
    new_offers = [o for o in offers if o["id"] not in seen_ids]
    
    if not new_offers:
        log("📭 Nenhuma oferta nova.")
        return
        
    log(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")
    processed_ids, success_count = set(seen_ids), 0
    
    for idx, offer in enumerate(new_offers, 1):
        log(f"\n{'=' * 50}\n📦 Oferta {idx}/{len(new_offers)}: {offer['preview_title']}")
        
        # 1. Abre os detalhes da oferta primeiro!
        page_title, validity, full_desc, detail_img_url = process_offer_details(offer)
        
        # 2. Tenta a imagem dos detalhes, se falhar usa a da página inicial
        final_img_url = detail_img_url or offer.get("img_url")
        
        if not final_img_url:
            log("⚠️ Sem imagem na página inicial nem nos detalhes, pulando...")
            processed_ids.add(offer["id"])
            continue
            
        img_path = download_image(final_img_url)
        if not img_path: 
            log("⚠️ Falha ao baixar a imagem final, pulando...")
            processed_ids.add(offer["id"])
            continue
        
        caption = build_caption(page_title, validity, offer["link"])
        message_id = send_photo_to_channel(img_path, caption)
        
        if message_id and send_description_comment(full_desc, offer["link"], message_id):
            success_count += 1
            log(f"✅ Oferta enviada com sucesso!")
        else:
            log(f"❌ Falha ao enviar")
            
        processed_ids.add(offer["id"])
        try: Path(img_path).unlink(missing_ok=True)
        except: pass
        
    history["ids"] = list(processed_ids)
    save_history(history)
    log(f"\n✅ Fim. {success_count}/{len(new_offers)} enviadas.")

def run_consumer():
    log("=" * 70)
    log("🤖 BOT LEOUOL - Consumer (Processando pendentes do Scriptable)")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 70)
    
    history = load_history()
    seen_ids = set(history.get("ids", []))
    pending_file = Path("pending_offers.json")
    
    if not pending_file.exists(): return
    with open(pending_file, 'r') as f: data = json.load(f)
    offers = data.get("offers", [])
    if not offers: return
    
    log(f"🎉 {len(offers)} ofertas pendentes encontradas!")
    processed_ids, success_count, failed_ids = set(seen_ids), 0, []
    
    for idx, offer in enumerate(offers, 1):
        if offer["id"] in seen_ids or not offer.get("img_url"):
            processed_ids.add(offer["id"])
            continue
            
        img_path = download_image(offer["img_url"])
        if not img_path: continue
        
        page_title = offer.get("title", offer.get("preview_title", "Oferta"))
        caption = build_caption(page_title, offer.get("validity"), offer["link"])
        message_id = send_photo_to_channel(img_path, caption)
        
        if message_id and send_description_comment(offer.get("description", ""), offer["link"], message_id):
            success_count += 1
            processed_ids.add(offer["id"])
        else:
            failed_ids.append(offer["id"])
            
        try: Path(img_path).unlink(missing_ok=True)
        except: pass
        time.sleep(2)
        
    history["ids"] = list(processed_ids)
    save_history(history)
    
    remaining_offers = [o for o in offers if o["id"] in failed_ids]
    with open(pending_file, 'w') as f:
        json.dump({"last_update": datetime.now().isoformat(), "offers": remaining_offers}, f, indent=2)
    log(f"\n✅ Consumer Finalizado: {success_count}/{len(offers)} enviadas.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--pending":
        run_consumer()
    else:
        run_scraper()
