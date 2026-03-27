# bot_leouol.py - Versão Definitiva (Anti-Block + Histórico Seguro)

import sys
import requests
import json
import os
import time
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

# A mágica do bypass: curl_cffi emula a impressão digital de um navegador real para evitar bloqueios do UOL
from curl_cffi import requests as cffi_requests

# ==============================================
# CONFIGURAÇÕES
# ==============================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GRUPO_COMENTARIOS_ID = os.environ.get("GRUPO_COMENTARIO_ID", "-1003802235343")

TARGET_URL = "https://clube.uol.com.br/?order=new"

HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"
MAX_OFFERS_PER_RUN = 10
MAX_HISTORY_SIZE = 200
MAX_CAPTION_LENGTH = 1024
MAX_COMMENT_LENGTH = 4096

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://clube.uol.com.br/"
}

# ==============================================
# FUNÇÕES UTILITÁRIAS E PADRONIZAÇÃO DE ID
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
    """Extrai estritamente o final do link (slug) para bater exatamente com o Scriptable."""
    try:
        parsed = urlparse(link)
        path_parts = parsed.path.rstrip('/').split('/')
        return path_parts[-1] if path_parts else link
    except:
        return link.split('?')[0].rstrip('/').split('/')[-1]

def load_history():
    path = Path(HISTORY_FILE)
    if not path.exists(): return {"ids": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # Extrai o slug certinho do histórico para montar a lista de verificação
        ids = [get_offer_id(str(x)) for x in data.get("ids", [])]
        return {"ids": list(dict.fromkeys(ids))[-MAX_HISTORY_SIZE:]}
    except Exception:
        return {"ids": []}

def save_history(history):
    try:
        ids = list(dict.fromkeys(history.get("ids", [])))[-MAX_HISTORY_SIZE:]
        Path(HISTORY_FILE).write_text(json.dumps({"ids": ids}, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"✅ Histórico salvo: {len(ids)} IDs")
        return True
    except:
        return False

def truncate_text(text: str, max_len: int, suffix: str = "...") -> str:
    if len(text) <= max_len: return text
    return text[:max_len - len(suffix)] + suffix

# ==============================================
# SCRAPER - USANDO CURL_CFFI PARA BYPASS
# ==============================================
def fetch_offers():
    offers = []
    seen_links = set()
    
    log(f"\n🌐 Buscando ofertas em: {TARGET_URL}")
    
    try:
        # Pulo do gato: impersonate emula perfeitamente o Chrome, burlando o firewall do UOL
        res = cffi_requests.get(TARGET_URL, headers=HEADERS, timeout=15, impersonate="chrome110")
        html = res.text
        log(f"✅ Página baixada: {len(html)} caracteres")
        
        blocks = re.split(r'<div[^>]*data-categoria=[\'"]', html, flags=re.IGNORECASE)
        log(f"📦 Blocos encontrados: {len(blocks) - 1}")
        
        for block in blocks[1:]:
            try:
                link_match = re.search(r'<a[^>]*href="([^"]+)"', block, re.IGNORECASE)
                if not link_match: continue
                link = link_match.group(1)
                if link == "#" or "javascript" in link or "minhas-recompensas" in link: continue
                if link.startswith("/"): link = "https://clube.uol.com.br" + link
                
                if link in seen_links: continue
                seen_links.add(link)
                
                title = ""
                title_match = re.search(r'class="[^"]*titulo[^"]*"[^>]*>([\s\S]*?)</', block, re.IGNORECASE)
                if title_match:
                    title = clean_text(re.sub(r'<[^>]+>', '', title_match.group(1)))
                else:
                    btn_match = re.search(r'<a[^>]*class="[^"]*btn[^"]*"[^>]*>([\s\S]*?)</', block, re.IGNORECASE)
                    if btn_match: title = clean_text(re.sub(r'<[^>]+>', '', btn_match.group(1)))
                
                if not title: continue
                
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
                    "original_link": link,
                    "preview_title": title,
                    "link": link,
                    "img_url": img_url
                })
                log(f"  🎫 Encontrado: {title[:40]}...")
                
            except Exception:
                continue
                
    except Exception as e:
        log(f"❌ Erro ao buscar ofertas: {e}")
        
    return offers[:MAX_OFFERS_PER_RUN]

def process_offer_details(offer):
    log(f"   🔍 Acessando detalhes: {offer['preview_title'][:40]}...")
    try:
        res = cffi_requests.get(offer['link'], headers=HEADERS, timeout=15, impersonate="chrome110")
        html = res.text
        
        page_title = offer['preview_title']
        h2_match = re.search(r'<h2[^>]*>([\s\S]*?)</h2>', html, re.IGNORECASE)
        if h2_match:
            page_title = clean_text(re.sub(r'<[^>]+>', '', h2_match.group(1)))
            
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

        validity = None
        for pattern in [r"[Bb]enefício válido de[^.!?\n]*[.!?]?", r"[Vv]álido até[^.!?\n]*[.!?]?", r"\d{2}/\d{2}/\d{4}.*?\d{2}/\d{2}/\d{4}"]:
            match = re.search(pattern, html)
            if match:
                validity = clean_text(re.sub(r'<[^>]+>', '', match.group(0)))
                break
                
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
        # Usa o requests tradicional pois baixar imagem não costuma dar block de firewall
        res = requests.get(img_url, headers=HEADERS, timeout=10)
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

def send_description_comment(desc: str, link: str, channel_msg_id: int) -> bool:
    log("   💬 Aguardando o Telegram rotear a mensagem para o grupo...")
    group_msg_id = None
    
    # Aumentei as tentativas e o delay para dar tempo do Telegram enviar a postagem para os comentários
    for _ in range(5):
        time.sleep(5)
        try:
            updates = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", timeout=10).json()
            for update in reversed(updates.get("result", [])):
                msg = update.get("message", {})
                if str(msg.get("chat", {}).get("id")) == str(GRUPO_COMENTARIOS_ID):
                    
                    # 1. Padrão Novo do Telegram (forward_origin)
                    origin = msg.get("forward_origin", {})
                    if origin.get("type") == "channel" and origin.get("message_id") == channel_msg_id:
                        group_msg_id = msg.get("message_id")
                        break
                        
                    # 2. Padrão Antigo (forward_from_message_id)
                    if msg.get("forward_from_message_id") == channel_msg_id:
                        group_msg_id = msg.get("message_id")
                        break
        except Exception as e:
            log(f"   ⚠️ Erro ao buscar ID: {e}")
            
        if group_msg_id: break

    comment_text = f"📋 <b>DESCRIÇÃO COMPLETA</b>\n\n{desc}\n\n🔗 <a href='{escape_html(link)}'>Link original</a>"
    data = {
        "chat_id": GRUPO_COMENTARIOS_ID,
        "text": truncate_text(comment_text, MAX_COMMENT_LENGTH),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    
    if group_msg_id:
        data["reply_to_message_id"] = group_msg_id
        log(f"   💬 Enviando comentário como resposta...")
    else:
        log(f"   💬 ID do grupo não encontrado. Enviando comentário solto no grupo.")
    
    try:
        response = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data=data, timeout=35)
        if response.ok:
            return True
        return False
    except Exception:
        return False

# ==============================================
# FLUXOS PRINCIPAIS
# ==============================================
def run_scraper():
    log("=" * 70)
    log("🤖 BOT LEOUOL - Scraper (Anti-Block Ativo)")
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
    processed_ids = set(seen_ids)
    success_count = 0
    failed_links = []
    
    for idx, offer in enumerate(new_offers, 1):
        log(f"\n{'=' * 50}\n📦 Oferta {idx}/{len(new_offers)}: {offer['preview_title']}")
        
        page_title, validity, full_desc, detail_img_url = process_offer_details(offer)
        final_img_url = detail_img_url or offer.get("img_url")
        
        if not final_img_url:
            log("⚠️ Sem imagem, mantendo como pendente")
            failed_links.append(offer["link"])
            continue
            
        img_path = download_image(final_img_url)
        if not img_path: 
            log("⚠️ Falha ao baixar imagem, mantendo como pendente")
            failed_links.append(offer["link"])
            continue
        
        caption = build_caption(page_title, validity, offer["link"])
        message_id = send_photo_to_channel(img_path, caption)
        
        if message_id:
            if send_description_comment(full_desc, offer["link"], message_id):
                success_count += 1
                processed_ids.add(offer["id"])
                log(f"  ✅ Oferta {idx} enviada!")
            else:
                log(f"  ⚠️ Foto enviada, mas comentário falhou")
                failed_links.append(offer["link"])
        else:
            log(f"  ❌ Falha ao enviar foto")
            failed_links.append(offer["link"])
        
        try: Path(img_path).unlink(missing_ok=True)
        except: pass
    
    # Atualiza histórico guardando o original_link
    history["ids"] = [o["original_link"] for o in new_offers if o["id"] in processed_ids] + list(seen_ids)
    save_history(history)
    
    # Atualiza pending_offers.json (Zera se tudo deu certo)
    pending_file = Path(PENDING_FILE)
    if failed_links:
        all_failed = [o for o in new_offers if o["link"] in failed_links]
        with open(pending_file, 'w') as f:
            json.dump({"last_update": datetime.now().isoformat(), "offers": all_failed}, f, indent=2)
        log(f"⚠️ {len(failed_links)} ofertas foram para o pending")
    else:
        if pending_file.exists():
            with open(pending_file, 'w') as f:
                json.dump({"last_update": datetime.now().isoformat(), "offers": []}, f, indent=2)
            log("✅ Arquivo pending_offers.json limpo")
            
    log(f"\n✅ Fim. {success_count}/{len(new_offers)} enviadas.")

def run_consumer():
    log("=" * 70)
    log("🤖 BOT LEOUOL - Consumer (Processando pendentes do Scriptable)")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 70)
    
    history = load_history()
    seen_ids = set(history.get("ids", []))
    pending_file = Path(PENDING_FILE)
    
    if not pending_file.exists():
        log("📭 Nenhuma oferta pendente")
        return
    
    with open(pending_file, 'r') as f:
        data = json.load(f)
    
    offers = data.get("offers", [])
    if not offers:
        log("📭 Nenhuma oferta pendente")
        return
    
    log(f"🎉 {len(offers)} ofertas pendentes encontradas!")
    processed_ids = set(seen_ids)
    success_count = 0
    failed_links = []
    
    for idx, offer in enumerate(offers, 1):
        log(f"\n{'=' * 50}")
        log(f"📦 Oferta {idx}/{len(offers)}")
        log(f"🏷️ {offer.get('title', offer.get('preview_title', ''))[:80]}")
        
        offer_id = get_offer_id(offer["link"])
        if offer_id in seen_ids:
            log("  ⏭️ Oferta já enviada anteriormente (removendo do pending)")
            processed_ids.add(offer_id)
            continue
        
        if not offer.get("img_url"):
            log("  ⚠️ Sem imagem, mantendo no pending")
            failed_links.append(offer["link"])
            continue
        
        img_path = download_image(offer["img_url"])
        if not img_path:
            log("  ⚠️ Falha ao baixar imagem")
            failed_links.append(offer["link"])
            continue
        
        page_title = offer.get("title", offer.get("preview_title", "Oferta"))
        validity = offer.get("validity")
        full_description = offer.get("description", "Descrição não disponível")
        
        caption = build_caption(page_title, validity, offer["link"])
        message_id = send_photo_to_channel(img_path, caption)
        
        if message_id:
            if send_description_comment(full_description, offer["link"], message_id):
                success_count += 1
                processed_ids.add(offer_id)
                log(f"  ✅ Oferta {idx} enviada!")
            else:
                log(f"  ⚠️ Foto enviada mas comentário falhou")
                failed_links.append(offer["link"])
        else:
            log(f"  ❌ Falha ao enviar foto")
            failed_links.append(offer["link"])
        
        try: Path(img_path).unlink(missing_ok=True)
        except: pass
        time.sleep(2)
    
    # Atualiza histórico
    history["ids"] = [o.get("original_link", o["link"]) for o in offers if get_offer_id(o["link"]) in processed_ids] + list(seen_ids)
    save_history(history)
    
    # Se todas foram enviadas, esvazia o pending
    remaining_offers = [o for o in offers if o["link"] in failed_links]
    with open(pending_file, 'w') as f:
        json.dump({"last_update": datetime.now().isoformat(), "offers": remaining_offers}, f, indent=2)
    
    if remaining_offers:
        log(f"⚠️ {len(remaining_offers)} ofertas continuam no pending")
    else:
        log("✅ Arquivo pending_offers.json limpo!")
    
    log(f"\n✅ Consumer Finalizado: {success_count}/{len(offers)} enviadas.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--pending":
        run_consumer()
    else:
        run_scraper()
