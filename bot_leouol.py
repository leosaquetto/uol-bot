# bot_leouol.py
# ------------------------------
# BOT LEOUOL - Versão Híbrida
# Combina simplicidade do código antigo com formatação HTML correta
# ------------------------------

import requests
import json
import os
import time
import re
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ==============================================
# CONFIGURAÇÕES
# ==============================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "historico_leouol.json"

MAX_OFFERS_PER_RUN = 8
MAX_HISTORY_SIZE = 200

# User-Agents realistas para rotação
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

# ==============================================
# FUNÇÕES UTILITÁRIAS
# ==============================================
def log(msg: str) -> None:
    """Log com timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def human_delay(min_s: float = 1.0, max_s: float = 2.5) -> None:
    """Delay humano com variação"""
    time.sleep(random.uniform(min_s, max_s))

def normalize_spaces(text: Optional[str]) -> str:
    """Normaliza espaços em texto"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def escape_html(text: str) -> str:
    """Escapa caracteres especiais HTML"""
    if not text:
        return ""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;"))

def load_history() -> Dict[str, List[str]]:
    """Carrega histórico de ofertas enviadas"""
    path = Path(HISTORY_FILE)
    if not path.exists():
        return {"ids": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        ids = data.get("ids", [])
        if not isinstance(ids, list):
            return {"ids": []}
        return {"ids": [str(x) for x in ids][-MAX_HISTORY_SIZE:]}
    except Exception as e:
        log(f"⚠️ Erro ao carregar histórico: {e}")
        return {"ids": []}

def save_history(history: Dict[str, List[str]]) -> bool:
    """Salva histórico de ofertas enviadas"""
    try:
        ids = [str(x) for x in history.get("ids", [])][-MAX_HISTORY_SIZE:]
        Path(HISTORY_FILE).write_text(
            json.dumps({"ids": ids}, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        log(f"✅ Histórico salvo: {len(ids)} IDs")
        return True
    except Exception as e:
        log(f"⚠️ Erro ao salvar histórico: {e}")
        return False

# ==============================================
# CONFIGURAÇÃO DO DRIVER (ESTILO CÓDIGO ANTIGO)
# ==============================================
def setup_driver():
    """Configura driver com evasão sutil (estilo código antigo)"""
    chrome_options = Options()
    
    # Argumentos essenciais (do código antigo)
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    # Headless (do código antigo - funciona bem)
    chrome_options.add_argument("--headless=new")
    
    # Resolução realista
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Evasão sutil
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--lang=pt-BR")
    
    # User-Agent aleatório
    random_ua = random.choice(USER_AGENTS)
    chrome_options.add_argument(f"--user-agent={random_ua}")
    
    # Ignorar erros de certificado
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors=yes")
    
    log(f"🚀 Iniciando Chrome (UA: {random_ua[:50]}...)")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Remover vestígio de automação
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    except:
        pass
    
    return driver

# ==============================================
# EXTRAÇÃO DE DADOS
# ==============================================
def extract_partner_from_page(driver) -> Optional[str]:
    """Extrai o nome do parceiro da página da oferta"""
    try:
        # Tenta encontrar o nome do parceiro no topo da página
        selectors = [
            "h1[class*='partner']",
            ".partner-name",
            ".beneficio-header h2",
            "h2[class*='partner']",
            "[class*='parceiro'] h2",
            "[class*='parceiro'] h1"
        ]
        
        for selector in selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                return normalize_spaces(elems[0].text)
        
        # Se não achar, tenta extrair do título da página
        title = driver.title
        match = re.search(r'[–—-]\s*([^–—-]+)$', title)
        if match:
            return normalize_spaces(match.group(1))
            
    except Exception as e:
        log(f"  ⚠️ Erro ao extrair parceiro: {e}")
    return None

def extract_offer_details(driver) -> Dict[str, any]:
    """Extrai benefício, regras e validade"""
    details = {
        "beneficio": None,
        "regras": [],
        "validade": None
    }
    
    try:
        # Procura por parágrafos que contenham informações
        paragraphs = driver.find_elements(By.CSS_SELECTOR, "p, .description, .text, [class*='descricao']")
        
        for p in paragraphs:
            text = normalize_spaces(p.text)
            if not text or len(text) < 10:
                continue
                
            # Identifica se é benefício
            if "benefício" in text.lower() or "Benefício" in text:
                details["beneficio"] = text
                
            # Identifica regras
            elif "regra" in text.lower() or "não é válido" in text.lower() or "não se aplica" in text.lower():
                if len(text) > 20:
                    details["regras"].append(text)
                
        # Procura por validade
        validity_selectors = [".validity", "[class*='validade']", ".date", "time"]
        for selector in validity_selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                details["validade"] = normalize_spaces(elems[0].text)
                break
                
    except Exception as e:
        log(f"  ⚠️ Erro ao extrair detalhes: {e}")
    
    return details

def extract_offer_image(container) -> Optional[str]:
    """Extrai URL da imagem da oferta"""
    try:
        # Procura por imagens com data-src (lazy loading)
        imgs = container.find_elements(By.CSS_SELECTOR, "img[data-src]")
        if imgs:
            img_url = imgs[0].get_attribute("data-src")
            if img_url:
                if img_url.startswith("//"):
                    return "https:" + img_url
                return img_url
        
        # Tenta background image
        elements_with_bg = container.find_elements(By.CSS_SELECTOR, "[style*='background']")
        for el in elements_with_bg:
            style = el.get_attribute("style")
            if style:
                match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
                if match:
                    img_url = match.group(1)
                    if img_url.startswith("//"):
                        return "https:" + img_url
                    return img_url
        
        # Último recurso: qualquer imagem
        imgs = container.find_elements(By.CSS_SELECTOR, "img")
        if imgs:
            img_url = imgs[0].get_attribute("src")
            if img_url:
                if img_url.startswith("//"):
                    return "https:" + img_url
                return img_url
                
    except Exception as e:
        log(f"  ⚠️ Erro ao extrair imagem: {e}")
    
    return None

# ==============================================
# BUSCA DE OFERTAS
# ==============================================
def fetch_offers(driver) -> List[Dict[str, str]]:
    """Busca ofertas na página principal"""
    log(f"🌐 Carregando: {TARGET_URL}")
    driver.get(TARGET_URL)
    
    # Aguarda carregamento inicial
    human_delay(3, 5)
    
    # Scroll suave para carregar lazy loading
    driver.execute_script("window.scrollBy(0, 500);")
    human_delay(1, 2)
    driver.execute_script("window.scrollBy(0, 500);")
    human_delay(1, 2)
    
    # Aguarda elementos carregarem
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.beneficio"))
        )
    except Exception:
        log("⚠️ Timeout aguardando containers")
        return []
    
    # Pega containers
    containers = driver.find_elements(By.CSS_SELECTOR, "div.beneficio")
    log(f"📦 Containers encontrados: {len(containers)}")
    
    if not containers:
        return []
    
    offers = []
    seen_ids = set()
    
    for container in containers[:MAX_OFFERS_PER_RUN * 2]:
        try:
            # Título
            title_elem = container.find_element(By.CSS_SELECTOR, ".titulo, h2, h3, p")
            title = normalize_spaces(title_elem.text)
            if not title:
                continue
            
            # Link
            link_elem = container.find_element(By.CSS_SELECTOR, "a")
            link = link_elem.get_attribute("href")
            if not link:
                continue
            
            # ID único da oferta
            offer_id = link.split("/")[-1] if "/" in link else link
            
            if offer_id in seen_ids:
                continue
            seen_ids.add(offer_id)
            
            # Imagem
            img_url = extract_offer_image(container)
            
            offers.append({
                "id": offer_id,
                "title": title,
                "link": link,
                "img_url": img_url
            })
            
            log(f"  📦 {title[:50]}...")
            
        except Exception as e:
            log(f"  ⚠️ Erro ao processar container: {e}")
            continue
    
    return offers[:MAX_OFFERS_PER_RUN]

# ==============================================
# DOWNLOAD DE IMAGEM
# ==============================================
def download_image(img_url: str) -> Optional[str]:
    """Baixa imagem para envio"""
    if not img_url:
        return None
    
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://clube.uol.com.br/',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        }
        
        response = requests.get(img_url, headers=headers, timeout=10)
        if response.status_code == 200 and response.content:
            temp_path = f"/tmp/leouol_{int(time.time())}_{random.randint(1000, 9999)}.jpg"
            Path(temp_path).write_bytes(response.content)
            return temp_path
            
    except Exception as e:
        log(f"  ⚠️ Erro ao baixar imagem: {e}")
    
    return None

# ==============================================
# ENVIO PARA TELEGRAM (FORMATAÇÃO HTML CORRETA)
# ==============================================
def send_to_telegram(offer: Dict, img_path: str, partner: Optional[str], details: Dict) -> bool:
    """Envia oferta com formatação HTML"""
    try:
        # Monta legenda com formatação HTML
        parts = []
        
        # Título em negrito
        parts.append(f"<b>{escape_html(offer['title'])}</b>")
        
        # Parceiro (se existir)
        if partner:
            parts.append(f"🏷️ <b>Parceiro:</b> {escape_html(partner)}")
        
        # Benefício (se existir)
        if details.get("beneficio"):
            beneficio = escape_html(details['beneficio'][:500])
            parts.append(f"🎁 <b>Benefício:</b> {beneficio}")
        
        # Regras (se existirem)
        if details.get("regras"):
            for regra in details["regras"][:3]:
                regra_clean = escape_html(regra[:300])
                parts.append(f"📋 <b>Regras:</b> {regra_clean}")
        
        # Validade (se existir)
        if details.get("validade"):
            parts.append(f"📅 <b>Validade:</b> {escape_html(details['validade'])}")
        
        # Link
        parts.append(f"🔗 <a href='{escape_html(offer['link'])}'>Acessar oferta</a>")
        
        caption = "\n\n".join(parts)
        
        # Limita tamanho (Telegram: 1024 caracteres)
        if len(caption) > 1024:
            caption = caption[:1020] + "..."
        
        log(f"📤 Enviando oferta: {offer['title'][:50]}...")
        
        # Envia foto com legenda
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        
        with open(img_path, 'rb') as photo:
            files = {'photo': photo}
            data = {
                'chat_id': TELEGRAM_CHAT_ID,
                'caption': caption,
                'parse_mode': 'HTML'
            }
            response = requests.post(url, data=data, files=files, timeout=30)
        
        if response.ok:
            log(f"✅ Enviado com sucesso!")
            return True
        else:
            log(f"❌ Erro: {response.text}")
            
            # Tenta enviar só o texto se falhar
            log("🔄 Tentando enviar apenas texto...")
            text_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            text_data = {
                'chat_id': TELEGRAM_CHAT_ID,
                'text': caption,
                'parse_mode': 'HTML',
                'disable_web_page_preview': False
            }
            text_response = requests.post(text_url, data=text_data, timeout=30)
            return text_response.ok
            
    except Exception as e:
        log(f"❌ Erro ao enviar: {e}")
        return False

# ==============================================
# PROCESSAMENTO DE OFERTA
# ==============================================
def process_offer(driver, offer: Dict) -> Tuple[Optional[str], Dict]:
    """Processa página da oferta para extrair detalhes"""
    partner = None
    details = {"beneficio": None, "regras": [], "validade": None}
    
    try:
        log(f"🔍 Acessando página da oferta...")
        driver.get(offer['link'])
        
        # Aguarda carregamento
        human_delay(2, 4)
        
        # Extrai dados
        partner = extract_partner_from_page(driver)
        details = extract_offer_details(driver)
        
        if partner:
            log(f"  🤝 Parceiro: {partner}")
        if details.get("beneficio"):
            log(f"  🎁 Benefício: {details['beneficio'][:50]}...")
            
    except Exception as e:
        log(f"  ⚠️ Erro ao processar página: {e}")
    
    return partner, details

# ==============================================
# FUNÇÃO PRINCIPAL
# ==============================================
def main():
    log("=" * 70)
    log("🤖 BOT LEOUOL - Versão Híbrida")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 70)
    
    # Verifica variáveis de ambiente
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log("❌ Variáveis TELEGRAM_TOKEN e TELEGRAM_CHAT_ID são obrigatórias")
        return
    
    # Carrega histórico
    history = load_history()
    seen_ids = set(history.get("ids", []))
    
    driver = None
    
    try:
        # Inicia driver
        driver = setup_driver()
        
        # Busca ofertas
        offers = fetch_offers(driver)
        
        if not offers:
            log("📭 Nenhuma oferta encontrada")
            return
        
        # Filtra ofertas novas
        new_offers = [o for o in offers if o["id"] not in seen_ids]
        
        if not new_offers:
            log("📭 Nenhuma oferta nova")
            return
        
        log(f"\n🎉 {len(new_offers)} nova(s) oferta(s)!")
        
        # Processa cada oferta
        processed_ids = set(seen_ids)
        success_count = 0
        
        for idx, offer in enumerate(new_offers, 1):
            log(f"\n{'=' * 50}")
            log(f"📦 Oferta {idx}/{len(new_offers)}")
            log(f"🏷️ {offer['title'][:80]}")
            log(f"🔗 {offer['link'][:80]}")
            
            # Verifica imagem
            if not offer.get("img_url"):
                log("⚠️ Sem imagem, marcando como vista")
                processed_ids.add(offer["id"])
                continue
            
            # Baixa imagem
            img_path = download_image(offer["img_url"])
            if not img_path:
                log("⚠️ Falha ao baixar imagem, marcando como vista")
                processed_ids.add(offer["id"])
                continue
            
            # Processa detalhes da oferta
            partner, details = process_offer(driver, offer)
            
            # Envia para Telegram
            success = send_to_telegram(offer, img_path, partner, details)
            
            # Limpa arquivo temporário
            try:
                Path(img_path).unlink(missing_ok=True)
            except:
                pass
            
            if success:
                success_count += 1
                processed_ids.add(offer["id"])
                log(f"✅ Oferta {idx} processada com sucesso!")
            else:
                log(f"❌ Falha ao processar oferta {idx}")
            
            # Pausa entre ofertas
            if idx < len(new_offers):
                human_delay(2, 4)
        
        # Atualiza histórico
        history["ids"] = list(processed_ids)
        save_history(history)
        
        log(f"\n✅ Fim. {success_count}/{len(new_offers)} ofertas enviadas.")
        
    except Exception as e:
        log(f"💥 Erro fatal: {e}")
        
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

if __name__ == "__main__":
    main()
