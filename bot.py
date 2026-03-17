# ------------------------------
# Clube UOL Bot - Versão Python para GitHub Actions
# ------------------------------

import requests
import json
import os
import time
from datetime import datetime
import re

# CONFIGURAÇÕES (via Secrets do GitHub)
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TARGET_URL = "https://clube.uol.com.br/?order=new"
HISTORY_FILE = "history.json"

def load_history():
    """Carrega histórico de IDs já enviados"""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                return json.load(f)
    except:
        pass
    return {"lastIds": []}

def save_history(history):
    """Salva histórico"""
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f)

def send_to_telegram(offer):
    """Envia mensagem para o Telegram"""
    try:
        message = f"*{offer['title']}*\n\n🔗 [Acessar oferta]({offer['link']})\n"
        
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        response = requests.post(url, json=payload, timeout=30)
        return response.ok
    except Exception as e:
        print(f"Erro ao enviar: {e}")
        return False

def extract_offers_from_html(html):
    """Extrai ofertas do HTML da página"""
    offers = []
    
    # Padrões para encontrar os containers de oferta
    patterns = [
        r'<article.*?>(.*?)</article>',
        r'<div class="beneficio.*?>(.*?)</div>',
        r'<div class="card.*?>(.*?)</div>'
    ]
    
    # Por enquanto, vamos usar uma abordagem mais simples:
    # Acessar a URL e usar uma API headless é complexo no GitHub Actions
    
    print("ATENÇÃO: Para GitHub Actions, precisamos de uma abordagem diferente!")
    print("O Python não consegue executar JavaScript como o WebView do Scriptable.")
    print("\nOPÇÕES:")
    print("1. Usar Selenium (mais pesado, pode consumir muitos minutos)")
    print("2. Encontrar uma API do Clube UOL (se existir)")
    print("3. Manter no celular com automação iOS (recomendado! ✅)")
    
    return []

def fetch_offers():
    """Busca ofertas do site"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(TARGET_URL, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return extract_offers_from_html(response.text)
        else:
            print(f"Erro HTTP: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"Erro na requisição: {e}")
        return []

def run_bot():
    """Função principal"""
    print(f"🤖 Bot UOL iniciado - {datetime.now()}")
    
    # Carrega histórico
    history = load_history()
    seen_ids = set(history.get("lastIds", []))
    print(f"📋 IDs no histórico: {len(seen_ids)}")
    
    # Busca ofertas
    print("🔍 Buscando ofertas...")
    current_offers = fetch_offers()
    
    if not current_offers:
        print("❌ Nenhuma oferta encontrada")
        return
    
    print(f"📊 Total: {len(current_offers)} ofertas")
    
    # Cria IDs e filtra novas
    offers_with_ids = []
    for offer in current_offers:
        offer_id = re.sub(r'[^a-z0-9]', '', offer['title'].lower())[:40]
        offers_with_ids.append({
            "id": offer_id,
            "title": offer['title'],
            "link": offer.get('link', TARGET_URL)
        })
    
    new_offers = [o for o in offers_with_ids if o['id'] not in seen_ids]
    
    if new_offers:
        print(f"🎉 {len(new_offers)} nova(s) oferta(s)!")
        
        for i, offer in enumerate(new_offers, 1):
            print(f"📤 ({i}/{len(new_offers)}) {offer['title'][:50]}...")
            send_to_telegram(offer)
            
            if i < len(new_offers):
                time.sleep(2)
        
        # Atualiza histórico
        all_ids = [o['id'] for o in offers_with_ids]
        save_history({"lastIds": all_ids})
        print("✅ Concluído!")
    else:
        print("📭 Nenhuma oferta nova")
    
    print(f"✅ Bot finalizado - {datetime.now()}")

if __name__ == "__main__":
    run_bot()
