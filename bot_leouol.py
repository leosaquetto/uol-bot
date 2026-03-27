# Adicione esta função no final do arquivo, antes do if __name__

def main_from_pending():
    """Processa ofertas do arquivo pending_offers.json"""
    log("=" * 70)
    log("🤖 BOT LEOUOL - Processando ofertas pendentes")
    log(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    log("=" * 70)
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log("❌ Variáveis TELEGRAM_TOKEN e TELEGRAM_CHAT_ID são obrigatórias")
        return
    
    history = load_history()
    seen_ids = set(history.get("ids", []))
    
    pending_file = Path("pending_offers.json")
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
    
    for idx, offer in enumerate(offers, 1):
        log(f"\n{'=' * 50}")
        log(f"📦 Oferta {idx}/{len(offers)}")
        log(f"🏷️ {offer.get('title', offer.get('preview_title', ''))[:80]}")
        
        if offer["id"] in seen_ids:
            log("  ⏭️ Oferta já enviada anteriormente")
            processed_ids.add(offer["id"])
            continue
        
        if not offer.get("img_url"):
            log("  ⚠️ Sem imagem, ignorando")
            processed_ids.add(offer["id"])
            continue
        
        img_path = download_image(offer["img_url"])
        if not img_path:
            log("  ⚠️ Falha ao baixar imagem")
            processed_ids.add(offer["id"])
            continue
        
        page_title = offer.get("title", offer.get("preview_title", "Oferta"))
        validity = offer.get("validity")
        full_description = offer.get("description", "Descrição não disponível")
        
        caption = build_caption(page_title, validity, offer["link"])
        message_id = send_photo_to_channel(img_path, caption)
        
        if message_id:
            success = send_description_comment(full_description, offer["link"], message_id)
            if success:
                success_count += 1
                processed_ids.add(offer["id"])
                log(f"  ✅ Oferta {idx} enviada!")
            else:
                log(f"  ⚠️ Foto enviada mas comentário falhou")
                processed_ids.add(offer["id"])
        else:
            log(f"  ❌ Falha ao enviar foto")
        
        try:
            Path(img_path).unlink(missing_ok=True)
        except:
            pass
        
        time.sleep(2)
    
    history["ids"] = list(processed_ids)
    save_history(history)
    
    log(f"\n✅ Fim. {success_count}/{len(offers)} ofertas enviadas.")

# Modifique o final:
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--pending":
        main_from_pending()
    else:
        main()
