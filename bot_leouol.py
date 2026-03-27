def fetch_offers():
    offers = []
    seen_links = set()
    
    log(f"\n🌐 Buscando ofertas em: {TARGET_URL}")
    
    try:
        res = requests.get(TARGET_URL, headers=HEADERS, timeout=15, verify=False)
        html = res.text
        log(f"✅ Página baixada: {len(html)} caracteres")
        
        # Fatiar o HTML igual ao Scriptable
        blocks = re.split(r'<div[^>]*data-categoria=[\'"]', html, flags=re.IGNORECASE)
        log(f"📦 Blocos encontrados: {len(blocks)}")
        
        for block in blocks[1:]:  # Pula o primeiro
            try:
                # 1. LINK (igual Scriptable)
                link_match = re.search(r'<a[^>]*href="([^"]+)"', block, re.IGNORECASE)
                if not link_match: continue
                link = link_match.group(1)
                if link == "#" or "javascript" in link or "minhas-recompensas" in link: continue
                if link.startswith("/"): link = "https://clube.uol.com.br" + link
                
                if link in seen_links: continue
                seen_links.add(link)
                
                # 2. TÍTULO (igual Scriptable)
                title = ""
                title_match = re.search(r'class="[^"]*titulo[^"]*"[^>]*>([\s\S]*?)</', block, re.IGNORECASE)
                if title_match:
                    title = clean_text(re.sub(r'<[^>]+>', '', title_match.group(1)))
                else:
                    btn_match = re.search(r'<a[^>]*class="[^"]*btn[^"]*"[^>]*>([\s\S]*?)</', block, re.IGNORECASE)
                    if btn_match: title = clean_text(re.sub(r'<[^>]+>', '', btn_match.group(1)))
                
                if not title: continue
                
                # 3. IMAGEM - MESMA LÓGICA DO SCRIPTABLE!
                img_url = ""
                srcs = []
                
                # Tenta data-src primeiro (lazy loading)
                img_matches = re.finditer(r'<img[^>]*data-src="([^"]+)"', block, re.IGNORECASE)
                for match in img_matches:
                    img = match.group(1)
                    # Filtra logos de parceiros e data:image
                    if "data:image" not in img and "/parceiros/" not in img:
                        srcs.append(img)
                
                # Se não achou data-src, tenta src normal
                if not srcs:
                    src_matches = re.finditer(r'<img[^>]*src="([^"]+)"', block, re.IGNORECASE)
                    for match in src_matches:
                        img = match.group(1)
                        if "data:image" not in img and "/parceiros/" not in img:
                            srcs.append(img)
                
                # Pega a primeira imagem válida
                if srcs:
                    img_url = srcs[0]
                    if img_url.startswith("/"):
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
