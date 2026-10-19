
    const INITIAL = __INITIAL_DATA_JSON__;
    let settings = INITIAL.settings;
    let historyData = INITIAL.historyData || { active: [], history: [] };
    const credentials = INITIAL.credentials;
    const ICONS = INITIAL.icons;
    const NTFY_TOPIC = INITIAL.topic;

    const AVATAR_URLS = {
      leosaquetto: 'https://files.catbox.moe/a44poo.jpg',
      gaoriel: 'https://files.catbox.moe/0hj4g2.jpg'
    };
    function getAvatarUrl(user) {
      if (user === 'gaoriel' || user === 'gabriel') return AVATAR_URLS.gaoriel;
      return AVATAR_URLS.leosaquetto;
    }

    const CATEGORY_MAP = {
      'EXPOSICOES': { id: 'EXPOSICOES', label: 'Expo', icon: ICONS.art },
      'MUSEU': { id: 'MUSEU', label: 'Museu', icon: ICONS.library },
      'MUSICA': { id: 'MUSICA', label: 'Música', icon: ICONS.music },
      'TEATRO': { id: 'TEATRO', label: 'Teatro', icon: ICONS.masksTheater },
      'FUTEBOL': { id: 'FUTEBOL', label: 'Futebol', icon: '⚽' },
      'OUTROS': { id: 'OUTROS', label: 'Outros', icon: ICONS.ticketText }
    };

    let activeFilter = null;
    let activeOffer = null;
    let isGrouped = settings.groupedView !== undefined ? settings.groupedView : true;
    let currentTab = settings.currentTab || (isGrouped ? 'home' : 'tickets');
    let selectedHistoryYear = null;
    let selectedHistoryMonth = null;

    function historyIdentity(item, index, origin, section) {
      const campaign = item && catalogCampaign(item.link || item.url);
      if (campaign) return 'campaign:' + campaign.key;
      return item && item.id ? 'id:' + String(item.id) : 'legacy:' + origin + ':' + section + ':' + index;
    }

    function reconcileHistorySnapshots(nativeSnapshot, localSnapshot) {
      const result = { schemaVersion: 2, active: [], history: [] };
      const trustedCapture = entry => {
        if (!['worker', 'native', 'portal-client'].includes(entry && entry.captureSource)) return 0;
        const value = catalogTime(entry.capturedAt);
        return value && value <= Date.now() + 300000 ? value : 0;
      };
      const records = new Map();
      const observed = item => Math.max(catalogTime(item.lastSeen), catalogTime(item.lastSourceSeenAt),
        catalogTime(item.soldOutAt), catalogTime(item.restockedAt));
      const add = (snapshot, origin, section) => {
        const list = snapshot && Array.isArray(snapshot[section]) ? snapshot[section] : [];
        list.forEach((item, index) => {
          if (!item || typeof item !== 'object') return;
          const key = historyIdentity(item, index, origin, section);
          const previous = records.get(key);
          if (!previous) {
            records.set(key, { item: { ...item }, origin, section });
            return;
          }
          const winner = origin === 'native' && previous.origin !== 'native' ||
            origin === previous.origin && (observed(item) > observed(previous.item) ||
              observed(item) === observed(previous.item) && section === 'history' && previous.section === 'active');
          const preferred = winner ? item : previous.item;
          const secondary = winner ? previous.item : item;
          const merged = { ...secondary, ...preferred };
          // Preserve a known capture across versions, but never promote a
          // legacy firstSeen that may actually be the Story or event date.
          const candidates = [previous.item, item].filter(entry => trustedCapture(entry));
          const worker = candidates.filter(entry => entry.captureSource === 'worker');
          const selected = (worker.length ? worker : candidates).sort((a, b) => trustedCapture(a) - trustedCapture(b))[0];
          merged.legacyFirstSeen = previous.item.legacyFirstSeen || item.legacyFirstSeen ||
            (!selected && (previous.item.firstSeen || item.firstSeen)) || 0;
          merged.capturedAt = merged.firstSeen = selected ? trustedCapture(selected) : 0;
          merged.captureSource = selected ? selected.captureSource : 'legacy';
          records.set(key, { item: merged, origin: winner ? origin : previous.origin,
            section: winner ? section : previous.section });
        });
      };
      // The native snapshot owns availability when a legacy WebView cache
      // disagrees. A local-only record is retained as unverified history.
      for (const section of ['history', 'active']) add(localSnapshot, 'local', section);
      for (const section of ['history', 'active']) add(nativeSnapshot, 'native', section);
      for (const { item, origin, section } of records.values()) {
        const known = trustedCapture(item);
        const normalized = known ? item : { ...item,
          legacyFirstSeen: item.legacyFirstSeen || item.firstSeen || 0,
          capturedAt: 0, firstSeen: 0, captureSource: 'legacy' };
        if (origin === 'local') normalized.cacheUnverified = true;
        else delete normalized.cacheUnverified;
        result[origin === 'local' && section === 'active' ? 'history' : section].push(normalized);
      }
      return result;
    }

    const legacyPrivateStorageSnapshot = new Map();
    function isPublicSettingsPayload(value) {
      return value && value.schemaVersion === 2 && !Array.isArray(value) &&
        Object.keys(value).every(key => ['schemaVersion', 'priorizarWords', 'despriorizarWords', 'groupedView', 'currentTab'].includes(key));
    }
    function hunterLegacyPrivateCacheManifest() {
      if (!INITIAL.nativeAccounts || typeof localStorage?.key !== 'function') return [];
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (typeof key !== 'string' || legacyPrivateStorageSnapshot.has(key)) continue;
        if (key !== 'uol_hub_settings' && key !== 'uol_real_redemptions' && !key.startsWith('uol_vc_')) continue;
        const value = localStorage.getItem(key);
        if (typeof value !== 'string') continue;
        if (key === 'uol_hub_settings') {
          try { if (isPublicSettingsPayload(JSON.parse(value))) continue; } catch (_) {}
        }
        legacyPrivateStorageSnapshot.set(key, value);
      }
      return Array.from(legacyPrivateStorageSnapshot, ([key, value]) => ({ key, length: value.length }));
    }
    function hunterLegacyPrivateCacheChunk(key, offset, limit) {
      const value = legacyPrivateStorageSnapshot.get(key);
      const start = Number(offset);
      const size = Number(limit);
      if (typeof value !== 'string' || !Number.isSafeInteger(start) || start < 0 ||
          start > value.length || !Number.isSafeInteger(size) || size < 1 || size > 32768) return null;
      return { key, offset: start, length: value.length, data: value.slice(start, start + size) };
    }
    function saveSafeLegacyRedemptions(rows) {
      if (!Array.isArray(rows) || !rows.length) return true;
      const fields = ['id', 'title', 'partner', 'thumb', 'url', 'user', 'redeemedAt',
        'description', 'rules', 'category', 'imageUrl', 'offerUrl', 'redemptionDate'];
      let existing = [];
      try { existing = JSON.parse(localStorage.getItem('uol_real_redemptions_v2') || '[]'); } catch (_) {}
      if (!Array.isArray(existing)) existing = [];
      const cleaned = rows.filter(row => row && typeof row === 'object').map(row => {
        const safe = {};
        for (const field of fields) {
          const content = row[field];
          if (typeof content === 'string' && !/^data:/i.test(content)) {
            if (['url', 'thumb', 'imageUrl', 'offerUrl'].includes(field)) {
              try {
                const parsed = new URL(content);
                if (parsed.protocol === 'https:' && !parsed.username && !parsed.password)
                  safe[field] = (parsed.origin + parsed.pathname).slice(0, 2048);
              } catch (_) {}
            } else safe[field] = content.slice(0, 20000);
          }
          else if (field === 'id' && Number.isSafeInteger(content)) safe[field] = content;
        }
        safe.cacheUnverified = true;
        return safe;
      });
      const merged = new Map();
      [...existing, ...cleaned].forEach(row => {
        const key = row.user && (row.id || row.url) ? String(row.user) + '|' + String(row.id || row.url)
          : JSON.stringify(row);
        merged.set(key, { ...(merged.get(key) || {}), ...row });
      });
      const serialized = JSON.stringify(Array.from(merged.values()));
      if (serialized.length > 2_000_000) return false;
      localStorage.setItem('uol_real_redemptions_v2', serialized);
      return true;
    }
    function hunterLegacyPrivateCacheCommitted(keys) {
      if (!Array.isArray(keys)) return;
      for (const key of keys) {
        const value = legacyPrivateStorageSnapshot.get(key);
        if (typeof value !== 'string') continue;
        const current = localStorage.getItem(key);
        if (current !== value && key !== 'uol_hub_settings') continue;
        try {
          if (key === 'uol_hub_settings') {
            if (current !== value && !isPublicSettingsPayload(JSON.parse(current))) continue;
            let old = {};
            try { old = JSON.parse(value) || {}; } catch (_) {}
            if (!saveSafeLegacyRedemptions([...(Array.isArray(old.realRedemptions) ? old.realRedemptions : []),
              ...(Array.isArray(old.myRedemptions) ? old.myRedemptions : [])])) continue;
            legacyPrivateStorageSnapshot.delete(key);
            if (current === value && !saveSettingsToStorage()) continue;
          } else if (key === 'uol_real_redemptions') {
            let rows = JSON.parse(value);
            if (!Array.isArray(rows)) rows = [];
            if (!saveSafeLegacyRedemptions(rows)) continue;
            localStorage.removeItem(key);
          } else if (key.startsWith('uol_vc_')) {
            localStorage.removeItem(key);
          }
          legacyPrivateStorageSnapshot.delete(key);
        } catch (_) { /* Keep the original cache until native archival can be retried. */ }
      }
    }
    if (typeof window !== 'undefined') {
      window.hunterLegacyPrivateCacheManifest = hunterLegacyPrivateCacheManifest;
      window.hunterLegacyPrivateCacheChunk = hunterLegacyPrivateCacheChunk;
      window.hunterLegacyPrivateCacheCommitted = hunterLegacyPrivateCacheCommitted;
    }

    // Inicialização da interface
    window.addEventListener('load', () => {
      installHeaderLayout();
      // Local storage contains display preferences and public offer history only.
      // Legacy copies may contain cookies/passwords; never hydrate those fields.
      let localHistorySnapshot = { active: [], history: [] };
      try {
        const localSet = localStorage.getItem('uol_hub_settings');
        if (localSet) {
          const saved = JSON.parse(localSet);
          if (saved && typeof saved === 'object') {
            for (const key of ['priorizarWords', 'despriorizarWords', 'groupedView', 'currentTab']) {
              if (Object.prototype.hasOwnProperty.call(saved, key)) settings[key] = saved[key];
            }
            if (!INITIAL.nativeAccounts && ['leosaquetto', 'gaoriel'].includes(saved.currentUser)) settings.currentUser = saved.currentUser;
          }
          if (!INITIAL.nativeAccounts || isPublicSettingsPayload(saved)) saveSettingsToStorage();
        }
        const localHist = localStorage.getItem('uol_offers_history');
        if (localHist) {
          const parsed = JSON.parse(localHist);
          if (parsed && Array.isArray(parsed.active) && Array.isArray(parsed.history)) {
            localHistorySnapshot = parsed;
          }
        }
      } catch(e) {}
      historyData = reconcileHistorySnapshots(historyData, localHistorySnapshot);

      // Native device preference wins over a stale WebView cache after updates.
      initializeNavigationTheme();

      // Reclassifica categorias de ofertas em cache com as regras vigentes (ex: musicais -> MUSICA)
      if (historyData.active && Array.isArray(historyData.active)) {
        historyData.active.forEach(o => {
          o.category = classifyCategory(o.title, o.link || o.id, o.description);
        });
      }
      if (historyData.history && Array.isArray(historyData.history)) {
        historyData.history.forEach(o => {
          o.category = classifyCategory(o.title, o.link || o.id, o.description);
        });
      }

      // Se temos resgates reais pré-carregados na inicialização nativa
      if (INITIAL.realRedemptions && Array.isArray(INITIAL.realRedemptions) && INITIAL.realRedemptions.length > 0) {
        settings.realRedemptions = getAllRedemptionsList();
      }

      updateUserAvatar();
      updateCredsStatusUI();
      updateTimelineBadgeDot();
      setHunterCollectionStatus(INITIAL.hunterCollection);

      // Se temos ofertas ativas em cache no histórico, atualiza badge
      if (historyData.active && historyData.active.length > 0) {
        const visible = homeOfferSets(historyData.active, historyData.history);
        updateCategoryCounts(visible.activeItems);
        document.getElementById('activesCountText').textContent = visible.activeItems.length + ' Ativas';
        setCatalogHeaderStatus('Atualizando…', 'loading', visible.activeItems.length + visible.soldOutItems.length);
      }

      // Inicializa na aba ativa
      // Capture legacy private values before switchTab can persist public
      // preferences; the native archive reads this immutable session copy.
      const legacyPrivateCachePending = INITIAL.nativeAccounts && hunterLegacyPrivateCacheManifest().length > 0;
      switchTab(currentTab, true);
      if (legacyPrivateCachePending) setTimeout(() => { window.location.href = 'hunter-legacy://migrate'; }, 0);
      setTimeout(hunterRequestLinkChecks, 100);

      // Dispara streaming ao vivo e sincronização de resgates oficiais
      streamLiveOffers();
      if (!INITIAL.collectionPausedForUpdate) fetchRealRedemptions();

      // Listener de scroll para fade da camada clara da header
      window.addEventListener('scroll', handleHeaderScroll, { passive: true });
      handleHeaderScroll();

      // The first screen uses the system font. Fetch Manrope after boot so an
      // unavailable font service cannot hold the WebView's initial load.
      setTimeout(() => {
        const stylesheet = document.createElement('link');
        stylesheet.rel = 'stylesheet';
        stylesheet.href = 'https://fonts.googleapis.com/css2?family=Manrope:wght@300;400;500;600;700;800&display=swap';
        document.head.appendChild(stylesheet);
      }, 0);
    });

    // HEADER_LAYOUT_START
    function measureHeaderHeight(header = document.querySelector('.top-header')) {
      const style = document.documentElement && document.documentElement.style;
      if (!header || !style || typeof style.setProperty !== 'function') return;
      const height = Math.ceil(typeof header.getBoundingClientRect === 'function'
        ? header.getBoundingClientRect().height : header.offsetHeight);
      if (Number.isFinite(height) && height > 0) style.setProperty('--hunter-header-height', height + 'px');
    }

    function installHeaderLayout() {
      const header = document.querySelector('.top-header');
      measureHeaderHeight(header);
      if (header && typeof ResizeObserver === 'function') {
        new ResizeObserver(() => measureHeaderHeight(header)).observe(header);
      } else if (header && typeof MutationObserver === 'function') {
        // Older WebViews still reserve the new height when either status wraps.
        new MutationObserver(() => measureHeaderHeight(header)).observe(header, {
          childList: true, characterData: true, subtree: true, attributes: true, attributeFilter: ['hidden', 'open']
        });
      }
      window.addEventListener('resize', () => measureHeaderHeight(header), { passive: true });
      window.addEventListener('orientationchange', () => measureHeaderHeight(header), { passive: true });
      if (document.fonts && document.fonts.ready) document.fonts.ready.then(() => measureHeaderHeight(header)).catch(() => {});
    }

    function handleHeaderScroll() {
      const header = document.querySelector('.top-header');
      if (!header) return;
      if (window.scrollY > 8) {
        header.classList.add('header-scrolled');
      } else {
        header.classList.remove('header-scrolled');
      }
    }
    // HEADER_LAYOUT_END

    function saveSettingsToStorage() {
      if (INITIAL.nativeAccounts && legacyPrivateStorageSnapshot.has('uol_hub_settings')) return false;
      try {
        const publicSettings = {
          schemaVersion: 2,
          priorizarWords: Array.isArray(settings.priorizarWords) ? settings.priorizarWords : [],
          despriorizarWords: Array.isArray(settings.despriorizarWords) ? settings.despriorizarWords : [],
          groupedView: Boolean(settings.groupedView),
          currentTab: settings.currentTab || currentTab || 'home'
        };
        if (!INITIAL.nativeAccounts && ['leosaquetto', 'gaoriel'].includes(settings.currentUser)) publicSettings.currentUser = settings.currentUser;
        const serialized = JSON.stringify(publicSettings);
        if (localStorage.getItem('uol_hub_settings') !== serialized) {
          try { localStorage.setItem('uol_hub_settings', serialized); }
          catch (_) {
            // A legacy payload may itself exhaust the quota. Scrub it, then
            // retry the small preference projection without touching history.
            localStorage.removeItem('uol_hub_settings');
            localStorage.setItem('uol_hub_settings', serialized);
          }
        }
        return true;
      } catch(e) { return false; }
    }

    function saveHistoryToStorage() {
      try {
        const serialized = JSON.stringify({ schemaVersion: 2, active: historyData.active || [], history: historyData.history || [] });
        // The native history remains authoritative; an oversized WebView mirror
        // must not evict other WebKit storage or truncate historical records.
        if (serialized.length > 2_000_000) return false;
        if (localStorage.getItem('uol_offers_history') !== serialized) localStorage.setItem('uol_offers_history', serialized);
        return true;
      } catch(e) { return false; }
    }

    function getCachedDescription(id) {
      if (typeof id !== 'string' || !/^[A-Za-z0-9_-]{1,160}$/.test(id) ||
        ['__proto__', 'constructor', 'prototype'].includes(id)) return '';
      try {
        const raw = localStorage.getItem('uol_offer_descriptions') || '{}';
        if (raw.length > 2_000_000) return '';
        const map = JSON.parse(raw);
        const value = map && !Array.isArray(map) && Object.prototype.hasOwnProperty.call(map, id) ? map[id] : '';
        return typeof value === 'string' ? value.slice(0, 20_000) : '';
      } catch(e) { return ''; }
    }

    function setCachedDescription(id, desc) {
      if (typeof id !== 'string' || !/^[A-Za-z0-9_-]{1,160}$/.test(id) ||
        ['__proto__', 'constructor', 'prototype'].includes(id) || typeof desc !== 'string' || !desc) return;
      try {
        const raw = localStorage.getItem('uol_offer_descriptions') || '{}';
        if (raw.length > 2_000_000) return;
        const map = JSON.parse(raw);
        if (!map || typeof map !== 'object' || Array.isArray(map)) return;
        const value = desc.slice(0, 20_000);
        if (map[id] === value) return;
        map[id] = value;
        const serialized = JSON.stringify(map);
        if (serialized.length <= 2_000_000) localStorage.setItem('uol_offer_descriptions', serialized);
      } catch(e) {}
    }

    // ===================================================================
    // FORMATAÇÃO INTELIGENTE: SENTENCE CASE ("Em sentença")
    // ===================================================================
    function toSentenceCase(str) {
      if (!str) return '';
      str = str.trim();
      let lower = str.toLowerCase();
      let formatted = lower.replace(/(^\s*\w|[.!?:]\s*\w)/g, c => c.toUpperCase());
      formatted = formatted
        .replace(/\buol\b/gi, 'UOL')
        .replace(/\bsp\b/gi, 'SP')
        .replace(/\brj\b/gi, 'RJ')
        .replace(/\bba\b/gi, 'BA')
        .replace(/\bpr\b/gi, 'PR')
        .replace(/\bmg\b/gi, 'MG');
      return formatted;
    }

    // ===================================================================
    // CLASSIFICAÇÃO DE CATEGORIAS
    // ===================================================================
    function classifyCategory(title, url, description) {
      const text = ((title || '') + ' ' + (url || '') + ' ' + (description || '')).toLowerCase();
      const normalize = value => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[-_]+/g, ' ');
      const titleContext = normalize((title || '') + ' ' + (url || ''));
      const footballContext = normalize(text);

      // 1. Exposições (Expo Barra Funda, arte, exposições)
      if (text.includes('expo barra funda') || text.includes('expo-barra-funda') || text.includes('exposi')) {
        return 'EXPOSICOES';
      }

      // 2. Teatros prioritários (Teatro UOL, Total Energies)
      if (
        text.includes('teatro uol') ||
        text.includes('teatro-uol') ||
        text.includes('total energies') ||
        text.includes('total-energies')
      ) {
        return 'TEATRO';
      }

      // Stadium names host several kinds of event. Require football evidence,
      // while keeping an explicit show, theatre or museum title in its category.
      const otherEventTitle = /\b(?:show|shows|musical|musicais|concerto|festival|teatro|theatro|peca|comedia|stand up|museu|pinacoteca)\b/.test(titleContext);
      const theatreDescription = /\b(?:peca|espetaculo)\b/.test(footballContext) && /\b(?:teatro|teatral)\b/.test(footballContext);
      const musicDescription = /\b(?:show|shows|concerto|musical|musicais|festival|orquestra)\b/.test(footballContext) && !/\b(?:partida|jogo|classico)\b/.test(footballContext);
      const otherSportTitle = /\b(?:basquete|volei|voleibol|tenis|rugby)\b/.test(titleContext);
      const footballEvidence = /\b(?:futebol|brasileirao|campeonato brasileiro|copa do brasil|libertadores|copa sul americana)\b/.test(footballContext) ||
        (/\b(?:x|vs|versus|jogo|partida|classico)\b/.test(footballContext) && /\b(?:estadio|arena|corinthians)\b/.test(footballContext));
      if (!otherEventTitle && !theatreDescription && !musicDescription && !otherSportTitle && footballEvidence) return 'FUTEBOL';

      // 3. Música (Theatro Municipal RJ, Vibra São Paulo, Buenos Aires, Shows, Concertos, Musicais)
      if (
        text.includes('theatro municipal') ||
        text.includes('theatro-municipal') ||
        text.includes('vibra sao paulo') ||
        text.includes('vibra-sao-paulo') ||
        text.includes('vibra são paulo') ||
        text.includes('buenos aires') ||
        text.includes('show') ||
        text.includes('musica') ||
        text.includes('música') ||
        text.includes('musical') ||
        text.includes('musicais') ||
        text.includes('cor purpura') ||
        text.includes('cor-purpura') ||
        text.includes('cor púrpura') ||
        text.includes('multiplan') ||
        text.includes('orquestra') ||
        text.includes('concerto') ||
        text.includes('festival') ||
        text.includes('banda') ||
        text.includes('tributo') ||
        text.includes('tango')
      ) {
        return 'MUSICA';
      }

      // 4. Museu
      if (
        text.includes('museu') ||
        text.includes('pinacoteca') ||
        text.includes('arte brasileira') ||
        text.includes('masp') ||
        text.includes('mab ')
      ) {
        return 'MUSEU';
      }

      // 5. Teatro
      if (
        text.includes('teatro') ||
        text.includes('comedia') ||
        text.includes('comédia') ||
        text.includes('stand') ||
        text.includes('drama') ||
        text.includes('peca') ||
        text.includes('peça')
      ) {
        return 'TEATRO';
      }

      return 'OUTROS';
    }

    function checkHighlight(title, url, description) {
      const titleUrl = ((title || '') + ' ' + (url || '')).toLowerCase();

      // Regra de despriorização: se o título/URL contiver palavra despriorizada, não pesquisa nem destaca
      const despriorizar = settings.despriorizarWords || [];
      for (const dw of despriorizar) {
        const clean = dw.trim().toLowerCase();
        if (clean && titleUrl.includes(clean)) {
          return false;
        }
      }

      // Verifica palavras priorizadas no título ou url
      const priorizar = settings.priorizarWords || ['museu'];
      for (const pw of priorizar) {
        const clean = pw.trim().toLowerCase();
        if (clean && titleUrl.includes(clean)) {
          return true;
        }
      }

      // Se não achou no título/url, mas tiver descrição disponível, busca na descrição
      if (description) {
        const descLow = description.toLowerCase();
        for (const pw of priorizar) {
          const clean = pw.trim().toLowerCase();
          if (clean && descLow.includes(clean)) {
            return true;
          }
        }
      }

      return false;
    }

    // ===================================================================
    // TEMPO NO AR / RECÊNCIA
    // ===================================================================
    function formatTimeDuration(fromMs, toMs) {
      const diff = Math.max(0, toMs - fromMs);
      const mins = Math.floor(diff / (1000 * 60));
      const hours = Math.floor(mins / 60);
      const days = Math.floor(hours / 24);

      if (days > 0) {
        const remH = hours % 24;
        return days + 'd ' + (remH > 0 ? remH + 'h' : '');
      }
      if (hours > 0) {
        const remM = mins % 60;
        return hours + 'h ' + (remM > 0 ? remM + 'm' : '');
      }
      return Math.max(1, mins) + ' m';
    }

    function formatRelativeTime(fromMs, now) {
      if (!fromMs) return 'Hoje';
      const diff = Math.max(0, now - fromMs);
      const mins = Math.floor(diff / (1000 * 60));
      const hours = Math.floor(mins / 60);
      const days = Math.floor(hours / 24);

      if (mins < 1) return 'Agora';
      if (mins < 60) return mins + 'm atrás';
      const remMins = mins % 60;
      if (hours < 24) {
        return remMins > 0 ? (hours + 'h ' + remMins + 'm atrás') : (hours + 'h atrás');
      }
      if (days === 1) return '1 dia atrás';
      if (days <= 7) return days + ' dias atrás';
      const d = new Date(fromMs);
      const day = String(d.getDate()).padStart(2, '0');
      const month = String(d.getMonth() + 1).padStart(2, '0');
      const year = String(d.getFullYear()).slice(-2);
      return `${day}/${month}/${year}`;
    }

    function formatStopwatchDuration(start, end) {
      if (!start) return '0s';
      const s = typeof start === 'number' ? start : new Date(start).getTime();
      const e = end ? (typeof end === 'number' ? end : new Date(end).getTime()) : Date.now();
      const diffSec = Math.max(0, Math.floor((e - s) / 1000));
      if (diffSec < 60) return diffSec + 's';
      if (diffSec < 3600) {
        const m = Math.floor(diffSec / 60);
        const sec = diffSec % 60;
        return sec > 0 ? (m + 'm ' + sec + 's') : (m + 'm');
      }
      const h = Math.floor(diffSec / 3600);
      const m = Math.floor((diffSec % 3600) / 60);
      return m > 0 ? (h + 'h ' + m + 'm') : (h + 'h');
    }

    function formatExpiredAgo(fromMs, now) {
      if (!fromMs) return 'Há instantes';
      const diff = Math.max(0, now - fromMs);
      const mins = Math.floor(diff / (1000 * 60));
      const hours = Math.floor(mins / 60);
      const days = Math.floor(hours / 24);

      if (mins <= 1) return 'Há 1m';
      if (mins < 60) return 'Há ' + mins + 'm';
      if (hours === 1) return 'Há 1 hora';
      if (hours < 24) {
        const remMins = mins % 60;
        return remMins > 0 ? ('Há ' + hours + 'h ' + remMins + 'm') : ('Há ' + hours + 'h');
      }
      if (days === 1) return '1 dia atrás';
      if (days <= 7) return days + ' dias atrás';
      const d = new Date(fromMs);
      const day = String(d.getDate()).padStart(2, '0');
      const month = String(d.getMonth() + 1).padStart(2, '0');
      const year = String(d.getFullYear()).slice(-2);
      return `${day}/${month}/${year}`;
    }

    // ===================================================================
    // CATÁLOGO COMPARTILHADO: O WORKER COLETA; O CELULAR REUTILIZA
    // ===================================================================
    // CATALOG_ADAPTER_START
    const CATALOG_URL = 'https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/catalog';
    const AVAILABILITY_EVENTS_URL = CATALOG_URL + '/availability-events';
    const CATALOG_CACHE_KEY = 'uol_shared_catalog_v1';
    const CATALOG_MAX_BYTES = 2 * 1024 * 1024;
    let catalogRefreshStarted = false;
    let catalogRefreshInFlight = false;
    let catalogRefreshSettled = false;
    let catalogStatusFresh = false;
    const linkCheckOverlay = new Map(); // Session-only: the native checker owns durable evidence.
    let linkCheckProgress = null;
    let lastNativeLinkRequestAt = 0;
    const linkChecksNotBefore = Date.now() + 1500;
    let lastNativeLinkSignature = '';
    let lastAvailabilityEventsFetchAt = 0;

    function catalogCampaign(link) {
      try {
        if (typeof link !== 'string' || link.length > 2048 || /[\u0000-\u001f\u007f\\]/.test(link)) return null;
        const url = new URL(link, 'https://clube.uol.com.br');
        if (url.protocol !== 'https:' || url.hostname !== 'clube.uol.com.br' || url.port || url.username || url.password) return null;
        const match = url.pathname.match(/^\/campanhasdeingresso\/([a-z0-9][a-z0-9-]*)\/?$/i);
        if (!match) return null;
        return { key: match[1].toLowerCase(), slug: match[1], link: 'https://clube.uol.com.br/campanhasdeingresso/' + match[1] };
      } catch (e) { return null; }
    }

    function catalogTime(value) {
      const time = typeof value === 'number' ? value : (typeof value === 'string' ? Date.parse(value) : 0);
      return Number.isFinite(time) && time > 0 ? time : 0;
    }

    function captureTime(item) {
      if (!item || !['worker', 'native', 'portal-client'].includes(item.captureSource)) return 0;
      return plausibleCaptureTime(item.capturedAt);
    }

    function plausibleCaptureTime(value) {
      const time = catalogTime(value);
      return time && time <= Date.now() + 300000 ? time : 0;
    }

    function linkCheckGeneration(item) {
      return String(Math.max(captureTime(item), catalogTime(item && item.restockedAt)) || 0);
    }

    function linkUnavailableAt(item) {
      const campaign = catalogCampaign(item && item.link);
      if (!campaign) return 0;
      return Math.max(catalogTime(item && item.linkUnavailableAt),
        catalogTime(linkCheckOverlay.get(campaign.key + '|' + linkCheckGeneration(item))));
    }

    function hunterActiveLinkCandidates(offset = 0, limit = 100) {
      const seen = new Set();
      const candidates = [];
      const active = historyData.active || [];
      const activeKeys = new Set(active.map(item => catalogCampaign(item && item.link)?.key).filter(Boolean));
      const archived = (historyData.history || []).filter(item =>
        (item.cacheUnverified === true || item.linkUnavailableAt) &&
        !activeKeys.has(catalogCampaign(item && item.link)?.key));
      const queue = [...archived.filter(item => item.cacheUnverified === true), ...active,
        ...archived.filter(item => item.cacheUnverified !== true)];
      queue.forEach(item => {
        const campaign = catalogCampaign(item && item.link);
        if (!campaign || !item.cacheUnverified && !item.linkUnavailableAt &&
          (item.isExpired || item.availability === 'sold_out' || confirmedSoldOutTime(item))) return;
        const session = sessionDateInfo(item);
        const today = saoPauloParts(Date.now());
        if (session && session.timestamp < Date.UTC(today.year, today.month - 1, today.day)) return;
        const generation = linkCheckGeneration(item);
        const key = campaign.key + '|' + generation;
        if (seen.has(key)) return;
        seen.add(key);
        candidates.push({ id: String(item.id || campaign.slug), link: campaign.link, generation: generation });
      });
      const start = Math.max(0, Math.floor(Number(offset) || 0));
      const size = Math.min(100, Math.max(1, Math.floor(Number(limit) || 100)));
      return candidates.slice(start, start + size);
    }

    function hunterApplyLinkCheckResults(results, progress) {
      let changed = false;
      let historyChanged = false;
      (Array.isArray(results) ? results : []).forEach(result => {
        const campaign = catalogCampaign(result && result.link);
        if (!campaign || !['available', 'sold_out', 'unavailable', 'unknown'].includes(result.status)) return;
        const matches = item => String(item.id) === String(result.id) &&
          catalogCampaign(item.link)?.key === campaign.key && linkCheckGeneration(item) === String(result.generation);
        const activeCandidate = (historyData.active || []).find(matches);
        const historyCandidate = activeCandidate ? null : (historyData.history || []).find(item =>
          (item.cacheUnverified || item.linkUnavailableAt) && matches(item));
        const candidate = activeCandidate || historyCandidate;
        if (!candidate) return;
        const checkedAt = catalogTime(result.checkedAt);
        if (!checkedAt || checkedAt > Date.now() + 300000) return;
        const newerEvidence = availabilityEvidence(candidate.availabilityEvidence);
        if (newerEvidence && newerEvidence.observedAt > checkedAt) return;
        const key = campaign.key + '|' + result.generation;
        // The native queue replays its last result while a new probe is pending.
        // An old positive must not reactivate an uncertain local-only record.
        if (result.cached === true && result.status !== 'unavailable') return;
        if (historyCandidate && result.status === 'available') {
          historyCandidate.availabilityEvidence = { state: 'available', observedAt: checkedAt, source: 'iphone' };
          historyCandidate.restockedAt = checkedAt;
          historyCandidate.lastLinkCheckedAt = checkedAt;
          historyCandidate.availability = 'observed';
          historyCandidate.isExpired = false;
          delete historyCandidate.expiredAt;
          delete historyCandidate.soldOutAt;
          delete historyCandidate.linkUnavailableAt;
          delete historyCandidate.cacheUnverified;
          historyData.history = historyData.history.filter(item => item !== historyCandidate);
          historyData.active.push(historyCandidate);
          historyChanged = changed = true;
        } else if (result.status === 'sold_out') {
          candidate.availabilityEvidence = { state: 'sold_out', observedAt: checkedAt, source: 'iphone' };
          candidate.soldOutAt = candidate.expiredAt = checkedAt;
          candidate.lastLinkCheckedAt = checkedAt;
          candidate.availability = 'sold_out';
          candidate.isExpired = true;
          delete candidate.cacheUnverified;
          delete candidate.linkUnavailableAt;
          if (activeCandidate) {
            historyData.active = historyData.active.filter(item => item !== activeCandidate);
            historyData.history.push(activeCandidate);
          }
          historyChanged = changed = true;
        } else if (historyCandidate && result.status === 'unavailable' && result.cached !== true) {
          historyCandidate.linkUnavailableAt = catalogTime(result.unavailableAt) || checkedAt;
          historyCandidate.lastLinkCheckedAt = checkedAt;
          delete historyCandidate.cacheUnverified;
          historyChanged = changed = true;
        }
        if (result.status === 'unavailable') {
          const at = catalogTime(result.unavailableAt) || checkedAt;
          if (at && at <= checkedAt && linkCheckOverlay.get(key) !== at) { linkCheckOverlay.set(key, at); changed = true; }
        } else if (result.status === 'available') {
          if (activeCandidate?.linkUnavailableAt) {
            delete activeCandidate.linkUnavailableAt;
            historyChanged = changed = true;
          }
          if (linkCheckOverlay.delete(key)) changed = true;
        }
        // Unknown evidence cannot reverse a previously confirmed unavailable link.
      });
      if (historyChanged) saveHistoryToStorage();
      if (progress && [progress.verified, progress.total, progress.pending].every(Number.isSafeInteger)) {
        linkCheckProgress = { verified: progress.verified, total: progress.total, pending: progress.pending };
        const status = document.getElementById('liveStatusText');
        if (status && progress.pending > 0) status.setAttribute('aria-label',
          'Verificando links: ' + progress.verified + ' de ' + progress.total);
        updateCatalogStatusTrigger();
      }
      if (changed) renderCatalogStatus();
    }

    function hunterRequestLinkChecks() {
      if (!INITIAL.nativeLinkChecks) return;
      const candidates = [];
      for (let offset = 0; ; offset += 100) {
        const page = hunterActiveLinkCandidates(offset, 100);
        if (!page.length) break;
        candidates.push(...page);
        if (page.length < 100) break;
      }
      const signature = candidates.map(item => item.link + '|' + item.generation).sort().join(';');
      if (!signature || (signature === lastNativeLinkSignature && Date.now() - lastNativeLinkRequestAt < 30000)) return;
      lastNativeLinkSignature = signature;
      lastNativeLinkRequestAt = Date.now();
      const go = () => { window.location.href = 'hunter-links://start'; };
      const delay = Math.max(0, linkChecksNotBefore - Date.now());
      if (typeof requestAnimationFrame === 'function') requestAnimationFrame(() => setTimeout(go, delay));
      else setTimeout(go, delay);
    }

    async function refreshAvailabilityEvents() {
      if (!INITIAL.nativeBrowser || Date.now() - lastAvailabilityEventsFetchAt < 120000) return;
      lastAvailabilityEventsFetchAt = Date.now();
      const known = new Map();
      [...(historyData.active || []), ...(historyData.history || [])].forEach(o => {
        known.set(String(o.id), o);
        if (o.catalogOfferId) known.set(String(o.catalogOfferId), o);
      });
      const events = [];
      let cursor = '';
      try {
        for (let page = 0; page < 5; page++) {
          const controller = typeof AbortController === 'function' ? new AbortController() : null;
          const timer = controller ? setTimeout(() => controller.abort(), 4000) : null;
          let response;
          try {
            response = await fetch(AVAILABILITY_EVENTS_URL + (cursor ? '?cursor=' + encodeURIComponent(cursor) : ''),
              { credentials: 'omit', ...(controller ? { signal: controller.signal } : {}) });
          } finally { if (timer) clearTimeout(timer); }
          if (!response.ok) return;
          const body = await response.text();
          if (body.length > 131072) return;
          const data = JSON.parse(body);
          if (!data || !data.ok || data.schemaVersion !== 1 || !Array.isArray(data.events) || data.events.length > 100) return;
          data.events.forEach(entry => {
            if (!entry || !['sold_out', 'restocked'].includes(entry.kind) ||
                typeof entry.offerId !== 'string' || entry.offerId.length > 160) return;
            const occurredAt = catalogTime(entry.occurredAt);
            if (!occurredAt || occurredAt > Date.now() + 300000) return;
            const offer = known.get(entry.offerId);
            events.push({ offerId: offer ? offer.id : entry.offerId, eventType: entry.kind, occurredAt: occurredAt,
              title: offer ? String(offer.rawTitle || offer.title || '').slice(0, 280) : '',
              imageUrl: offer ? validOfferImage(offer.imageUrl) : '' });
          });
          if (!data.nextCursor || typeof data.nextCursor !== 'string' || data.nextCursor.length > 512) break;
          cursor = data.nextCursor;
        }
        if (typeof window !== 'undefined') {
          window.hunterAvailabilityEvents = events;
          if (typeof CustomEvent === 'function') window.dispatchEvent(new CustomEvent('hunter-availability-events'));
        }
      } catch (e) { /* An unavailable timeline endpoint must not delay the dashboard. */ }
    }
    if (typeof window !== 'undefined') {
      window.hunterAvailabilityEvents = [];
      window.hunterActiveLinkCandidates = hunterActiveLinkCandidates;
      window.hunterApplyLinkCheckResults = hunterApplyLinkCheckResults;
      window.hunterRequestLinkChecks = hunterRequestLinkChecks;
    }

    function boundedCatalogText(value, maxLength) {
      return typeof value === 'string' && value.length <= maxLength;
    }

    function isCatalogSnapshot(data) {
      return Boolean(data && data.ok === true && data.schemaVersion === 1 &&
        Array.isArray(data.offers) && data.offers.length <= 300 &&
        Array.isArray(data.stories) && data.stories.length <= 30 &&
        data.coverage && data.coverage.scope === 'recent_tracked' && typeof data.coverage.truncated === 'boolean' &&
        data.sources && ['api', 'ticketListing'].every(key => data.sources[key] && typeof data.sources[key].healthy === 'boolean') &&
        data.offers.every(o => o && boundedCatalogText(o.id, 160) && boundedCatalogText(o.title, 280) &&
          boundedCatalogText(o.description || '', 8192) && boundedCatalogText(o.partner || '', 120) &&
          boundedCatalogText(o.validity || '', 500) && boundedCatalogText(o.imageUrl || '', 2048) && catalogCampaign(o.link) &&
          ['observed', 'sold_out'].includes(o.availability) && typeof o.isPortal === 'boolean') &&
        data.stories.every(s => s && boundedCatalogText(s.storyId, 100) &&
          boundedCatalogText(s.imageUrl || '', 2048) && catalogCampaign(s.link)));
    }

    function readCatalogCache() {
      try {
        const raw = localStorage.getItem(CATALOG_CACHE_KEY);
        if (!raw || raw.length > CATALOG_MAX_BYTES) return null;
        const cache = JSON.parse(raw);
        if (!cache || !isCatalogSnapshot(cache.snapshot) || !Number.isFinite(cache.fetchedAt) ||
            cache.fetchedAt <= 0 || cache.fetchedAt > Date.now() + 300000) return null;
        const etag = typeof cache.etag === 'string' && cache.etag.length <= 200 && !/[\r\n]/.test(cache.etag) ? cache.etag : '';
        return { snapshot: cache.snapshot, fetchedAt: cache.fetchedAt, etag: etag };
      } catch (e) { return null; }
    }

    function saveCatalogCache(snapshot, etag) {
      // Snapshot, data e validador pertencem à mesma gravação: nunca reutilizar ETag sem seu corpo.
      try {
        const serialized = JSON.stringify({ snapshot: snapshot, etag: etag || '', fetchedAt: Date.now() });
        if (serialized.length <= CATALOG_MAX_BYTES) localStorage.setItem(CATALOG_CACHE_KEY, serialized);
      } catch (e) {}
    }

    // UI_DATA_HELPERS_START
    function validOfferImage(value) {
      try {
        if (typeof value !== 'string' || value.length > 2048 || /[\u0000-\u001f\u007f]/.test(value)) return '';
        const u = new URL(value);
        if (u.protocol !== 'https:' || u.username || u.password || u.port ||
            u.href === 'https://i.imgur.com/UdIgTfI.png') return '';
        return u.href;
      } catch (e) { return ''; }
    }

    function offerImage(items) {
      const list = (Array.isArray(items) ? items : [items]).filter(Boolean);
      const campaign = list.find(o => validOfferImage(o.imageUrl) && o.imageSource !== 'story' &&
        o.source !== 'Stories_Only' && o.source !== 'Stories');
      return validOfferImage(campaign && campaign.imageUrl) ||
        list.map(o => validOfferImage(o.imageUrl) || validOfferImage(o.thumb)).find(Boolean) || '';
    }

    function availabilityEvidence(value) {
      if (!value || !['available', 'sold_out'].includes(value.state) ||
          !['api', 'ticket_listing', 'worker', 'iphone'].includes(value.source)) return null;
      const observedAt = catalogTime(value.observedAt);
      if (!observedAt || observedAt > Date.now() + 300000) return null;
      return { state: value.state, observedAt: observedAt, source: value.source };
    }

    function confirmedSoldOutTime(item) {
      const evidence = availabilityEvidence(item && item.availabilityEvidence);
      const soldOutAt = Math.max(catalogTime(item && item.soldOutAt), catalogTime(item && item.expiredAt),
        evidence && evidence.state === 'sold_out' ? evidence.observedAt : 0);
      const availableAt = Math.max(catalogTime(item && item.restockedAt),
        evidence && evidence.state === 'available' ? evidence.observedAt : 0);
      return availableAt > soldOutAt ? 0 : soldOutAt;
    }

    function homeOfferSets(active, history, now = Date.now()) {
      const today = saoPauloParts(now);
      const todayCivil = Date.UTC(today.year, today.month - 1, today.day);
      const currentSession = o => {
        const session = o && sessionDateInfo(o, now);
        return !session || session.timestamp >= todayCivil;
      };
      const activeItems = (active || []).filter(o => o && !o.cacheUnverified && currentSession(o) && !o.isExpired &&
        o.availability !== 'sold_out' && !confirmedSoldOutTime(o) && !linkUnavailableAt(o));
      const unavailableItems = (active || []).filter(o => o && !o.cacheUnverified && currentSession(o) && !o.isExpired &&
        o.availability !== 'sold_out' && !confirmedSoldOutTime(o) && linkUnavailableAt(o))
        .map(o => ({ ...o, linkUnavailableAt: linkUnavailableAt(o) }));
      const key = o => { const campaign = catalogCampaign(o.link); return campaign ? campaign.key : o.id; };
      const activeIds = new Set([...activeItems, ...unavailableItems].map(key));
      const soldOutItems = (history || []).filter(o => {
        const at = confirmedSoldOutTime(o);
        return o && !o.cacheUnverified && !o.linkUnavailableAt && currentSession(o) &&
          !activeIds.has(key(o)) && at > 0 && at <= now && now - at < 86400000;
      });
      return { activeItems: activeItems, soldOutItems: soldOutItems, unavailableItems: unavailableItems };
    }

    function hunterStatusPresentation(status, found, reason, context) {
      const states = {
        starting: ['Buscando novidades', 'buscando novidades…', 'activity', 'refresh'],
        updated: ['Coleta pausada', 'Nova versão instalada. A coleta retoma na próxima abertura.', 'neutral', 'clock'],
        complete: ['Coleta concluída', 'coleta concluída', 'neutral', 'check'],
        not_configured: ['Configuração pendente', 'configuração privada pendente', 'attention', 'alert'],
        activation_required: ['Ative a coleta', 'ative a coleta em Ajustes', 'attention', 'lock'],
        access_pending: ['Aguardando aprovação', 'acesso aguardando aprovação', 'attention', 'clock'],
        access_revoked: ['Acesso suspenso', 'acesso suspenso; coleta pausada', 'attention', 'lock'],
        access_expired: ['Acesso expirado', 'acesso expirado; solicite aprovação', 'attention', 'lock'],
        setup_unavailable: ['Verifique o acesso', 'não foi possível verificar o acesso; tente novamente em Ajustes', 'attention', 'alert'],
        unavailable: ['Coleta indisponível', 'coleta indisponível', 'attention', 'alert'],
        failed: ['Coleta indisponível', 'coleta indisponível', 'attention', 'alert'],
        error: ['Coleta indisponível', 'coleta indisponível', 'attention', 'alert'],
        busy: ['Coleta em andamento', 'outra execução em andamento', 'activity', 'refresh'],
        auth_required: ['Autenticação necessária', 'autenticação necessária; coleta suspensa', 'attention', 'lock'],
        connected_waiting: ['Login concluído', 'login concluído; coleta aguardando', 'waiting', 'pause'],
        rate_limited: ['Consulta limitada', 'consulta limitada pelo serviço', 'waiting', 'pause'],
        backoff: ['Aguardando', 'aguardando intervalo de segurança', 'waiting', 'pause'],
        cooldown: ['Pausa breve', 'aguardando intervalo mínimo', 'waiting', 'clock'],
        daily_budget: ['Limite diário', 'limite diário atingido', 'waiting', 'pause'],
        unknown: ['A confirmar', 'consulta não confirmada', 'neutral', 'alert'],
        empty: ['Sem novidades', 'sem novidades', 'neutral', 'check']
      };
      const privateSetupReasons = {
        bootstrap_missing: 'aguardando arquivo privado no iCloud',
        bootstrap_unavailable: 'arquivo privado indisponível; tentando novamente na próxima abertura',
        bootstrap_invalid: 'arquivo privado inválido; configuração pendente',
        device_mismatch: 'configuração privada pertence a outro aparelho'
      };
      const count = Number.isSafeInteger(found) && found > 0 ? found : 0;
      const known = Object.prototype.hasOwnProperty.call(states, status);
      const entry = status === 'found'
        ? [count ? '+' + count : 'Sem novidades', count + ' novidades encontradas', count ? 'positive' : 'neutral', count ? 'plus' : 'check']
        : states[known ? status : 'unknown'];
      const presentation = {
        status: status === 'found' || known ? status : 'unknown',
        label: entry[0],
        detail: status === 'not_configured' && Object.prototype.hasOwnProperty.call(privateSetupReasons, reason) ? privateSetupReasons[reason] : entry[1],
        tone: entry[2], icon: entry[3], attention: entry[2] === 'attention'
      };
      if (context && context.transport === 'webview') {
        const details = {
          auth_required: 'entre no Instagram neste iPhone em Ajustes',
          rate_limited: 'Instagram limitou as consultas (HTTP 429)',
          daily_budget: 'limite diário de consultas deste iPhone atingido',
          backoff: 'aguardando nova tentativa após falha de consulta',
          unknown: 'consulta ao Instagram não confirmada',
          ready: 'login aberto; captura ainda não confirmada',
          cancelled: 'conexão cancelada; captura não confirmada',
          empty: 'consulta concluída; nenhum Story elegível',
          found: count + ' Stories encontrados'
        };
        const reasons = {
          network_error: 'falha de rede ao consultar o Instagram',
          browser_network_error: 'falha de rede ao consultar o Instagram',
          browser_evaluation_failed: 'não foi possível executar a consulta no navegador deste iPhone',
          browser_bridge_unsupported: 'o navegador não conseguiu devolver o resultado da consulta',
          browser_script_error: 'a consulta falhou ao executar no navegador',
          browser_runtime_unsupported: 'o navegador não oferece um recurso necessário à leitura da resposta',
          browser_load_failed: 'não foi possível carregar a página do Instagram',
          browser_timeout: 'o navegador excedeu o tempo de consulta',
          request_timeout: 'o Instagram não respondeu dentro do tempo de consulta',
          browser_response_invalid: 'o Instagram retornou dados que a captura não reconheceu',
          browser_story_page_invalid: 'o navegador não abriu os Stories do clubeuol',
          browser_story_data_invalid: 'a página de Stories retornou dados inválidos',
          browser_story_data_missing: 'a página abriu, mas os dados dos Stories ainda não estão disponíveis',
          non_json_response: 'o Instagram retornou uma página em vez dos dados da consulta',
          body_limit: 'a resposta do Instagram excedeu o tamanho permitido',
          profile_identity_mismatch: 'não foi possível validar o perfil clubeuol',
          owner_mismatch: 'não foi possível confirmar que os Stories pertencem ao clubeuol',
          no_validated_story_structure: 'a resposta não contém uma lista de Stories validável',
          api_error: 'o Instagram recusou a consulta',
          malformed_story_payload: 'os dados de um Story não passaram na validação',
          structure_limit: 'a resposta contém mais dados do que a captura permite',
          conflicting_story_payload: 'a resposta contém dados conflitantes para o mesmo Story',
          http_status: 'o Instagram não aceitou a consulta',
          unexpected_response_url: 'a consulta retornou de um endereço inesperado',
          browser_login_failed: 'não foi possível verificar o navegador após fechar o login',
          login_incomplete: 'o login ainda não foi confirmado',
          local_outbox_full: 'a fila local está cheia; aguarde o envio das capturas pendentes',
          http_429: 'Instagram limitou as consultas (HTTP 429)',
          rate_limited: 'Instagram limitou as consultas (HTTP 429)',
          daily_budget: 'limite diário de consultas deste iPhone atingido'
        };
        if (Object.prototype.hasOwnProperty.call(details, status)) presentation.detail = details[status];
        if (['unknown', 'backoff', 'unavailable', 'failed', 'error'].includes(status) && Object.prototype.hasOwnProperty.call(reasons, reason)) presentation.detail = reasons[reason];
        if (status === 'connected_waiting') presentation.detail = 'login concluído; coleta aguardando ' + (reason === 'http_429' ? 'o fim da limitação anterior (HTTP 429)' : 'a renovação do limite diário');
        const stages = { stories_page: 'página de Stories', profile: 'consulta do perfil', stories: 'consulta dos Stories', login: 'verificação do login' };
        if (!['found', 'empty'].includes(status) && Object.prototype.hasOwnProperty.call(stages, context.stage)) presentation.detail += '; etapa: ' + stages[context.stage];
        if (Number.isInteger(context.statusCode) && context.statusCode >= 100 && context.statusCode <= 599 && context.statusCode !== 429) presentation.detail += ' (HTTP ' + context.statusCode + ')';
        if (['rate_limited', 'backoff', 'daily_budget', 'connected_waiting'].includes(status) && typeof context.nextAttemptAt === 'string' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{3})?Z$/.test(context.nextAttemptAt) && Number.isFinite(Date.parse(context.nextAttemptAt))) {
          presentation.detail += '; próxima tentativa em ' + new Date(context.nextAttemptAt).toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
        }
      }
      return presentation;
    }

    function hunterStatusIcon(icon) {
      const paths = {
        check: '<path d="m5 12 4 4L19 6"/>',
        plus: '<path d="M12 5v14M5 12h14"/>',
        pause: '<path d="M8 5v14M16 5v14"/>',
        clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
        lock: '<rect x="5" y="10" width="14" height="11" rx="3"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/>',
        alert: '<circle cx="12" cy="12" r="9"/><path d="M12 7v6m0 3v1"/>',
        refresh: '<path d="M20 7v5h-5M4 17v-5h5M6 7a7 7 0 0 1 12-1l2 2M4 16l2 2a7 7 0 0 0 12-1"/>'
      };
      return '<svg class="collection-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' + paths[icon] + '</svg>';
    }

    function setHunterCollectionStatus(summary) {
      const element = document.getElementById('hunterCollectionStatus');
      if (!element) return;
      const details = document.getElementById('hunterCollectionDetails');
      const overview = document.getElementById('hunterCollectionOverview');
      const diagnostics = document.getElementById('hunterCollectionDiagnostics');
      element.hidden = !summary || summary.status === 'disabled';
      if (element.hidden) {
        overview.innerHTML = '';
        diagnostics.innerHTML = '';
        details.open = false;
        element.setAttribute('data-attention-key', '');
        if (typeof updateCatalogStatusTrigger === 'function') updateCatalogStatusTrigger();
        return;
      }
      const count = value => Number.isSafeInteger(value) && value > 0 ? value : 0;
      const primary = hunterStatusPresentation(summary.status || 'complete', summary.found, summary.reason);
      const initial = summary.status === 'starting' || summary.status === 'updated';
      const metrics = initial ? [] : [
        { value: count(summary.received), label: 'recebidas', detail: 'recebidas pelo servidor' },
        { value: count(summary.delivered), label: 'entregues', detail: 'entregues' },
        { value: count(summary.pending), label: 'pendentes', detail: 'pendentes no iPhone' }
      ].filter(metric => metric.value);
      const sources = initial ? [] : ['portal', 'instagram'].flatMap(source => {
        const result = summary.sources && summary.sources[source];
        return result ? [{ name: source === 'portal' ? 'Portal' : 'Instagram', ...hunterStatusPresentation(result.status, result.found, result.reason, source === 'instagram' ? result : null) }] : [];
      });
      const attentionKey = [primary, ...sources].filter(state => state.attention).map(state => (state.name || 'coleta') + ':' + state.status + ':' + state.detail).join('|');
      if (attentionKey && attentionKey !== element.getAttribute('data-attention-key')) details.open = true;
      else if (!attentionKey && element.getAttribute('data-attention-key')) details.open = false;
      element.setAttribute('data-attention-key', attentionKey);
      const showPrimary = primary.status !== 'complete' || (!metrics.length && !sources.length);
      overview.innerHTML = (showPrimary ? '<span class="collection-state" data-tone="' + primary.tone + '">' + hunterStatusIcon(primary.icon) + '<span>' + primary.label + '</span></span>' : '') +
        metrics.map(metric => '<span class="collection-metric"><strong>' + metric.value + '</strong> ' + metric.label + '</span>').join('') +
        sources.map(source => '<span class="collection-source" data-tone="' + source.tone + '"><span class="collection-source-name">' + source.name + '</span>' + hunterStatusIcon(source.icon) +
          '<span' + (source.icon === 'check' || primary.attention && source.status === primary.status ? ' class="visually-hidden"' : '') + '>' + source.label + '</span></span>').join('');
      diagnostics.innerHTML = '<p class="collection-context">Neste iPhone</p>' +
        (showPrimary && (!primary.attention || !sources.some(source => source.detail === primary.detail)) ? '<p' + (primary.attention ? ' class="collection-attention"' : '') + '>' + primary.detail + '</p>' : '') +
        metrics.map(metric => '<p>' + metric.value + ' ' + metric.detail + '</p>').join('') +
        sources.map(source => '<p' + (source.attention ? ' class="collection-attention"' : '') + '><strong>' + source.name + ':</strong> ' + source.detail + '</p>').join('');
      if (typeof updateCatalogStatusTrigger === 'function') updateCatalogStatusTrigger();
    }

    function updateCatalogStatusTrigger() {
      const trigger = document.getElementById('catalogStatusTrigger');
      if (!trigger) return;
      const attention = !!document.getElementById('hunterCollectionStatus')?.getAttribute('data-attention-key');
      const caption = document.getElementById('catalogStatusTriggerText');
      const status = document.getElementById('liveStatusText');
      const state = document.getElementById('liveDot')?.getAttribute('data-state') || 'loading';
      if (caption) caption.textContent = attention ? 'Verificar coleta' :
        linkCheckProgress && linkCheckProgress.pending > 0 ?
          'Links ' + linkCheckProgress.verified + '/' + linkCheckProgress.total :
          state === 'fresh' ? 'Atualizado' : state === 'cache' ? 'Em cache' : 'Atualizando…';
      trigger.setAttribute('data-attention', String(attention));
      trigger.setAttribute('data-state', state);
      trigger.setAttribute('aria-label', (attention ? 'Coleta precisa de atenção. ' : '') + (status?.getAttribute('aria-label') || 'Catálogo') + '. Abrir detalhes do catálogo e da coleta');
    }

    function openCatalogStatus() {
      const modal = document.getElementById('catalogStatusModal');
      if (!modal) return;
      modal.hunterReturnFocus = document.activeElement;
      modal.style.display = 'flex';
      document.getElementById('catalogStatusTrigger')?.setAttribute('aria-expanded', 'true');
      const details = document.getElementById('hunterCollectionDetails');
      if (details) details.open = true;
      modal.querySelector('.catalog-status-close')?.focus();
      modal.onkeydown = event => {
        if (event.key === 'Escape') { event.preventDefault(); closeCatalogStatus(); }
        if (event.key !== 'Tab') return;
        const controls = [...modal.querySelectorAll('button, summary')].filter(control => control.offsetParent !== null);
        const first = controls[0], last = controls[controls.length - 1];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
      };
    }

    function closeCatalogStatus() {
      const modal = document.getElementById('catalogStatusModal');
      if (!modal) return;
      modal.style.display = 'none';
      document.getElementById('catalogStatusTrigger')?.setAttribute('aria-expanded', 'false');
      modal.hunterReturnFocus?.focus?.();
    }
    // UI_DATA_HELPERS_END

    function mergeCatalogSnapshot(snapshot) {
      const records = new Map();
      const identity = o => { const campaign = catalogCampaign(o.link); return campaign ? campaign.key : 'local:' + o.id; };
      (historyData.history || []).forEach(o => records.set(identity(o), { offer: o, past: true }));
      (historyData.active || []).forEach(o => records.set(identity(o), { offer: o, past: false }));
      const bounded = (value, max) => typeof value === 'string'
        ? value.slice(0, max).replace(/[\u0000-\u001f\u007f]/g, ' ').trim() : '';
      const nonempty = (value, fallback, max) => bounded(value, max) || bounded(fallback, max);
      const image = validOfferImage;

      function upsert(entry, story) {
        const campaign = catalogCampaign(entry.link);
        if (!campaign) return;
        const previous = records.get(campaign.key);
        const old = previous ? previous.offer : {};
        const id = bounded(old.id, 160) || campaign.slug;
        const codeMatch = campaign.slug.match(/^(p[A-Za-z0-9]+)-/i);
        const rawTitle = nonempty(entry.title, old.rawTitle || old.title || campaign.slug.replace(/^p[a-z0-9]+-/i, '').replace(/-/g, ' '), 1000);
        const cachedDesc = bounded(old.description || getCachedDescription(id), 20000);
        const description = nonempty(entry.description, cachedDesc, 20000);
        const richerDescription = description.length >= cachedDesc.length ? description : cachedDesc;
        const isPortal = entry.isPortal === true || old.isPortal === true || old.source === 'Portal';
        const hasStory = Boolean(story || entry.hasStory || old.hasStory || old.source === 'Stories' || old.source === 'Stories_Only' || old.badge === 'Stories');
        const workerCapture = entry.captureSource === 'portal-client' ? 0 : plausibleCaptureTime(entry.firstSeenAt);
        const portalCapture = entry.captureSource === 'portal-client' ? plausibleCaptureTime(entry.capturedAt) : 0;
        const priorCapture = captureTime(old);
        const capturedAt = workerCapture && old.captureSource === 'worker' && priorCapture
          ? Math.min(workerCapture, priorCapture) : (workerCapture || priorCapture || portalCapture);
        const legacyFirstSeen = old.legacyFirstSeen || (!priorCapture && (old.firstSeen || old.capturedAt)) || 0;
        const captureSource = workerCapture ? 'worker' : (priorCapture ? old.captureSource :
          (portalCapture ? 'portal-client' : (legacyFirstSeen ? 'legacy' : 'unknown')));
        const lastSourceSeenAt = Math.max(catalogTime(old.lastSourceSeenAt), catalogTime(old.lastSeen),
          catalogTime(entry.lastSeenAt));
        const offer = {
          ...old,
          id: id,
          catalogOfferId: nonempty(entry.id, old.catalogOfferId || '', 160),
          code: bounded(old.code, 32) || (codeMatch ? codeMatch[1] : campaign.slug.slice(0, 3)),
          link: old.link || campaign.link,
          rawTitle: rawTitle,
          title: toSentenceCase(rawTitle),
          imageUrl: story ? (offerImage(old) || image(entry.imageUrl)) : (image(entry.imageUrl) || old.imageUrl || ''),
          imageSource: story && offerImage(old) ? old.imageSource : (story ? 'story' : (image(entry.imageUrl) ? 'campaign' : old.imageSource)),
          description: richerDescription,
          partner: nonempty(entry.partner, old.partner || '', 500),
          validity: nonempty(entry.validity, '', 500).length > bounded(old.validity, 500).length
            ? nonempty(entry.validity, '', 500) : bounded(old.validity, 500),
          category: classifyCategory(rawTitle, campaign.slug, richerDescription),
          source: isPortal ? 'Portal' : (hasStory ? 'Stories_Only' : 'Worker'),
          isPortal: isPortal,
          hasStory: hasStory,
          capturedAt: capturedAt,
          captureSource: captureSource,
          firstSeen: capturedAt, // Legacy consumers read this alias; it is never an event date.
          legacyFirstSeen: legacyFirstSeen,
          sourcePublishedAt: Math.max(catalogTime(old.sourcePublishedAt), catalogTime(entry.publishedAt)),
          lastSourceSeenAt: lastSourceSeenAt,
          lastSeen: lastSourceSeenAt,
          viewsCount: old.viewsCount || 0
        };
        offer.eventDate = (sessionDateInfo(offer) || {}).key || old.eventDate || '';
        let past = Boolean(previous && previous.past);
        if (story) {
          offer.storyId = entry.storyId;
          offer.storyExpiresAt = catalogTime(entry.expiresAt);
          // A validade de um Story não informa se os ingressos esgotaram.
        } else {
          const incomingEvidence = availabilityEvidence(entry.availabilityEvidence);
          const oldEvidence = availabilityEvidence(old.availabilityEvidence);
          const soldOutAt = catalogTime(entry.soldOutAt) ||
            (incomingEvidence && incomingEvidence.state === 'sold_out' ? incomingEvidence.observedAt : 0);
          const restockedAt = catalogTime(entry.restockedAt);
          const oldSoldOutAt = confirmedSoldOutTime(old);
          const oldConfirmedAt = Math.max(oldEvidence ? oldEvidence.observedAt : 0, oldSoldOutAt, catalogTime(old.restockedAt));
          const availableAt = Math.max(restockedAt, incomingEvidence && incomingEvidence.state === 'available' ? incomingEvidence.observedAt : 0);
          const incomingAt = Math.max(soldOutAt, availableAt);
          const stale = oldConfirmedAt > incomingAt;
          if (incomingEvidence && !stale) offer.availabilityEvidence = incomingEvidence;
          if (incomingEvidence && !stale && Date.now() - incomingEvidence.observedAt <= 600000)
            delete offer.cacheUnverified;
          if (incomingEvidence?.state === 'available' && !stale &&
              incomingEvidence.observedAt > catalogTime(old.linkUnavailableAt))
            delete offer.linkUnavailableAt;
          if (!stale && availableAt <= soldOutAt && (entry.availability === 'sold_out' || (incomingEvidence && incomingEvidence.state === 'sold_out'))) {
            past = true;
            // Unknown exhaustion time remains unknown. Cache refresh is not an event.
            if (soldOutAt || oldSoldOutAt) offer.expiredAt = offer.soldOutAt = soldOutAt || oldSoldOutAt;
            else { delete offer.expiredAt; delete offer.soldOutAt; }
            offer.isExpired = true;
          } else if (!stale && past && availableAt > oldSoldOutAt) {
            past = false;
            delete offer.expiredAt;
            delete offer.soldOutAt;
            offer.isExpired = false;
          }
          if (restockedAt > catalogTime(old.restockedAt)) offer.restockedAt = restockedAt;
          offer.availability = past ? 'sold_out' : 'observed';
        }
        records.set(campaign.key, { offer: offer, past: past });
      }
      // Gerações diferentes do Worker podem ter o mesmo link. Vence a observação
      // mais recente, sem deixar uma geração antiga apagar dados atuais.
      const latest = new Map();
      const observedAt = o => Math.max(catalogTime(o.firstSeenAt), catalogTime(o.lastSeenAt), catalogTime(o.soldOutAt), catalogTime(o.restockedAt), catalogTime(o.availabilityEvidence && o.availabilityEvidence.observedAt));
      snapshot.offers.forEach(o => {
        const campaign = catalogCampaign(o.link);
        if (campaign && (!latest.has(campaign.key) || observedAt(o) > observedAt(latest.get(campaign.key)))) latest.set(campaign.key, o);
      });
      latest.forEach(o => upsert(o, false));
      snapshot.stories.forEach(s => upsert(s, true));
      // O snapshot é limitado. Ausência, truncamento ou resposta vazia nunca apagam ofertas conhecidas.
      historyData.active = Array.from(records.values()).filter(r => !r.past).map(r => r.offer);
      historyData.history = Array.from(records.values()).filter(r => r.past).map(r => r.offer);
      saveHistoryToStorage();
    }

    function handleNativeCollectionResult(result, nativeHistory) {
      if (result && typeof result === 'object') {
        INITIAL.hunterCollection = { ...(INITIAL.hunterCollection || {}), ...result };
        if (result.access) INITIAL.hunterAccess = result.access;
        setHunterCollectionStatus(INITIAL.hunterCollection);
        if (typeof renderHunterAccessUI === 'function') renderHunterAccessUI();
      }
      if (nativeHistory && Array.isArray(nativeHistory.active) && Array.isArray(nativeHistory.history)) {
        const identity = item => catalogCampaign(item && item.link)?.key || 'local:' + String(item && item.id || '');
        const records = new Map();
        (historyData.history || []).forEach(item => records.set(identity(item), { offer: item, past: true }));
        (historyData.active || []).forEach(item => records.set(identity(item), { offer: item, past: false }));
        const add = (item, past) => {
          if (!item || typeof item !== 'object' || !item.id || !item.title) return;
          const key = identity(item);
          const previous = records.get(key);
          const old = previous && previous.offer || {};
          const incomingCapture = captureTime(item);
          const oldCapture = captureTime(old);
          const preserveWorkerCapture = old.captureSource === 'worker' && oldCapture;
          const capturedAt = preserveWorkerCapture ? oldCapture : (incomingCapture && oldCapture ? Math.min(incomingCapture, oldCapture) : incomingCapture || oldCapture);
          const incomingTransition = Math.max(catalogTime(item.soldOutAt), catalogTime(item.expiredAt), catalogTime(item.restockedAt));
          const oldTransition = Math.max(catalogTime(old.soldOutAt), catalogTime(old.expiredAt), catalogTime(old.restockedAt),
            catalogTime(old.availabilityEvidence && old.availabilityEvidence.observedAt));
          const incomingObserved = Math.max(incomingTransition, catalogTime(item.lastSourceSeenAt), catalogTime(item.lastSeen));
          const oldObserved = Math.max(oldTransition, catalogTime(old.lastSourceSeenAt), catalogTime(old.lastSeen));
          const incomingWins = !previous || old.cacheUnverified === true || incomingTransition > oldTransition ||
            incomingTransition === oldTransition && (old.cacheUnverified || incomingObserved >= oldObserved);
          const merged = incomingWins ? { ...old, ...item } : { ...item, ...old };
          if (incomingWins) delete merged.cacheUnverified;
          const incomingEvidence = availabilityEvidence(item.availabilityEvidence);
          if (incomingWins && incomingEvidence?.state === 'available' &&
              incomingEvidence.observedAt > catalogTime(old.linkUnavailableAt))
            delete merged.linkUnavailableAt;
          merged.capturedAt = merged.firstSeen = capturedAt || 0;
          merged.legacyFirstSeen = old.legacyFirstSeen || item.legacyFirstSeen ||
            (!oldCapture && (old.firstSeen || old.capturedAt)) ||
            (!incomingCapture && (item.firstSeen || item.capturedAt)) || 0;
          merged.captureSource = preserveWorkerCapture ? 'worker' :
            (oldCapture ? old.captureSource : (incomingCapture ? item.captureSource : (merged.legacyFirstSeen ? 'legacy' : 'unknown')));
          merged.lastSourceSeenAt = merged.lastSeen = Math.max(catalogTime(old.lastSourceSeenAt),
            catalogTime(old.lastSeen), catalogTime(item.lastSourceSeenAt), catalogTime(item.lastSeen));
          merged.sourcePublishedAt = Math.max(catalogTime(old.sourcePublishedAt), catalogTime(item.sourcePublishedAt));
          merged.eventDate = (sessionDateInfo(merged) || {}).key || old.eventDate || '';
          if (!incomingWins) {
            merged.description = old.description || item.description || '';
            merged.imageUrl = old.imageUrl || item.imageUrl || '';
          }
          records.set(key, { offer: merged, past: incomingWins ? past : previous.past });
        };
        nativeHistory.history.slice(0, 2000).forEach(item => add(item, true));
        nativeHistory.active.slice(0, 2000).forEach(item => add(item, false));
        historyData.active = Array.from(records.values()).filter(record => !record.past).map(record => record.offer);
        historyData.history = Array.from(records.values()).filter(record => record.past).map(record => record.offer);
        saveHistoryToStorage();
        renderCatalogStatus();
        hunterRequestLinkChecks();
      }
    }
    if (typeof window !== 'undefined') window.handleNativeCollectionResult = handleNativeCollectionResult;

    function setCatalogHeaderStatus(text, state, visibleCount) {
      const status = document.getElementById('liveStatusText');
      const dot = document.getElementById('liveDot');
      const count = document.getElementById('catalogOfferCount');
      if (status) {
        status.textContent = text === 'Catálogo em cache' ? 'Em cache' : text === 'Catálogo atualizado' ? 'Atualizado' : text;
        status.setAttribute('aria-label', text);
        status.setAttribute('aria-busy', String(state === 'loading'));
      }
      if (dot) {
        dot.style.backgroundColor = state === 'fresh' ? '#00e676' : state === 'cache' ? '#ffb300' : '#38bdf8';
        dot.setAttribute('data-state', state);
      }
      if (count && Number.isSafeInteger(visibleCount)) count.textContent = visibleCount + (visibleCount === 1 ? ' oferta' : ' ofertas');
      if (typeof updateCatalogStatusTrigger === 'function') updateCatalogStatusTrigger();
    }

    function renderCatalogStatus(fresh = catalogStatusFresh) {
      catalogStatusFresh = fresh;
      const sets = homeOfferSets(historyData.active, historyData.history);
      const offers = sets.activeItems;
      const visibleCount = offers.length + sets.soldOutItems.length;
      if (currentTab === 'home' || currentTab === 'tickets') renderOffers(historyData.active);
      else if (currentTab === 'history') renderHistoryScreen();
      updateCategoryCounts(offers);
      document.getElementById('activesCountText').textContent = offers.length + ' Ativas';
      const loading = catalogRefreshInFlight && !catalogRefreshSettled;
      setCatalogHeaderStatus(loading ? 'Atualizando…' : fresh ? 'Catálogo atualizado' : 'Catálogo em cache',
        loading ? 'loading' : fresh ? 'fresh' : 'cache', visibleCount);
      updateTimelineBadgeDot();
    }

    async function refreshHunterCatalog() {
      if (catalogRefreshInFlight) return;
      const button = document.getElementById('btnRefreshCatalog');
      if (button) { button.disabled = true; button.setAttribute('aria-busy', 'true'); }
      setCatalogHeaderStatus('Atualizando…', 'loading');
      try { await streamLiveOffers(true); }
      finally {
        if (button) { button.disabled = false; button.setAttribute('aria-busy', 'false'); }
      }
    }

    async function streamLiveOffers(force = false) {
      if (catalogRefreshInFlight || (catalogRefreshStarted && !force)) return;
      catalogRefreshInFlight = true;
      catalogRefreshSettled = false;
      catalogRefreshStarted = true;
      const cache = readCatalogCache();
      try {
        let snapshot = cache && cache.snapshot;
        // Revalidate on each opening; the cache remains the immediate first frame.
        const headers = cache && cache.etag ? { 'If-None-Match': cache.etag } : {};
        const controller = typeof AbortController === 'function' ? new AbortController() : null;
        const timeout = controller ? setTimeout(() => controller.abort(), 10000) : null;
        try {
          const response = await fetch(CATALOG_URL, { headers: headers, credentials: 'omit', ...(controller ? { signal: controller.signal } : {}) });
          if (response.status === 304 && cache) {
            saveCatalogCache(snapshot, cache.etag);
          } else {
            if (!response.ok) throw new Error('Catalog unavailable');
            const body = await response.text();
            if (body.length > CATALOG_MAX_BYTES) throw new Error('Catalog too large');
            snapshot = JSON.parse(body);
            if (!isCatalogSnapshot(snapshot)) throw new Error('Invalid catalog');
            saveCatalogCache(snapshot, response.headers.get('ETag'));
          }
        } finally { if (timeout) clearTimeout(timeout); }
        catalogRefreshSettled = true;
        mergeCatalogSnapshot(snapshot);
        renderCatalogStatus(['api', 'ticketListing'].some(key => {
          const age = Date.now() - catalogTime(snapshot.sources[key].lastSuccessAt);
          return snapshot.sources[key].healthy && age >= 0 && age <= 600000;
        }));
      } catch (e) {
        catalogRefreshSettled = true;
        if (cache) {
          mergeCatalogSnapshot(cache.snapshot);
          renderCatalogStatus(false);
        } else if (typeof INITIAL !== 'undefined' && (INITIAL.hunterCollection || INITIAL.collectionPausedForUpdate)) {
          // Native collection owns source requests; do not duplicate it from the WebView.
          renderCatalogStatus(false);
        } else {
          await streamPortalOffersFallback();
        }
      } finally {
        catalogRefreshInFlight = false;
        const sk = document.getElementById('skeletonList');
        if (sk) sk.style.display = 'none';
        const status = document.getElementById('liveStatusText');
        if (status) status.setAttribute('aria-busy', 'false');
        hunterRequestLinkChecks();
        refreshAvailabilityEvents();
      }
    }

    async function streamPortalOffersFallback() {
      try {
        const [portalRes, storiesRes] = await Promise.allSettled([
          fetch('/campanhasdeingresso'),
          fetch('https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/stories')
        ]);

        if (portalRes.status !== 'fulfilled' || !portalRes.value.ok) throw new Error('Portal fetch failed');
        const html = await portalRes.value.text();

        const storySlugs = new Set();
        if (storiesRes.status === 'fulfilled') {
          try {
            const sData = await storiesRes.value.json();
            if (sData && sData.stories && Array.isArray(sData.stories)) {
              sData.stories.forEach(st => {
                if (st.link) {
                  const sSlug = st.link.split('/').pop().toLowerCase();
                  storySlugs.add(sSlug);
                  storySlugs.add(st.link.toLowerCase());
                }
              });
            }
          } catch (e) {}
        }

        const cardRegex = /<div[^>]*class="[^"]*beneficio[^"]*"[\s\S]*?<a[^>]*href="(\/campanhasdeingresso\/[^"]+)"[\s\S]*?data-src="([^"]+)"[\s\S]*?<p[^>]*class="titulo mb-0">([\s\S]*?)<\/p>[\s\S]*?<!-- Fim div beneficio -->/gi;

        let m;
        const liveOffers = [];
        const now = Date.now();
        const existingMap = new Map();

        // Indexa histórico conhecido
        (historyData.active || []).forEach(o => existingMap.set(o.id, o));
        (historyData.history || []).forEach(o => existingMap.set(o.id, o));

        while ((m = cardRegex.exec(html)) !== null) {
          const rawUrl = m[1];
          const slug = rawUrl.split('/').pop();
          const codeMatch = slug.match(/^(p[A-Za-z0-9]+)-/i);
          const code = codeMatch ? codeMatch[1] : slug.slice(0, 3);
          const rawTitle = m[3].replace(/<[^>]+>/g, '').trim();
          const title = toSentenceCase(rawTitle);
          const link = 'https://clube.uol.com.br' + rawUrl;
          const imageUrl = m[2];
          const old = existingMap.get(slug);
          const cachedDesc = (old && old.description) ? old.description : getCachedDescription(slug);
          const category = classifyCategory(title, slug, cachedDesc);

          const firstSeen = captureTime(old) || now;
          const viewsCount = old && old.viewsCount ? old.viewsCount : 0;

          // Se estiver nos stories do worker ou histórico anterior, marca como Stories!
          const isStory = storySlugs.has(slug.toLowerCase()) ||
                          storySlugs.has(link.toLowerCase()) ||
                          Boolean(old && (old.hasStory || old.source === 'Stories' || old.badge === 'Stories'));

          liveOffers.push({
            id: slug,
            code: code,
            title: title,
            rawTitle: rawTitle,
            link: link,
            imageUrl: imageUrl,
            category: category,
            source: 'Portal',
            isPortal: true,
            hasStory: isStory,
            firstSeen: firstSeen,
            capturedAt: firstSeen,
            captureSource: captureTime(old) ? (old.captureSource || 'legacy') : 'portal-client',
            lastSourceSeenAt: now,
            lastSeen: now,
            viewsCount: viewsCount,
            description: cachedDesc
          });
        }

        mergeCatalogSnapshot({ offers: liveOffers.map(o => ({ ...o, availability: 'observed', firstSeenAt: o.firstSeen, lastSeenAt: now })), stories: [] });
        renderCatalogStatus(false);
        hunterRequestLinkChecks();
      } catch (e) {
        renderCatalogStatus(false);
      }
    }
    // CATALOG_ADAPTER_END

    // ===================================================================
    // RENDERIZAÇÃO DE CARDS
    // ===================================================================
    const sessionTrackedIds = new Set();

    function recordSessionImpressions(offers) {
      if (!offers || offers.length === 0) return;
      let viewCounts = {};
      try {
        viewCounts = JSON.parse(localStorage.getItem('uol_offer_view_counts') || '{}');
      } catch(e) {}
      let changed = false;
      offers.forEach(o => {
        if (!sessionTrackedIds.has(o.id)) {
          sessionTrackedIds.add(o.id);
          const prev = viewCounts[o.id] != null ? viewCounts[o.id] : (o.viewsCount || 0);
          viewCounts[o.id] = prev + 1;
          o.viewsCount = viewCounts[o.id];
          changed = true;
        } else if (viewCounts[o.id] != null) {
          o.viewsCount = viewCounts[o.id];
        }
      });
      if (changed) {
        try {
          localStorage.setItem('uol_offer_view_counts', JSON.stringify(viewCounts));
        } catch(e) {}
        saveHistoryToStorage();
      }
    }

    function renderOffers(list) {
      const home = homeOfferSets(list, historyData.history);
      recordSessionImpressions(home.activeItems);

      const container = document.getElementById('cardList');
      if (!container) return;
      container.innerHTML = '';

      let filtered = home.activeItems;
      if (activeFilter) filtered = filtered.filter(o => o.category === activeFilter);

      if (isGrouped) {
        const unavailable = home.unavailableItems.filter(o => !activeFilter || o.category === activeFilter);
        renderGroupedOffers(filtered, [...home.soldOutItems, ...unavailable], container);
      } else {
        renderIndividualOffers(filtered, container, home.unavailableItems.filter(o => !activeFilter || o.category === activeFilter));
      }
    }

    function getTodayAndTomorrowStrings() {
      // Horário de Brasília / São Paulo (UTC-3)
      const nowBr = new Date(Date.now() - 3 * 3600 * 1000);
      const d = String(nowBr.getUTCDate()).padStart(2, '0');
      const m = String(nowBr.getUTCMonth() + 1).padStart(2, '0');
      const todayStr = d + '/' + m;

      const tmrBr = new Date(Date.now() - 3 * 3600 * 1000 + 86400000);
      const td = String(tmrBr.getUTCDate()).padStart(2, '0');
      const tm = String(tmrBr.getUTCMonth() + 1).padStart(2, '0');
      const tmrStr = td + '/' + tm;

      return { todayStr, tmrStr };
    }

    function formatSessionDateBadge(dateStr) {
      if (!dateStr) return '';
      const { todayStr, tmrStr } = getTodayAndTomorrowStrings();
      if (dateStr === todayStr) return 'HOJE';
      if (dateStr === tmrStr) return 'AMANHÃ';

      if (dateStr.toUpperCase().startsWith('ATÉ ')) {
        return dateStr.toUpperCase();
      }

      const fullMonths = {
        '01': 'JAN', '02': 'FEV', '03': 'MAR', '04': 'ABR',
        '05': 'MAI', '06': 'JUN', '07': 'JUL', '08': 'AGO',
        '09': 'SET', '10': 'OUT', '11': 'NOV', '12': 'DEZ'
      };

      const m = dateStr.match(/^(\d{1,2})\/(\d{1,2})$/);
      if (m) {
        const day = parseInt(m[1], 10);
        const mon = fullMonths[m[2].padStart(2, '0')] || m[2];
        return day + ' ' + mon;
      }
      return dateStr.toUpperCase();
    }

    function isStoriesOnlyOffer(item) {
      if (!item) return false;
      const str = ((item.rawTitle || item.title || '') + ' ' + (item.eventTitle || '') + ' ' + (item.description || '')).toLowerCase();
      if (str.includes('zayn') || str.includes('maroon 5') || str.includes('love is like world tour') || str.includes('konnakol')) {
        return true;
      }
      return item.source === 'Stories_Only';
    }

    // REDEMPTION_IDENTITY_START
    function redemptionIdentity(record) {
      if (!record || typeof record !== 'object') return null;
      const account = String(record.user || record.accountId || '').trim().toLowerCase();
      if (!account) return null;
      const id = record.id != null && /^[A-Za-z0-9_-]{1,200}$/.test(String(record.id).trim()) &&
        !/^redemption-.+-\d+$/.test(String(record.id).trim()) ? String(record.id).trim() : '';
      const url = typeof record.url === 'string' ? record.url.trim() : '';
      const receipt = /^https:\/\/clube\.uol\.com\.br\/perfil\/beneficios\/([A-Za-z0-9_-]{1,200})\/?$/.exec(url);
      if (id && receipt && receipt[1] !== id) return null;
      const voucher = id || receipt ? 'id:' + (id || receipt[1]) :
        (url && !catalogCampaign(url) ? 'url:' + url : '');
      return voucher ? account + '|' + voucher : null;
    }

    function getAllRedemptionsList() {
      const records = new Map();
      const unidentified = new Set();
      const bundled = typeof INITIAL !== 'undefined' && Array.isArray(INITIAL.realRedemptions) ? INITIAL.realRedemptions : [];
      const privateFields = new Set(['voucherImageDataUrl', 'localVoucherFile', 'pdfData', 'voucherData', 'imageData']);
      const nativeOnlyFields = ['nativeVoucherIndex', 'hasNativeVoucher', 'receiptVerified', 'receiptVerifiedAt'];
      const safeRecord = record => Object.fromEntries(Object.entries(record).filter(([key, value]) =>
        !privateFields.has(key) && !(typeof value === 'string' && /^data:/i.test(value))));
      const verified = new Map();
      const liveVerified = typeof window !== 'undefined' && window.hunterVerifiedRedemptions instanceof Map
        ? Array.from(window.hunterVerifiedRedemptions.values()).filter(record => record && typeof record === 'object') : [];
      bundled.forEach(record => {
        const key = redemptionIdentity(record);
        if (key && record.receiptVerified === true) verified.set(key, safeRecord(record));
      });
      liveVerified.forEach(record => {
        const key = redemptionIdentity(record);
        if (key) verified.set(key, safeRecord(record));
      });
      const add = (items, origin) => {
        if (!Array.isArray(items)) return;
        items.forEach((record, index) => {
          if (!record || typeof record !== 'object') return;
          const identity = redemptionIdentity(record);
          const safe = safeRecord(record);
          if (origin !== 'bundle' && origin !== 'native') {
            nativeOnlyFields.forEach(field => { delete safe[field]; });
          }
          if (!identity) {
            // Cópias idênticas entre bundle e cache não são novos resgates.
            const fingerprint = JSON.stringify(Object.fromEntries(Object.keys(safe).sort().map(key => [key, safe[key]])));
            if (unidentified.has(fingerprint)) return;
            unidentified.add(fingerprint);
          }
          if (origin !== 'bundle' && origin !== 'native') safe.cacheUnverified = true;
          const key = identity || 'unidentified:' + origin + ':' + index;
          const old = records.get(key) || {};
          const auth = identity && verified.get(identity);
          const merged = { ...old, ...safe };
          if (auth) {
            // Verified receipt wins even when an older WebView cache is visited last.
            nativeOnlyFields.forEach(field => { delete merged[field]; });
            for (const field of ['id', 'user', 'accountId', 'url']) {
              if (!Object.prototype.hasOwnProperty.call(auth, field)) delete merged[field];
            }
            Object.assign(merged, auth);
            delete merged.cacheUnverified;
            for (const optional of ['cupomCode', 'downloadUrl']) {
              if (!merged[optional] && (safe[optional] || old[optional])) merged[optional] = safe[optional] || old[optional];
            }
          }
          records.set(key, merged);
        });
      };
      add(bundled, 'bundle');
      if (typeof settings !== 'undefined') add(settings.realRedemptions, 'settings');
      try { add(JSON.parse(localStorage.getItem('uol_real_redemptions') || '[]'), 'storage'); } catch (e) {}
      try { add(JSON.parse(localStorage.getItem('uol_real_redemptions_v2') || '[]'), 'storage-v2'); } catch (e) {}
      if (typeof settings !== 'undefined') add(settings.myRedemptions, 'myRedemptions');
      add(liveVerified, 'native');
      return Array.from(records.values());
    }
    // REDEMPTION_IDENTITY_END

    function cleanTextForMatch(str) {
      if (!str) return '';
      return String(str)
        .toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/\b(?:2\s*ingressos|par\s*de\s*ingressos|ingresso|show|turne|parque|nubank|espetaculo|teatro)\b/gi, '')
        .replace(/[^a-z0-9]/gi, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    }

    function getRedeemedUsersForOffer(offer) {
      if (!offer) return [];
      const matchedUsers = new Set();
      const offerCampaign = catalogCampaign(offer.link || offer.url);
      const benefitId = value => {
        try {
          const url = new URL(value);
          if (url.protocol !== 'https:' || url.hostname !== 'clube.uol.com.br' || url.username || url.password || url.port) return '';
          return (url.pathname.match(/^\/beneficios\/(\d+)(?:\/|$)/) || [])[1] || '';
        } catch (e) { return ''; }
      };
      const offerBenefit = benefitId(offer.link || offer.url);
      getAllRedemptionsList().forEach(record => {
        const account = String(record.user || record.accountId || '').trim().toLowerCase();
        if (!account) return;
        const links = [record.campaignUrl, record.link, record.url, record.downloadUrl].filter(Boolean);
        const matched = links.some(link => {
          const campaign = catalogCampaign(link);
          return (offerCampaign && campaign && offerCampaign.key === campaign.key) ||
            (offerBenefit && benefitId(link) === offerBenefit);
        });
        if (matched) matchedUsers.add(account);
      });
      return Array.from(matchedUsers);
    }

    function getRedeemedUsersForGroup(group) {
      if (!group) return [];
      const allItems = [...(group.activeItems || []), ...(group.soldOutItems || [])];
      if (group.parent) allItems.push(group.parent);
      const userSet = new Set();
      allItems.forEach(item => {
        const users = getRedeemedUsersForOffer(item);
        users.forEach(u => userSet.add(u));
      });
      return Array.from(userSet);
    }

    function renderRedeemedFriendsPill(users) {
      if (!users || users.length === 0) return '';
      const hasLeo = users.includes('leosaquetto');
      const hasGabriel = users.includes('gaoriel');
      if (!hasLeo && !hasGabriel) return '';

      let text = '';
      let avatarsHtml = '';

      if (hasLeo && hasGabriel) {
        text = 'Resgatado por Leo e Gabriel';
        avatarsHtml = `
          <img class="friend-avatar" src="${AVATAR_URLS.leosaquetto}" alt="Leo" onerror="this.src='https://i.imgur.com/UdIgTfI.png'" />
          <img class="friend-avatar overlap" src="${AVATAR_URLS.gaoriel}" alt="Gabriel" onerror="this.src='https://i.imgur.com/UdIgTfI.png'" />
        `;
      } else if (hasLeo) {
        text = 'Resgatado por Leo';
        avatarsHtml = `
          <img class="friend-avatar" src="${AVATAR_URLS.leosaquetto}" alt="Leo" onerror="this.src='https://i.imgur.com/UdIgTfI.png'" />
        `;
      } else {
        text = 'Resgatado por Gabriel';
        avatarsHtml = `
          <img class="friend-avatar" src="${AVATAR_URLS.gaoriel}" alt="Gabriel" onerror="this.src='https://i.imgur.com/UdIgTfI.png'" />
        `;
      }

      return `
        <div class="card-redeemed-friends">
          <div class="redeemed-avatars-stack">
            ${avatarsHtml}
          </div>
          <span class="friend-redeemed-text">${text}</span>
        </div>
      `;
    }

    function extractSessionDate(item) {
      if (!item) return null;
      const title = item.rawTitle || item.title || '';
      const desc = [item.description, item.validity].filter(value => typeof value === 'string').join(' ');

      const monthsMap = {
        jan: '01', fev: '02', mar: '03', abr: '04', mai: '05', jun: '06',
        jul: '07', ago: '08', set: '09', out: '10', nov: '11', dez: '12'
      };
      const monthNames = {
        '01': 'JAN', '02': 'FEV', '03': 'MAR', '04': 'ABR',
        '05': 'MAI', '06': 'JUN', '07': 'JUL', '08': 'AGO',
        '09': 'SET', '10': 'OUT', '11': 'NOV', '12': 'DEZ'
      };

      // 1. Data explícita no título primeiro (prioridade para sessões pontuais de shows/eventos)
      // Ex: "2 INGRESSOS: 10/10 Nubank Parque SP" -> 10/10 -> 10 OUT
      const mTitleRange = title.match(/(?:at[eé]|v[aá]lido\s+at[eé]|temporada\s+at[eé]|t[eé]rmino\s+em|\ba)\s+(\d{1,2}\/\d{1,2})\b/i);
      if (mTitleRange) {
        const parts = mTitleRange[1].split('/');
        const mon = monthNames[parts[1].padStart(2, '0')] || parts[1];
        return 'ATÉ ' + parseInt(parts[0], 10) + ' ' + mon;
      }

      const mTitle = title.match(/\b(\d{1,2}\/\d{1,2})\b/);
      if (mTitle) return mTitle[1];

      const mTitleNamed = title.match(/\b(\d{1,2})\s+de\s+(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-z]*\b/i);
      if (mTitleNamed) {
        const mon = monthsMap[mTitleNamed[2].toLowerCase().slice(0, 3)] || '10';
        return mTitleNamed[1].padStart(2, '0') + '/' + mon;
      }

      // 2. Temporada / Exposição / Período na descrição (limpando avisos de recebimento por email "enviado até...")
      const cleanDesc = desc.replace(/(?:enviad[oa]s?|resgate|recebimento)\s+at[eé][^.\n]*/gi, '');
      const mRange = cleanDesc.match(/(?:at[eé]|v[aá]lido\s+at[eé]|temporada\s+at[eé]|t[eé]rmino\s+em|\ba)\s+(\d{1,2}\/\d{1,2})\b/i);
      if (mRange) {
        const parts = mRange[1].split('/');
        const mon = monthNames[parts[1].padStart(2, '0')] || parts[1];
        return 'ATÉ ' + parseInt(parts[0], 10) + ' ' + mon;
      }
      const mRangeNamed = cleanDesc.match(/(?:at[eé]|v[aá]lido\s+at[eé]|temporada\s+at[eé]|\ba)\s+(\d{1,2})\s+de\s+(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-z]*/i);
      if (mRangeNamed) {
        const monNum = monthsMap[mRangeNamed[2].toLowerCase().slice(0, 3)] || '10';
        const mon = monthNames[monNum] || monNum;
        return 'ATÉ ' + parseInt(mRangeNamed[1], 10) + ' ' + mon;
      }

      // 3. Data na descrição (se não tiver no título)
      const mDesc = cleanDesc.match(/\b(\d{1,2}\/\d{1,2})\b/);
      if (mDesc) return mDesc[1];

      const mDescNamed = cleanDesc.match(/\b(\d{1,2})\s+de\s+(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-z]*\b/i);
      if (mDescNamed) {
        const mon = monthsMap[mDescNamed[2].toLowerCase().slice(0, 3)] || '10';
        return mDescNamed[1].padStart(2, '0') + '/' + mon;
      }

      return null;
    }

    function extractStateCode(item, norm) {
      const text = [
        (item && item.location_summary) || '',
        (norm && norm.venue) || '',
        (item && (item.rawTitle || item.title)) || '',
        (item && item.description) || '',
        (item && item.partner) || ''
      ].join(' ');

      const stateMatch = text.match(/(?:[-•/,\s]|^)(SP|RJ|BA|MG|RS|PR|SC|DF|PE|CE|GO|ES)(?:[-•/,\s]|$)/i);
      if (stateMatch) {
        return stateMatch[1].toUpperCase();
      }

      const lower = text.toLowerCase();
      if (lower.includes('são paulo') || lower.includes('sao paulo') || lower.includes('anhembi') || lower.includes('morumbi') || lower.includes('ibirapuera') || lower.includes('itaim') || lower.includes('paulista') || lower.includes('tuca')) {
        return 'SP';
      }
      if (lower.includes('rio de janeiro') || lower.includes('qualistage') || lower.includes('copacabana') || lower.includes('maracanã') || lower.includes('maracana') || lower.includes('barra da tijuca')) {
        return 'RJ';
      }
      if (lower.includes('salvador') || lower.includes('bahia')) {
        return 'BA';
      }
      if (lower.includes('belo horizonte') || lower.includes('minas gerais')) {
        return 'MG';
      }
      if (lower.includes('curitiba') || lower.includes('paraná') || lower.includes('parana')) {
        return 'PR';
      }
      if (lower.includes('porto alegre') || lower.includes('rio grande do sul')) {
        return 'RS';
      }
      if (lower.includes('brasília') || lower.includes('brasilia') || lower.includes('distrito federal')) {
        return 'DF';
      }

      return 'SP';
    }

    function getGroupKey(o) {
      const clean = String(o.rawTitle || o.title || '').slice(0, 280).toLowerCase()
        .replace(/^\d+\s*ingressos?\s*:?\s*/i, '')
        .replace(/\b\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\b/g, '')
        .replace(/\s+/g, ' ')
        .trim();
      if (validOfferImage(o.imageUrl)) {
        const baseImg = o.imageUrl.split('?')[0].split('/').pop().split('_').pop();
        // The same poster can advertise distinct events or cities.
        if (baseImg && baseImg.length > 8 && clean) return 'img:' + baseImg + '|title:' + clean;
      }
      return 'title:' + clean;
    }

    function getCleanParentTitle(rawTitle) {
      return toSentenceCase(
        (rawTitle || '')
          .replace(/^2\s*ingressos\s*:\s*/i, '')
          .replace(/\b\d{1,2}\/\d{1,2}\b/g, '')
          .replace(/\s+/g, ' ')
          .trim()
      );
    }

    function getSessionDateLabel(title) {
      const dateMatch = (title || '').match(/\b\d{1,2}\/\d{1,2}\b/);
      if (dateMatch) return 'Sessão ' + dateMatch[0];
      return toSentenceCase(title);
    }

    function renderIndividualOffers(list, container, unavailableItems = []) {
      const now = Date.now();
      let viewCounts = {};
      try {
        viewCounts = JSON.parse(localStorage.getItem('uol_offer_view_counts') || '{}');
      } catch(e) {}

      const ordered = [...list].sort((a, b) => {
        const highlight = Number(checkHighlight(b.title, b.id, b.description)) - Number(checkHighlight(a.title, a.id, a.description));
        return highlight || (captureTime(b) - captureTime(a)) || String(a.id || '').localeCompare(String(b.id || ''));
      });
      ordered.forEach((o, index) => {
        const isHighlight = checkHighlight(o.title, o.id, o.description);
        const views = viewCounts[o.id] != null ? viewCounts[o.id] : (o.viewsCount || 1);
        const capturedAt = captureTime(o);
        const isWithinTime = capturedAt > 0 && now >= capturedAt && now - capturedAt < (48 * 60 * 60 * 1000);
        const isNew = isWithinTime && (views <= 2);

        const card = document.createElement('div');
        card.className = 'card' + (isHighlight ? ' card-highlight' : '');
        card.style.setProperty('--delay', index);

        let newTagHtml = '';
        if (isNew) {
          newTagHtml = '<span class="card-tag tag-new"><svg viewBox="0 0 24 24" width="8.5" height="8.5" fill="currentColor"><path d="M12 0C12 6.627 6.627 12 0 12c6.627 0 12 5.373 12 12 0-6.627 5.373-12 12-12-6.627 0-12-5.373-12-12z"/></svg> NOVA</span>';
        }

        const relativeTimeStr = capturedAt ? formatRelativeTime(capturedAt, now) : 'Data não confirmada';
        // Tag de tempo alinhada à direita do card
        const timeTagHtml = '<span class="card-tag tag-age" style="margin-left: auto;">' + relativeTimeStr + '</span>';

        const imageMarkup = posterImageMarkup(o);
        const codeBadge = o.code ? '<div class="thumb-code-badge">' + htmlAttribute(o.code) + '</div>' : '';
        const displayTitle = o.rawTitle || o.title;

        // Se estiver no portal E stories, mostra as duas tags juntas!
        const isStoriesOnly = isStoriesOnlyOffer(o);
        const isStory = isStoriesOnly || Boolean(o.hasStory || o.source === 'Stories' || o.badge === 'Stories');
        const isPortal = isStoriesOnly ? false : (o.isPortal !== false);

        // Micro ícones de fontes sobrepostos (estilo citação de fontes Codex / Perplexity) ao lado da categoria
        let sourcesBadgeHtml = '';
        if (isPortal && isStory) {
          sourcesBadgeHtml = '<div class="source-stack" title="Disponível no Portal e Stories"><div class="source-mini" style="z-index: 2;">' + ICONS.uolColor + '</div><div class="source-mini mini-insta overlap" style="z-index: 1;">' + ICONS.instagram + '</div></div>';
        } else if (isPortal) {
          sourcesBadgeHtml = '<div class="source-stack" title="Disponível no Portal"><div class="source-mini">' + ICONS.uolColor + '</div></div>';
        } else if (isStory) {
          sourcesBadgeHtml = '<div class="source-stack" title="Disponível nos Stories"><div class="source-mini mini-insta">' + ICONS.instagram + '</div></div>';
        }

        // Tag de categoria com ícone
        const catKey = CATEGORY_MAP[o.category] ? o.category : 'OUTROS';
        const catDef = CATEGORY_MAP[catKey];
        const catTagHtml = '<span class="card-tag tag-category tag-cat-' + catKey + '">' + catDef.icon + ' ' + catDef.label + '</span>';

        // Amigos que resgataram
        const redeemedUsers = getRedeemedUsersForOffer(o);
        const redeemedFriendsHtml = renderRedeemedFriendsPill(redeemedUsers);

        card.innerHTML = `
          <div class="thumb-wrap">
            ${imageMarkup}
            ${codeBadge}
          </div>
          <div class="card-content">
            <div class="tag-row">
              ${catTagHtml}
              ${sourcesBadgeHtml}
              ${newTagHtml}
              ${timeTagHtml}
            </div>
            <div class="card-title">${htmlAttribute(displayTitle)}</div>
            ${redeemedFriendsHtml}
            <div class="card-actions-row">
              <button class="btn-action-small btn-details" type="button">
                ${ICONS.notes} Detalhes
              </button>
              <button class="btn-action-small btn-redeem" type="button">
                ${ICONS.tickets} Resgatar
              </button>
            </div>
          </div>
        `;
        card.querySelector?.('.btn-details')?.addEventListener?.('click', () => openDetails(o.id));
        card.querySelector?.('.btn-redeem')?.addEventListener?.('click', () => openConfirm(o.id));
        wirePosterImages(card);

        container.appendChild(card);
      });

      // 2. Por padrão, exibe também ofertas esgotadas nas últimas 24 horas (agrupadas ao final)
      const recentSoldOut = homeOfferSets(list, historyData.history, now).soldOutItems
        .filter(o => !activeFilter || o.category === activeFilter)
        .sort((a, b) => confirmedSoldOutTime(b) - confirmedSoldOutTime(a));

      if (recentSoldOut.length > 0) {
        const divEl = document.createElement('div');
        divEl.className = 'soldout-24h-divider';
        divEl.innerHTML = `
          <div class="soldout-24h-title">
            <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
            <span>Esgotadas nas últimas 24h</span>
          </div>
          <span class="soldout-24h-badge">${recentSoldOut.length} ${recentSoldOut.length === 1 ? 'oferta' : 'ofertas'}</span>
        `;
        container.appendChild(divEl);

        recentSoldOut.forEach((o, index) => {
          const card = document.createElement('div');
          card.className = 'card card-expired';
          card.style.setProperty('--delay', ordered.length + index);

          const expTimeStr = formatExpiredAgo(confirmedSoldOutTime(o), now);
          const timeTagHtml = '<span class="card-tag tag-expired-age" style="margin-left: auto;">' + expTimeStr + '</span>';

          const imageMarkup = posterImageMarkup(o);
          const codeBadge = o.code ? '<div class="thumb-code-badge">' + htmlAttribute(o.code) + '</div>' : '';
          const displayTitle = o.rawTitle || o.title;

          const isStoriesOnly = isStoriesOnlyOffer(o);
          const isStory = isStoriesOnly || Boolean(o.hasStory || o.source === 'Stories' || o.badge === 'Stories');
          const isPortal = isStoriesOnly ? false : (o.isPortal !== false);

          let sourcesBadgeHtml = '';
          if (isPortal && isStory) {
            sourcesBadgeHtml = '<div class="source-stack" title="Portal e Stories"><div class="source-mini">' + ICONS.uolColor + '</div><div class="source-mini mini-insta overlap">' + ICONS.instagram + '</div></div>';
          } else if (isPortal) {
            sourcesBadgeHtml = '<div class="source-stack" title="Portal"><div class="source-mini">' + ICONS.uolColor + '</div></div>';
          } else if (isStory) {
            sourcesBadgeHtml = '<div class="source-stack" title="Stories"><div class="source-mini mini-insta">' + ICONS.instagram + '</div></div>';
          }

          const catKey = CATEGORY_MAP[o.category] ? o.category : 'OUTROS';
          const catDef = CATEGORY_MAP[catKey];
          const catTagHtml = '<span class="card-tag tag-category tag-cat-' + catKey + '">' + catDef.icon + ' ' + catDef.label + '</span>';
          const expiredTagHtml = '<span class="card-tag" style="background: rgba(248, 113, 113, 0.14); color: #f87171; border: 1px solid rgba(248, 113, 113, 0.35);">Esgotado</span>';

          const redeemedUsers = getRedeemedUsersForOffer(o);
          const redeemedFriendsHtml = renderRedeemedFriendsPill(redeemedUsers);

          card.innerHTML = `
            <div class="thumb-wrap">
              ${imageMarkup}
              ${codeBadge}
            </div>
            <div class="card-content">
              <div class="tag-row">
                ${catTagHtml}
                ${sourcesBadgeHtml}
                ${expiredTagHtml}
                ${timeTagHtml}
              </div>
              <div class="card-title" style="color: #94a3b8;">${htmlAttribute(displayTitle)}</div>
              ${redeemedFriendsHtml}
              <div class="card-actions-row">
                <button class="btn-action-small btn-details" type="button">
                  ${ICONS.notes} Detalhes
                </button>
                <button class="btn-action-small" disabled style="opacity: 0.55; cursor: not-allowed; background: rgba(248, 113, 113, 0.12); color: #f87171; border: 1px solid rgba(248, 113, 113, 0.25);">
                  ${ICONS.expired || ''} Esgotado
                </button>
              </div>
            </div>
          `;
          card.querySelector?.('.btn-details')?.addEventListener?.('click', () => openDetails(o.id));
          wirePosterImages(card);

          container.appendChild(card);
        });
      }
      if (unavailableItems.length) {
        const heading = document.createElement('div');
        heading.className = 'soldout-24h-divider';
        heading.textContent = 'Links indisponíveis (' + unavailableItems.length + ')';
        container.appendChild(heading);
        unavailableItems.forEach((o, index) => {
          const card = document.createElement('div');
          card.className = 'card card-expired';
          card.style.setProperty('--delay', ordered.length + recentSoldOut.length + index);
          card.innerHTML = `<div class="thumb-wrap">${posterImageMarkup(o)}</div><div class="card-content">
            <div class="tag-row"><span class="card-tag tag-expired-age">Link indisponível</span></div>
            <div class="card-title">${htmlAttribute(o.rawTitle || o.title || '')}</div>
            <div class="card-actions-row"><button class="btn-action-small btn-details" type="button">${ICONS.notes} Detalhes</button>
            <button class="btn-action-small" type="button" disabled>Indisponível</button></div></div>`;
          card.querySelector?.('.btn-details')?.addEventListener?.('click', () => openDetails(o.id));
          wirePosterImages(card);
          container.appendChild(card);
        });
      }
    }

    // ===================================================================
    // NORMALIZAÇÃO INTELIGENTE DE TÍTULO DIRETO DO CORPO DA OFERTA (100% DINÂMICO)
    // ===================================================================
    function extractSmartTitleFromBody(desc) {
      if (!desc || typeof desc !== "string") return null;
      // Campaign prose comes with Markdown; legal terms after the lead are not event names.
      const lead = desc.slice(0, 5000)
        .split(/\n\s*(?:REGRAS? DE RESGATE|RECEBIMENTO DOS INGRESSOS|TERMOS E CONDIÇÕES|COMO RESGATAR)\b/i)[0]
        .replace(/<[^>]*>/g, ' ').replace(/\*\*|__|[`]/g, '').replace(/\r/g, '');
      const trustworthy = candidate => {
        const value = String(candidate || '').replace(/\s+/g, ' ').trim();
        const lower = value.toLocaleLowerCase('pt-BR');
        return value.length >= 3 && value.length <= 70 &&
          !/\b(?:qualquer evento|m[eê]s vigente|assinante|ingressos?|regras? de resgate|tempo limitado|promo[çc][ãa]o v[aá]lida)\b/i.test(lower) &&
          !/^(?:show|espet[aá]culo|evento|sess[aã]o|teatro|festival)$/i.test(value);
      };

      // 1. Títulos entre aspas no corpo (padrão oficial de redação das campanhas do Clube UOL)
      for (const qOpen of ['“', '"', '«']) {
        const qClose = qOpen === '“' ? '”' : (qOpen === '«' ? '»' : '"');
        const startIdx = lead.indexOf(qOpen);
        if (startIdx !== -1) {
          const endIdx = lead.indexOf(qClose, startIdx + 1);
          if (endIdx !== -1 && (endIdx - startIdx) >= 3 && (endIdx - startIdx) <= 80) {
            const candidate = lead.substring(startIdx + 1, endIdx).trim();
            const low = candidate.toLowerCase();
            if (
              !low.includes('ingressos') &&
              !low.includes('assinante uol') &&
              !low.includes('regras de resgate') &&
              !low.includes('tempo limitado') &&
              !low.includes('abrir no safari') &&
              !low.includes('promoção válida') &&
              !low.includes('\n') &&
              trustworthy(candidate)
            ) {
              return formatProperTitle(candidate);
            }
          }
        }
      }

      // 2. Ações e palavras-chave estruturadas no corpo
      const lower = lead.toLowerCase();
      const triggers = [
        'resgate 1 par de ingressos para o premiado espetáculo ',
        'resgate 1 par de ingressos para o premiado musical ',
        'resgate 1 par de ingressos para o aclamado espetáculo ',
        'resgate 1 par de ingressos para a premiada peça ',
        'resgate 1 par de ingressos para a aclamada peça ',
        'resgate 1 par de ingressos para o espetáculo ',
        'resgate 1 par de ingressos para o musical ',
        'resgate 1 par de ingressos para a peça ',
        'resgate 1 par de ingressos para a comédia ',
        'resgate 1 par de ingressos para a exposição ',
        'resgate 1 par de ingressos para o festival ',
        'resgate 1 par de ingressos para o show de ',
        'resgate 1 par de ingressos para o show ',
        'resgate 1 par de ingressos para curtir a turnê ',
        'resgate 1 par de ingressos para curtir o show ',
        'resgate 1 par de ingressos para curtir ',
        'resgate 1 par de ingressos para assistir ao musical ',
        'resgate 1 par de ingressos para assistir ao espetáculo ',
        'resgate 1 par de ingressos para assistir ao ',
        'resgate 1 par de ingressos para assistir à peça ',
        'resgate 1 par de ingressos para assistir à ',
        'resgate 1 par de ingressos para assistir a ',
        'resgate 1 par de ingressos para o ',
        'resgate 1 par de ingressos para a ',
        'resgate 1 par de ingressos para ',
        'ingressos para o premiado espetáculo ',
        'ingressos para o aclamado espetáculo ',
        'ingressos para o musical ',
        'ingressos para o espetáculo ',
        'ingressos para a peça ',
        'ingressos para a comédia ',
        'ingressos para a exposição ',
        'ingressos para o show ',
        'ingressos para o ',
        'ingressos para a ',
        'ingressos para '
      ];

      for (const trig of triggers) {
        const idx = lower.indexOf(trig);
        if (idx !== -1) {
          const sub = lead.substring(idx + trig.length).trim();
          // Não corta em "em" (ex: "Uma Noite em Buenos Aires") nem em traço (ex: "Percy Jackson – O Ladrão...")
          // Para apenas em pontuação, "e aproveite", "com direito" ou transição explícita de local
          const stopMatch = sub.match(/[,.\n\r]|(\s+(?:e\s+aproveite|com\s+direito|v[aá]lido)\b)|(\s+(?:no|na|em|[-–—])\s+(?:teatro|pavilhao|pavilhão|centro|audit[oó]rio|espa[çc]o|arena|allianz|vibra|qualistage|morumbi|maracan[ãa]|parque|museu|sala)\b)/i);
          const stopIdx = stopMatch ? stopMatch.index : Math.min(sub.length, 70);
          const candidate = sub.substring(0, stopIdx).replace(/^["'“«]+|["'”»]+$/g, '').trim();
          const low = candidate.toLowerCase();
          if (
            trustworthy(candidate)
          ) {
            return formatProperTitle(candidate);
          }
        }
      }

      return null;
    }

    function formatProperTitle(str) {
      if (!str) return '';
      let clean = str.trim()
        .replace(/^["'“«](.*)["'”»]$/, '$1')
        .replace(/^(?:o\s+)?(?:premiado|aclamado|imperd[ií]vel)\s+(?:espet[aá]culo|musical|pe[çc]a|show)\s*(?:de|:|–|-)?\s*/i, '')
        .replace(/^(?:o\s+)?(?:espet[aá]culo|musical|pe[çc]a|show)\s*:\s*/i, '')
        .replace(/^(?:o\s+)?espet[aá]culo\s+(?:de\s+)?/i, '')
        .replace(/^(?:o\s+)?musical\s+(?:de\s+)?/i, '')
        .replace(/^(?:a\s+)?pe[çc]a\s+(?:de\s+)?/i, '')
        .trim();

      if (clean === clean.toUpperCase() && clean.length > 3) {
        return clean;
      }

      const preps = new Set(['de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas', 'e', 'com', 'por', 'para']);
      let words = clean.split(/\s+/);
      let res = words.map(function(w, i) {
        let lower = w.toLowerCase();
        if (i > 0 && preps.has(lower)) return lower;
        return w.charAt(0).toUpperCase() + w.slice(1);
      }).join(' ')
        .replace(/\buol\b/gi, 'UOL')
        .replace(/\bsp\b/gi, 'SP')
        .replace(/\brj\b/gi, 'RJ')
        .replace(/\bba\b/gi, 'BA')
        .replace(/\bpr\b/gi, 'PR')
        .replace(/\bmg\b/gi, 'MG');

      res = res.replace(/([–—\-:]\s*)([a-zà-ú])/gi, function(m, p1, p2) {
        return p1 + p2.toUpperCase();
      });
      return res;
    }

    function formatVenueString(str) {
      if (!str) return '';
      let clean = str
        .replace(/\s*[-•–—]+\s*(?:SP|RJ|BA|MG|PR|RS|SC|DF|PE|CE|GO|ES)\b/gi, '')
        .replace(/\s+(?:SP|RJ|BA|MG|PR|RS|SC|DF|PE|CE|GO|ES)\b/gi, '')
        .replace(/\s*[-•–—]+\s*$/g, '')
        .trim();
      const preps = new Set(['de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na', 'nos', 'nas', 'e', 'com', 'por']);
      let words = clean.toLowerCase().split(/\s+/);
      let cased = words.map(function(w, i) {
        if (i > 0 && preps.has(w)) return w;
        return w.charAt(0).toUpperCase() + w.slice(1);
      }).join(' ')
        .replace(/\buol\b/gi, 'UOL');
      return cased;
    }

    // UI_DATE_HELPERS_START
    const SAO_PAULO_DATE_FORMAT = new Intl.DateTimeFormat('en-GB', { timeZone: 'America/Sao_Paulo',
      year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
      second: '2-digit', hourCycle: 'h23' });
    function saoPauloParts(timestamp) {
      const parts = SAO_PAULO_DATE_FORMAT.formatToParts(new Date(timestamp));
      return Object.fromEntries(parts.filter(p => p.type !== 'literal').map(p => [p.type, Number(p.value)]));
    }

    function validCalendarDate(year, month, day) {
      const value = new Date(Date.UTC(year, month - 1, day));
      return year >= 1900 && year <= 2200 && value.getUTCFullYear() === year &&
        value.getUTCMonth() === month - 1 && value.getUTCDate() === day;
    }

    function sessionDateInfo(item, now = Date.now()) {
      const extracted = extractSessionDate(item);
      if (!extracted) return null;
      const months = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ'];
      const numeric = extracted.match(/^(\d{1,2})\/(\d{1,2})$/);
      const named = extracted.match(/^ATÉ (\d{1,2}) ([A-Z]{3})$/);
      if (!numeric && !named) return null;
      const day = Number((numeric || named)[1]);
      const month = numeric ? Number(numeric[2]) : months.indexOf(named[2]) + 1;
      const anchor = saoPauloParts(catalogTime(item.firstSeen) || catalogTime(item.lastSeen) || now);
      let year = anchor.year;
      const text = [item.rawTitle || item.title || '', item.description || '', item.validity || ''].join(' ');
      const explicit = Array.from(text.matchAll(/\b(\d{1,2})\/(\d{1,2})\/(\d{2}|\d{4})\b/g))
        .find(m => Number(m[1]) === day && Number(m[2]) === month);
      const explicitNamed = Array.from(text.matchAll(/\b(\d{1,2})\s+de\s+(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-z]*\s+(?:de\s+)?(\d{4})\b/gi))
        .find(m => Number(m[1]) === day && months.indexOf(m[2].toUpperCase()) + 1 === month);
      if (explicit || explicitNamed) {
        const writtenYear = (explicit || explicitNamed)[3];
        // Contemporary ticket dates use 20YY for two-digit years. Four-digit
        // years retain their literal value and still pass calendar validation.
        year = writtenYear.length === 2 ? 2000 + Number(writtenYear) : Number(writtenYear);
      }
      else if (anchor.month === 12 && month === 1) year += 1;
      else if (anchor.month === 1 && month === 12) year -= 1;
      if (!validCalendarDate(year, month, day)) return null;
      const timestamp = Date.UTC(year, month - 1, day);
      const today = saoPauloParts(now);
      const todayUtc = Date.UTC(today.year, today.month - 1, today.day);
      let label = day + ' ' + months[month - 1];
      if (year !== today.year) label += ' ' + year;
      if (named) label = 'ATÉ ' + label;
      else if (timestamp === todayUtc) label = 'HOJE';
      else if (timestamp === todayUtc + 86400000) label = 'AMANHÃ';
      return { key: year + '-' + String(month).padStart(2, '0') + '-' + String(day).padStart(2, '0'),
        timestamp: timestamp, label: label, year: year, month: month, day: day };
    }

    function groupSessionDates(activeItems, soldOutItems, now = Date.now()) {
      const dates = new Map();
      const add = (item, soldOut) => {
        const date = sessionDateInfo(item, now);
        if (!date) return;
        const old = dates.get(date.key);
        const unverified = item.cacheUnverified === true;
        if (!old || old.unverified && !unverified || old.unverified === unverified && old.soldOut && !soldOut) {
          dates.set(date.key, { ...date, soldOut: soldOut, unverified,
            unavailable: Boolean(item.linkUnavailableAt || linkUnavailableAt(item)), itemId: item.id });
        }
      };
      (soldOutItems || []).forEach(item => add(item, true));
      (activeItems || []).forEach(item => add(item, false));
      return Array.from(dates.values()).sort((a, b) => a.timestamp - b.timestamp);
    }

    function renderSessionDatePills(activeItems, soldOutItems) {
      return groupSessionDates(activeItems, soldOutItems).map(date =>
        `<button type="button" class="micro-date-pill${date.unverified ? ' pill-unverified' : date.soldOut ? ' pill-soldout' : ''}${date.label === 'HOJE' && !date.unverified && !date.soldOut ? ' pill-today' : ''}" data-offer-id="${htmlAttribute(date.itemId)}" aria-label="${htmlAttribute(date.label + (date.unverified ? ', não verificado' : date.unavailable ? ', indisponível' : date.soldOut ? ', esgotada' : ', disponível'))}">${ICONS.calendar}<span>${htmlAttribute(date.label + (date.unverified ? ' · Não verificado' : ''))}</span></button>`
      ).join('');
    }

    function dateStrip(datesHtml, compact = false) {
      const count = (datesHtml.match(/class="micro-date-pill(?:\s|")/g) || []).length;
      return datesHtml ? `<div class="micro-dates-inline${compact ? ' dates-compact' : ''}${count > 3 ? ' dates-scroll' : ''}" role="group" aria-label="Datas das sessões">${datesHtml}</div>` : '';
    }

    const historyDateScrollPositions = new Map();

    function wireDateStrips(card, history = false) {
      if (!card || typeof card.querySelectorAll !== 'function') return;
      card.querySelectorAll('.micro-dates-inline').forEach(strip => {
        for (const eventName of ['click', 'touchstart', 'touchmove', 'touchend']) {
          strip.addEventListener(eventName, event => event.stopPropagation());
        }
        strip.querySelectorAll('[data-offer-id]').forEach(button => {
          button.addEventListener('click', event => {
            event.stopPropagation();
            if (button.dataset.offerId) openDetails(button.dataset.offerId);
          });
        });
        if (history) {
          strip.dataset.historyEnd = 'true';
          const key = card.hunterHistoryDateKey;
          if (key) {
            for (const eventName of ['touchstart', 'pointerdown', 'wheel', 'keydown']) {
              strip.addEventListener(eventName, () => { strip.hunterHistoryUserScroll = true; });
            }
            strip.addEventListener('scroll', () => {
              if (!strip.hunterHistoryUserScroll) return;
              const max = Math.max(0, strip.scrollWidth - strip.clientWidth);
              if (!max) return;
              const left = Math.max(0, Math.min(max, Number(strip.scrollLeft) || 0));
              historyDateScrollPositions.set(key, { left, atEnd: max - left <= 2 });
            });
          }
        }
      });
    }

    function wirePosterImages(card) {
      if (!card || typeof card.querySelectorAll !== 'function') return;
      card.querySelectorAll('.poster-img').forEach(image => image.addEventListener('error', () => image.remove(), { once: true }));
    }

    function alignHistoryDateStrips(card) {
      if (!card || typeof card.querySelectorAll !== 'function') return;
      card.querySelectorAll('.micro-dates-inline[data-history-end="true"]').forEach(strip => {
        const align = () => {
          if (strip.hunterHistoryUserScroll) return;
          const max = Math.max(0, strip.scrollWidth - strip.clientWidth);
          const prior = historyDateScrollPositions.get(card.hunterHistoryDateKey);
          strip.scrollLeft = prior && !prior.atEnd ? Math.min(max, prior.left) : max;
        };
        if (typeof requestAnimationFrame === 'function') requestAnimationFrame(align);
        else align();
      });
    }

    function htmlAttribute(value) {
      return String(value || '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
    }

    function posterImageMarkup(items) {
      const image = offerImage(items);
      const placeholder = `<div class="poster-placeholder" aria-label="Imagem indisponível">${ICONS.tickets}</div>`;
      return placeholder + (image ? `<img class="poster-img" src="${htmlAttribute(image)}" alt="Pôster" loading="lazy" />` : '');
    }
    // UI_DATE_HELPERS_END

    function normalizeEventInfo(items) {
      let eventTitle = '';
      let venue = '';

      const parent = (items && items[0]) || {};

      // Se o item já tiver eventTitle e venue explícitos (ex: Curadoria)
      if (parent.eventTitle) {
        return {
          eventTitle: parent.eventTitle,
          venue: formatVenueString(parent.venue || 'Nubank Parque SP')
        };
      }

      // 1. Extração prioritária e inteligente direta do corpo da oferta (description)
      // NUNCA amarra teatro a nome de peça - se houver outra peça no teatro, lê o corpo novo!
      if (Array.isArray(items)) {
        for (let i = 0; i < items.length; i++) {
          const desc = items[i] && items[i].description;
          if (desc && desc.length >= 20) {
            const extracted = extractSmartTitleFromBody(desc);
            if (extracted) {
              eventTitle = extracted;
              break;
            }
          }
        }
      }

      const raw = parent.rawTitle || parent.title || '';
      let cleanedRaw = raw
        .replace(/^2\s*ingressos\s*:\s*/i, '')
        .replace(/^[0-9]\s*ingressos?\s*:\s*/i, '')
        .replace(/\b\d{1,2}\/\d{1,2}\b/g, '')
        .replace(/\s+/g, ' ')
        .trim();

      // 2. Se ainda não há corpo em cache, identifica se o título original já traz o nome do evento
      if (!eventTitle) {
        // Procura separador de local no título (ex: "Percy Jackson... Teatro Claro SP" ou "Uma Noite em Buenos Aires Teatro Bradesco SP")
        const mVenue = cleanedRaw.match(/(?:no\s+|na\s+|em\s+|-\s*)?(?:teatro|pavilhao|pavilhão|centro|audit[oó]rio|espa[çc]o|arena|allianz|vibra|qualistage|morumbi|maracan[ãa]|parque|museu)[\s\S]*$/i);
        if (mVenue && mVenue.index > 3) {
          eventTitle = formatProperTitle(cleanedRaw.slice(0, mVenue.index).replace(/[-:]+$/, '').trim());
          cleanedRaw = mVenue[0].replace(/^(?:no|na|em|-)\s+/i, '').trim();
        } else if (cleanedRaw.includes(':')) {
          const parts = cleanedRaw.split(':');
          eventTitle = formatProperTitle(parts[0]);
          cleanedRaw = parts.slice(1).join(' ').trim();
        }
      }

      // 3. Montagem do Venue (Local/Teatro)
      if (eventTitle) {
        let venueCandidate = cleanedRaw;
        if (venueCandidate.toLowerCase().includes(eventTitle.toLowerCase())) {
          const literalTitle = eventTitle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          venueCandidate = venueCandidate.replace(new RegExp(literalTitle, 'gi'), '').replace(/^[\s\-:]+|[\s\-:]+$/g, '').trim();
        }
        if (!venueCandidate && parent.location_summary) {
          venueCandidate = parent.location_summary;
        }
        venue = formatVenueString(venueCandidate || parent.partner || 'Clube UOL');
      } else {
        // Fallback limpo: usa o próprio nome da atração/local do título formatado sem inventar peças antigas
        eventTitle = formatProperTitle(cleanedRaw);
        venue = formatVenueString(parent.location_summary || parent.partner || 'Clube UOL');
      }

      return { eventTitle: eventTitle, venue: venue };
    }

    // Busca assíncrona em segundo plano dos corpos das ofertas ativas que ainda não possuem descrição
    async function prefetchActiveOfferDescriptions(offers) {
      if (!offers || offers.length === 0) return;
      const missing = offers.filter(function(o) {
        return !o.description || o.description.length < 30;
      });
      if (missing.length === 0) return;

      let anyUpdated = false;
      for (let i = 0; i < missing.length; i += 3) {
        const chunk = missing.slice(i, i + 3);
        await Promise.allSettled(chunk.map(async function(o) {
          try {
            const res = await fetch(o.link);
            if (!res.ok) return;
            const html = await res.text();
            let desc = "";
            const mAssinante = html.match(/<p>\s*<p><strong>Assinante UOL[\s\S]*?<\/p>\s*<\/p>/i);
            if (mAssinante) {
              desc = mAssinante[0]
                .replace(/<\/p>\s*<p>/gi, "\n\n")
                .replace(/<br\s*\/?>/gi, "\n")
                .replace(/<[^>]+>/g, "")
                .replace(/&amp;/g, "&")
                .replace(/&quot;/g, '"')
                .replace(/&#34;/g, '"')
                .replace(/&#39;/g, "'")
                .replace(/&bull;/g, "•")
                .trim();
            } else {
              const mInfo = html.match(/class=["'][^"']*info-beneficio[^"']*["'][^>]*>([\s\S]*?)(?:<script|<footer)/i);
              if (mInfo) {
                desc = mInfo[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
              } else {
                const mMeta = html.match(/<meta\s+property="og:description"\s+content="([^"]+)"/i);
                if (mMeta) desc = mMeta[1].trim();
              }
            }
            if (desc && desc.length >= 25) {
              desc = desc.slice(0, 20000);
              o.description = desc;
              o.category = classifyCategory(o.rawTitle || o.title, o.link || o.id, desc);
              setCachedDescription(o.id, desc);
              anyUpdated = true;
            }
          } catch (err) {}
        }));
      }

      if (anyUpdated) {
        saveHistoryToStorage();
        renderOffers(historyData.active || []);
        updateCategoryCounts(historyData.active || []);
      }
    }

    // ===================================================================
    // CONSTRUÇÃO E AGRUPAMENTO DE OFERTAS (ATIVAS + ESGOTADAS)
    // ===================================================================
    function buildOfferGroups(activeList, historyList) {
      const groups = new Map();

      // 1. Processa ofertas ativas
      (activeList || []).forEach(o => {
        const key = getGroupKey(o);
        if (!groups.has(key)) {
          groups.set(key, {
            key: key,
            category: o.category || 'OUTROS',
            activeItems: [],
            soldOutItems: [],
            firstSeen: 0,
            latestCapturedAt: 0
          });
        }
        const g = groups.get(key);
        const unavailableAt = o.linkUnavailableAt || linkUnavailableAt(o);
        if (unavailableAt) g.soldOutItems.push({ ...o, linkUnavailableAt: unavailableAt, isExpired: false });
        else g.activeItems.push({ ...o, isExpired: false });
        if (o.category) g.category = o.category;
        const capturedAt = captureTime(o);
        if (capturedAt && (!g.firstSeen || capturedAt < g.firstSeen)) g.firstSeen = capturedAt;
        if (capturedAt > g.latestCapturedAt) g.latestCapturedAt = capturedAt;
      });

      // 2. Processa histórico (esgotadas)
      (historyList || []).forEach(o => {
        const key = getGroupKey(o);
        if (groups.has(key)) {
          const g = groups.get(key);
          const already = g.activeItems.some(x => x.id === o.id) || g.soldOutItems.some(x => x.id === o.id);
          if (!already) {
            g.soldOutItems.push({ ...o, isExpired: !o.linkUnavailableAt });
          }
        } else {
          groups.set(key, {
            key: key,
            category: o.category || 'OUTROS',
            activeItems: [],
            soldOutItems: [{ ...o, isExpired: !o.linkUnavailableAt }],
            firstSeen: 0,
            latestCapturedAt: 0
          });
        }
        const g = groups.get(key);
        const capturedAt = captureTime(o);
        if (capturedAt && (!g.firstSeen || capturedAt < g.firstSeen)) g.firstSeen = capturedAt;
        if (capturedAt > g.latestCapturedAt) g.latestCapturedAt = capturedAt;
      });

      const groupList = Array.from(groups.values());

      let viewCounts = {};
      try {
        viewCounts = JSON.parse(localStorage.getItem('uol_offer_view_counts') || '{}');
      } catch(e) {}
      const now = Date.now();

      groupList.forEach(g => {
        const allItems = [...g.activeItems, ...g.soldOutItems];
        g.allItems = allItems;
        g.parent = allItems[0];
        g.isHighlight = allItems.some(x => checkHighlight(x.title, x.id, x.description));
        g.hasNew = g.activeItems.some(x => {
          const v = viewCounts[x.id] != null ? viewCounts[x.id] : (x.viewsCount || 1);
          const capturedAt = captureTime(x);
          return capturedAt > 0 && now >= capturedAt && (now - capturedAt < 48 * 60 * 60 * 1000) && v <= 2;
        });
        g.isAllSoldOut = (g.activeItems.length === 0 && g.soldOutItems.length > 0);
        g.isAllUnavailable = g.isAllSoldOut && g.soldOutItems.every(x => x.linkUnavailableAt);
        const isStoriesOnly = allItems.length > 0 && allItems.every(x => isStoriesOnlyOffer(x));
        g.hasStory = isStoriesOnly || allItems.some(x => x.source === 'Stories' || x.hasStory || x.badge === 'Stories');
        g.hasPortal = isStoriesOnly ? false : allItems.some(x => x.isPortal !== false && !isStoriesOnlyOffer(x));
        g.norm = normalizeEventInfo(allItems);
      });

      return groupList;
    }

    // Mapa global dos grupos renderizados para abertura do modal interativo
    let currentRenderedGroups = new Map();

    function renderGroupedOffers(activeList, historyList, container) {
      container.innerHTML = '';
      currentRenderedGroups.clear();

      const allGroups = buildOfferGroups(activeList, historyList);
      allGroups.forEach(g => currentRenderedGroups.set(g.key, g));

      let filteredGroups = allGroups;
      if (activeFilter) {
        filteredGroups = allGroups.filter(g => g.category === activeFilter);
      }

      // 1. SEÇÃO DE DESTAQUES (Primeiro de tudo!)
      const highlightGroups = allGroups.filter(g => g.isHighlight);
      if (highlightGroups.length > 0 && (!activeFilter || highlightGroups.some(g => g.category === activeFilter))) {
        const hlToShow = activeFilter ? highlightGroups.filter(g => g.category === activeFilter) : highlightGroups;
        if (hlToShow.length > 0) {
          const destSection = createDestaqueHeroSection(hlToShow);
          container.appendChild(destSection);
        }
      }

      // 2. CATEGORIAS NORMAIS
      const categoryDefs = [
        { id: 'MUSEU', label: 'Museus', icon: ICONS.library },
        { id: 'TEATRO', label: 'Teatro', icon: ICONS.masksTheater },
        { id: 'MUSICA', label: 'Música & Shows', icon: ICONS.music },
        { id: 'FUTEBOL', label: 'Futebol', icon: CATEGORY_MAP.FUTEBOL.icon },
        { id: 'EXPOSICOES', label: 'Exposições', icon: ICONS.art },
        { id: 'OUTROS', label: 'Outros Benefícios', icon: ICONS.ticketText }
      ];

      categoryDefs.forEach(cat => {
        if (activeFilter && activeFilter !== cat.id) return;

        const catGroups = filteredGroups.filter(g => g.category === cat.id);
        if (catGroups.length === 0) return;

        // Ordenação dentro da categoria:
        // 1. Grupos com sessões disponíveis primeiro
        // 2. Grupos 100% esgotados POR ÚLTIMO em preto e branco
        catGroups.sort((a, b) => {
          if (a.isAllSoldOut !== b.isAllSoldOut) {
            return a.isAllSoldOut ? 1 : -1; // esgotados por último
          }
          if (a.isHighlight !== b.isHighlight) {
            return a.isHighlight ? -1 : 1;
          }
          if (a.hasNew !== b.hasNew) {
            return a.hasNew ? -1 : 1;
          }
          return (b.activeItems.length - a.activeItems.length);
        });

        const section = createCategoryCarousel(cat.id, cat.label, catGroups, false, cat.icon);
        container.appendChild(section);
      });
    }

    function createDestaqueHeroSection(groups) {
      const sec = document.createElement('div');
      sec.className = 'destaque-hero-section';
      sec.id = 'destaque_hero_section';

      const header = document.createElement('div');
      header.className = 'category-carousel-header';
      header.innerHTML = `
        <div class="category-carousel-title">
          <span>Destaques</span>
          <span class="cat-badge">${groups.length}</span>
        </div>
      `;
      sec.appendChild(header);

      const listContainer = document.createElement('div');
      listContainer.className = 'destaque-hero-container';

      groups.forEach((g, idx) => {
        const card = createPosterCard(g, idx, true, true);
        listContainer.appendChild(card);
      });

      sec.appendChild(listContainer);
      return sec;
    }

    function createCategoryCarousel(catId, catLabel, groups, isDestaque, catIconSvg) {
      const sec = document.createElement('div');
      sec.className = 'category-carousel-section';
      sec.id = 'cat_section_' + catId;

      const header = document.createElement('div');
      header.className = 'category-carousel-header';
      const badgeCount = `<span class="cat-badge">${groups.length}</span>`;

      header.innerHTML = `
        <div class="category-carousel-title">
          <span>${catLabel}</span>
          ${badgeCount}
        </div>
      `;
      sec.appendChild(header);

      const track = document.createElement('div');
      track.className = 'category-carousel-track';

      groups.forEach((g, idx) => {
        const card = createPosterCard(g, idx, isDestaque, false);
        track.appendChild(card);
      });

      sec.appendChild(track);
      return sec;
    }

    function createPosterCard(group, index, isDestaque, isHeroFullWidth) {
      const { activeItems, soldOutItems, norm, isHighlight, hasNew, isAllSoldOut, hasStory, hasPortal, parent } = group;
      const activeCount = activeItems.length;
      const soldOutCount = soldOutItems.length;
      const totalSessions = activeCount + soldOutCount;

      const card = document.createElement('div');
      let cardClass = 'poster-card';
      if (isHeroFullWidth) cardClass += ' destaque-hero-card';
      if (isHighlight || isDestaque) cardClass += ' card-highlight-poster';
      if (isAllSoldOut) cardClass += ' card-sold-out-grouped';
      card.className = cardClass;
      card.style.setProperty('--delay', index);

      // Top-left: Micro circular source icons + Relative time tag ("Há X tempo")
      let sourcesBadgeHtml = '';
      if (hasPortal && hasStory) {
        sourcesBadgeHtml = `
          <div class="source-circle-combo">
            <div class="source-mini-circle">${ICONS.uolColor}</div>
            <div class="source-mini-circle mini-insta overlap">${ICONS.instagram}</div>
          </div>
        `;
      } else if (hasPortal) {
        sourcesBadgeHtml = `<div class="source-circle-combo"><div class="source-mini-circle">${ICONS.uolColor}</div></div>`;
      } else if (hasStory) {
        sourcesBadgeHtml = `<div class="source-circle-combo"><div class="source-mini-circle mini-insta">${ICONS.instagram}</div></div>`;
      }

      const capturedAt = group.latestCapturedAt || captureTime(parent);
      const relativeTimeStr = capturedAt ? formatRelativeTime(capturedAt, Date.now()) : 'Data não confirmada';
      const timePillHtml = `<span class="poster-pill-badge">${relativeTimeStr}</span>`;

      // Top-right status pill
      let rightPillHtml = '';
      if (isHighlight || isDestaque) {
        if (!isHeroFullWidth) {
          rightPillHtml = `<span class="poster-status-pill pill-destaque pill-destaque-star-only" title="Destaque">${ICONS.star}</span>`;
        } else {
          rightPillHtml = `<span class="poster-status-pill pill-destaque">${ICONS.star} DESTAQUE</span>`;
        }
      } else {
        const stateCode = extractStateCode(parent, norm);
        rightPillHtml = `<span class="poster-status-pill pill-state">${htmlAttribute(stateCode)}</span>`;
      }

      // Container de datas micro inline [ 📅 DATA ]
      let microDatePillsHtml = renderSessionDatePills(activeItems, soldOutItems);

      if (!microDatePillsHtml) {
        if (isAllSoldOut) {
          microDatePillsHtml = `
            <span class="micro-date-pill pill-soldout">
              ${ICONS.calendar}
              <span>${group.isAllUnavailable ? 'Indisponível' : 'Esgotado'}</span>
            </span>
          `;
        } else {
          microDatePillsHtml = `
            <span class="micro-date-pill">
              ${ICONS.calendar}
              <span>${activeCount} ${activeCount === 1 ? 'sessão' : 'sessões'}</span>
            </span>
          `;
        }
      }


      // Action buttons: Apenas no Destaque (somente Resgatar alinhado à direita no rodapé).
      let actionButtonsHtml = '';
      if (isHeroFullWidth && activeCount > 0) {
        actionButtonsHtml = `
          <button class="btn-poster-redeem" type="button">
            ${ICONS.tickets} Resgatar
          </button>
        `;
      }

      card.addEventListener?.('click', () => {
        if (activeCount === 1) openDetails(activeItems[0].id);
        else openGroupSessionsModal(group.key);
      });

      const imageMarkup = posterImageMarkup([...activeItems, ...soldOutItems]);

      // Amigos que resgataram
      const redeemedUsers = getRedeemedUsersForGroup(group);
      const redeemedFriendsHtml = renderRedeemedFriendsPill(redeemedUsers);

      if (isHeroFullWidth) {
        card.innerHTML = `
          ${imageMarkup}
          <div class="poster-scrim"></div>
          <div class="poster-top-bar">
            <div style="display: flex; align-items: center; gap: 5px; flex-wrap: nowrap;">
              ${sourcesBadgeHtml}
              ${timePillHtml}
            </div>
            <div>${rightPillHtml}</div>
          </div>
          <div class="poster-bottom-content">
            <div class="poster-venue">${htmlAttribute(norm.venue || 'Clube UOL')}</div>
            <div class="destaque-bottom-row">
              <div class="destaque-hero-title">${htmlAttribute(norm.eventTitle)}</div>
              ${actionButtonsHtml}
            </div>
            ${dateStrip(microDatePillsHtml, true)}
            ${redeemedFriendsHtml}
          </div>
        `;
      } else {
        card.innerHTML = `
          ${imageMarkup}
          <div class="poster-scrim"></div>
          <div class="poster-top-bar">
            <div style="display: flex; align-items: center; gap: 5px; flex-wrap: nowrap; min-width: 0;">
              ${sourcesBadgeHtml}
              ${timePillHtml}
            </div>
            <div>${rightPillHtml}</div>
          </div>
          <div class="poster-bottom-content">
            <div class="poster-venue">${htmlAttribute(norm.venue || 'Clube UOL')}</div>
            <div class="poster-title">${htmlAttribute(norm.eventTitle)}</div>
            ${dateStrip(microDatePillsHtml)}
            ${redeemedFriendsHtml}
          </div>
        `;
      }

      card.querySelector?.('.btn-poster-redeem')?.addEventListener?.('click', event => {
        event.stopPropagation();
        if (activeCount === 1) openConfirm(activeItems[0].id);
        else openGroupSessionsModal(group.key);
      });
      wireDateStrips(card);
      wirePosterImages(card);

      return card;
    }

    // ===================================================================
    // MODAL DE SESSÕES DO GRUPO (SELEÇÃO DE DATA / SESSÃO)
    // ===================================================================
    function openGroupSessionsModal(groupKey) {
      const g = currentRenderedGroups.get(groupKey);
      if (!g) return;

      const modal = document.getElementById('groupSessionsModal');
      const thumbEl = document.getElementById('gsmThumb');
      const venueEl = document.getElementById('gsmVenue');
      const titleEl = document.getElementById('gsmTitle');
      const badgesEl = document.getElementById('gsmStatusBadges');
      const listEl = document.getElementById('gsmSessionsList');

      if (thumbEl) thumbEl.src = validOfferImage(g.parent && g.parent.imageUrl) || 'https://i.imgur.com/UdIgTfI.png';
      venueEl.textContent = g.norm.venue || 'CLUBE UOL';
      titleEl.textContent = g.norm.eventTitle;

      const activeCount = g.activeItems.length;
      const soldCount = g.soldOutItems.length;
      const totalCount = activeCount + soldCount;

      let badgesHtml = '';
      const catKey = CATEGORY_MAP[g.category] ? g.category : 'OUTROS';
      const catDef = CATEGORY_MAP[catKey];
      badgesHtml += '<span class="card-tag tag-category tag-cat-' + catKey + '">' + catDef.icon + ' ' + catDef.label + '</span>';

      const at = g.latestCapturedAt || captureTime(g.parent);
      const relTime = at ? formatRelativeTime(at, Date.now()) : 'Data não confirmada';
      badgesHtml += '<span class="card-tag tag-age">' + relTime + '</span>';

      if (activeCount > 0 && soldCount > 0) {
        badgesHtml += `<span class="poster-pill-badge">${activeCount} disponível de ${totalCount}</span>`;
      } else if (activeCount > 0) {
        badgesHtml += `<span class="poster-pill-badge">${activeCount} ${activeCount === 1 ? 'disponível' : 'disponíveis'}</span>`;
      } else {
        badgesHtml += `<span class="poster-pill-badge badge-expired">${g.isAllUnavailable ? 'Indisponível' : 'Esgotado'} (${soldCount})</span>`;
      }
      badgesEl.innerHTML = badgesHtml;

      let rowsHtml = '';
      // Sessões ativas primeiro
      g.activeItems.forEach(it => {
        const dateLabel = getSessionDateLabel(it.rawTitle || it.title);
        const itStory = it.source === 'Stories' || it.hasStory || it.badge === 'Stories';
        const itStoryBadge = itStory ? '<span class="card-tag tag-stories" style="font-size: 8.5px; padding: 2px 5px;">' + ICONS.instagram + ' Stories</span>' : '';
        rowsHtml += `
          <div class="session-item">
            <div class="session-info">
              <span class="card-tag tag-code" style="font-size: 9.5px; padding: 2px 6px;">[${htmlAttribute(it.code || '')}]</span>
              ${itStoryBadge}
              <div style="display: flex; flex-direction: column; gap: 1px;">
                <span style="font-weight: 700; font-size: 13px; color: #fff;">${htmlAttribute(dateLabel)}</span>
                <span style="font-size: 10.5px; color: #00e676; font-weight: 600;">● Disponível para resgate</span>
              </div>
            </div>
            <div class="session-btns">
              <button class="btn-mini btn-mini-details" type="button" data-session-action="details" data-offer-id="${htmlAttribute(it.id)}">${ICONS.notes} Detalhes</button>
              <button class="btn-mini btn-mini-redeem" type="button" data-session-action="redeem" data-offer-id="${htmlAttribute(it.id)}">${ICONS.tickets} Resgatar</button>
            </div>
          </div>
        `;
      });

      // Sessões esgotadas em seguida
      g.soldOutItems.forEach(it => {
        const dateLabel = getSessionDateLabel(it.rawTitle || it.title);
        const unavailable = Boolean(it.linkUnavailableAt);
        const itStory = it.source === 'Stories' || it.hasStory || it.badge === 'Stories';
        const itStoryBadge = itStory ? '<span class="card-tag tag-stories" style="font-size: 8.5px; padding: 2px 5px;">' + ICONS.instagram + ' Stories</span>' : '';
        rowsHtml += `
          <div class="session-item session-item-soldout">
            <div class="session-info">
              <span class="card-tag tag-code" style="font-size: 9.5px; padding: 2px 6px; opacity: 0.7;">[${htmlAttribute(it.code || '')}]</span>
              ${itStoryBadge}
              <div style="display: flex; flex-direction: column; gap: 1px;">
                <span style="font-weight: 700; font-size: 13px; color: #94a3b8; text-decoration: line-through;">${htmlAttribute(dateLabel)}</span>
                <span style="font-size: 10.5px; color: #f87171; font-weight: 600;">● ${unavailable ? 'Link indisponível' : 'Esgotado'}</span>
              </div>
            </div>
            <div class="session-btns">
              <button class="btn-mini btn-mini-details" type="button" data-session-action="details" data-offer-id="${htmlAttribute(it.id)}">${ICONS.notes} Detalhes</button>
              <button class="btn-mini btn-mini-expired" disabled style="opacity: 0.6; cursor: not-allowed; background: rgba(248, 113, 113, 0.15); color: #f87171;">${unavailable ? 'Indisponível' : 'Esgotado'}</button>
            </div>
          </div>
        `;
      });

      listEl.innerHTML = rowsHtml;
      listEl.querySelectorAll('[data-session-action]').forEach(button => {
        button.addEventListener('click', event => {
          event.stopPropagation();
          const id = button.dataset.offerId;
          if (button.dataset.sessionAction === 'details') openDetails(id);
          else proceedToRedeemSession(id);
        });
      });
      modal.style.display = 'flex';
    }

    function closeGroupSessionsModal(e) {
      if (e && e.stopPropagation) e.stopPropagation();
      const modal = document.getElementById('groupSessionsModal');
      if (modal) modal.style.display = 'none';
    }

    function proceedToRedeemSession(id) {
      closeGroupSessionsModal();
      openConfirm(id);
    }

    function deduplicateHistory(list) {
      const seen = new Map();
      const now = Date.now();
      const RETENTION_MS = 24 * 60 * 60 * 1000; // Mantém no histórico por pelo menos 24 horas

      (list || []).forEach(item => {
        const itemTime = item.expiredAt || item.lastSeen || item.firstSeen || now;
        if ((now - itemTime) > RETENTION_MS) return;

        const key = (item.code || '') + '|' + (item.rawTitle || item.title || '').trim().toLowerCase();
        if (!seen.has(key)) {
          seen.set(key, item);
        } else {
          const existing = seen.get(key);
          const earliestFirstSeen = Math.min(existing.firstSeen || now, item.firstSeen || now);
          const latestExpired = Math.max(existing.expiredAt || 0, item.expiredAt || 0);
          seen.set(key, {
            ...existing,
            ...item,
            firstSeen: earliestFirstSeen,
            expiredAt: latestExpired
          });
        }
      });
      return Array.from(seen.values());
    }

    // ===================================================================
    // NAVEGAÇÃO ENTRE ABAS (INÍCIO, INGRESSOS, HISTÓRICO, RESGATES)
    // ===================================================================
    function switchTab(tab, force = false) {
      if (!force && currentTab === tab) {
        window.scrollTo({ top: 0, behavior: 'smooth' });
        return;
      }
      currentTab = tab;
      settings.currentTab = tab;
      if (tab !== 'history' && historyRenderState && historyRenderState.observer) historyRenderState.observer.disconnect();

      // Atualiza botões do menu inferior flutuante
      document.querySelectorAll('#bottomNav .bottom-nav-item').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tab);
      });

      const mainView = document.getElementById('viewMain');
      const historyView = document.getElementById('viewHistory');
      const redemptionsView = document.getElementById('viewRedemptions');

      if (mainView) mainView.style.display = 'none';
      if (historyView) historyView.style.display = 'none';
      if (redemptionsView) redemptionsView.style.display = 'none';

      if (!force) {
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }

      if (tab === 'home') {
        isGrouped = true;
        settings.groupedView = true;
        if (activeFilter) {
          activeFilter = null;
          document.querySelectorAll('#categoryChips .chip').forEach(c => c.classList.remove('active'));
        }
        saveSettingsToStorage();
        if (mainView) mainView.style.display = 'block';
        renderOffers(historyData.active || []);
      } else if (tab === 'tickets') {
        isGrouped = false;
        settings.groupedView = false;
        saveSettingsToStorage();
        if (mainView) mainView.style.display = 'block';
        renderOffers(historyData.active || []);
      } else if (tab === 'history') {
        if (historyView) historyView.style.display = 'block';
        renderHistoryScreen();
      } else if (tab === 'redemptions') {
        if (redemptionsView) redemptionsView.style.display = 'block';
        renderRedemptionsScreen();
      }
    }

    // ===================================================================
    // CATÁLOGO HISTÓRICO & EDITOR'S CHOICE
    // ===================================================================
    const CURATED_EDITORS_CHOICE_SEEDS = [
      {
        id: "curated-zayn",
        title: "2 Ingressos: Zayn - The Konnakol Tour",
        eventTitle: "Zayn – The Konnakol Tour",
        rawTitle: "2 INGRESSOS: 10/10 Nubank Parque SP",
        imageUrl: "https://d310rdc9v8uuq.cloudfront.net/beneficios/cf34f716779818a54510b566c295f4ab.png",
        venue: "Nubank Parque SP",
        description: "Resgate 1 par de ingressos para curtir Zayn: The Konnakol Tour, o primeiro show solo de Zayn Malik no Brasil. Uma noite especial para cantar os maiores sucessos da carreira do artista e viver uma experiência inesquecível no Nubank Parque, em São Paulo.\n\nData do Show: 10 de Outubro de 2026.\nHorário: Abertura dos portões às 16h | Show principal às 20h.\n\nLocal: Nubank Parque - Rua Palestra Itália, 200, Água Branca, São Paulo - SP.\n\nRECEBIMENTO DOS INGRESSOS:\nSeu par de ingressos foi enviado para o e-mail cadastrado no UOL.",
        category: "MUSICA",
        firstSeen: 1791468540000,
        curatedTag: "★ Curadoria",
        source: "Stories",
        hasStory: true,
        isPortal: false,
        user: "leosaquetto",
        link: "https://clube.uol.com.br/perfil/beneficios/60874"
      },
      {
        id: "curated-maroon5",
        title: "2 Ingressos: Maroon 5 - Turnê Love Is Like World Tour",
        eventTitle: "Maroon 5 – Turnê Love Is Like World Tour",
        rawTitle: "2 INGRESSOS: 08/09 Nubank Parque SP",
        imageUrl: "https://d310rdc9v8uuq.cloudfront.net/beneficios/9109430f6cd5544f424923bd14eed21e.png",
        venue: "Nubank Parque SP",
        description: "Resgate 1 par de ingressos para curtir o Maroon 5, uma das bandas mais consagradas do pop rock mundial, ao vivo na turnê Love Is Like World Tour, em uma noite repleta de emoção, energia e sucessos que marcaram gerações, no Nubank Parque, em São Paulo.\n\nData: Abertura dos portões às 16h | Show às 20h30.\n\nLocal: Nubank Parque - Rua Palestra Itália, 200, Água Branca, São Paulo - SP.\n\nRECEBIMENTO DOS INGRESSOS:\nSeu par de ingressos foi enviado no seu e-mail cadastrado no UOL.",
        category: "MUSICA",
        firstSeen: 1788549900000,
        curatedTag: "★ Curadoria",
        source: "Stories",
        hasStory: true,
        isPortal: false,
        user: "leosaquetto",
        link: "https://clube.uol.com.br/perfil/beneficios/60466"
      },
      {
        id: "curated-c6fest",
        title: "2 Ingressos: C6 Fest 2026 Ibirapuera -SP",
        eventTitle: "C6 Fest 2026",
        rawTitle: "2 Ingressos: C6 Fest 2026 Ibirapuera -SP",
        imageUrl: "https://d310rdc9v8uuq.cloudfront.net/beneficios/52eb676188fed030309f65d94dc4e4a0.png",
        venue: "Parque Ibirapuera SP",
        description: "Festival internacional com grandes nomes do jazz contemporâneo, soul e indie rock.",
        category: "MUSICA",
        firstSeen: 1778250000000,
        curatedTag: "★ Curadoria",
        user: "gaoriel",
        link: "https://clube.uol.com.br/perfil/beneficios/58135"
      }
    ];

    const EDITORS_CHOICE_KEYWORDS = ['zayn', 'maroon 5', 'c6 fest', 'cirque du soleil', 'paul mccartney', 'coldplay', 'bruno mars'];

    const MONTH_FULL_NAMES = [
      'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
      'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
    ];

    const MONTH_SHORT_LABELS = [
      'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
      'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'
    ];

    function isEditorsChoice(o) {
      if (!o) return false;
      if (o.id && o.id.startsWith('curated-')) return true;
      const norm = normalizeEventInfo([o]);
      const str = ((norm.eventTitle || '') + ' ' + (o.rawTitle || o.title || '') + ' ' + (o.description || '')).toLowerCase();
      return EDITORS_CHOICE_KEYWORDS.some(k => str.includes(k));
    }

    function historyCurationText(offer) {
      return [offer.eventTitle, offer.rawTitle, offer.title, offer.description, offer.id, offer.link, offer.imageUrl]
        .filter(Boolean).join(' ').toLowerCase().replace(/maroon5/g, 'maroon 5').replace(/c6fest/g, 'c6 fest');
    }

    function getAllCatalogOffers() {
      historyDateCache = new WeakMap();
      const seen = new Map();
      (historyData.active || []).forEach(o => {
        const key = o.id || (o.rawTitle || o.title);
        if (key && !seen.has(key)) seen.set(key, o);
      });
      (historyData.history || []).forEach(o => {
        const key = o.id || (o.rawTitle || o.title);
        if (key && !seen.has(key)) seen.set(key, o);
      });

      (CURATED_EDITORS_CHOICE_SEEDS || []).forEach(seed => {
        const key = seed.id || (seed.rawTitle || seed.title);
        if (key && !seen.has(key)) {
          const seedKw = historyCurationText(seed);
          const covered = Array.from(seen.values()).some(existing => {
            const s = historyCurationText(existing);
            return EDITORS_CHOICE_KEYWORDS.some(k => seedKw.includes(k) && s.includes(k));
          });
          if (!covered) {
            seen.set(key, { ...seed, isCuratedSeed: true });
          }
        }
      });
      return Array.from(seen.values());
    }

    function getEditorsChoiceOffers(allOffers) {
      const matches = [];
      const seenKeys = new Set();

      // 1. Ofertas reais cadastradas que combinam com os termos de curadoria
      for (const kw of EDITORS_CHOICE_KEYWORDS) {
        for (const o of allOffers) {
          const key = o.id || (o.rawTitle || o.title);
          if (seenKeys.has(key)) continue;
          const str = historyCurationText(o);
          if (str.includes(kw)) {
            matches.push(o);
            seenKeys.add(key);
          }
        }
      }

      // Seeds already belong to allOffers. Include every other highlighted offer.
      for (const o of allOffers) {
        const key = o.id || (o.rawTitle || o.title);
        if (seenKeys.has(key)) continue;
        if (checkHighlight(o.title, o.id, o.description)) {
          matches.push(o);
          seenKeys.add(key);
        }
      }

      return matches;
    }

    let historyDateCache = new WeakMap();
    function getOfferYearAndMonth(o) {
      if (historyDateCache.has(o)) return historyDateCache.get(o);
      const capturedAt = catalogTime(o.latestCapturedAt) || captureTime(o);
      const parts = capturedAt ? saoPauloParts(capturedAt) : null;
      const result = parts ? { year: parts.year, month: parts.month - 1 } : { year: 'unknown', month: -1 };
      historyDateCache.set(o, result);
      return result;
    }

    function buildHistoryGroups(allOffers) {
      const observed = (allOffers || []).filter(o => o && !o.isCuratedSeed);
      const activeIds = new Set((historyData.active || []).map(o => o.id));
      const groups = buildOfferGroups(observed.filter(o => activeIds.has(o.id)), observed.filter(o => !activeIds.has(o.id)));
      return groups.sort((a, b) => (b.latestCapturedAt - a.latestCapturedAt) || String(a.key).localeCompare(String(b.key)));
    }

    let historySearchQuery = '';
    let historySearchTimer = null;
    function historyMatchesSearch(offer) {
      const normalize = value => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLocaleLowerCase('pt-BR');
      const query = normalize(historySearchQuery).trim();
      if (!query) return true;
      if (Array.isArray(offer.allItems)) return offer.allItems.some(historyMatchesSearch);
      const text = normalize([offer.title, offer.rawTitle, offer.partner, offer.venue, offer.location_summary].filter(Boolean).join(' '));
      return query.split(/\s+/).every(word => text.includes(word));
    }

    function setHistorySearch(value) {
      historySearchQuery = String(value || '').slice(0, 200);
      clearTimeout(historySearchTimer);
      historySearchTimer = setTimeout(() => {
        const allOffers = getAllCatalogOffers();
        const groups = buildHistoryGroups(allOffers);
        renderHistoryMonthSelector(selectedHistoryYear, groups);
        renderHistoryCurated(allOffers);
        renderHistoryMonths(groups, selectedHistoryYear, document.getElementById('historyMonthsContainer'));
      }, 180);
    }

    function renderHistoryScreen() {
      const container = document.getElementById('viewHistory');
      if (!container) return;

      const allOffers = getAllCatalogOffers();
      const historyGroups = buildHistoryGroups(allOffers);
      const today = saoPauloParts(Date.now());
      if (!Number.isInteger(selectedHistoryYear)) selectedHistoryYear = today.year;
      if (selectedHistoryMonth === null) selectedHistoryMonth = today.month - 1;

      // 1. Insights & Métricas
      const totalCount = historyGroups.length;

      // Categoria mais frequente
      const catCounts = { MUSEU: 0, TEATRO: 0, MUSICA: 0, FUTEBOL: 0, EXPOSICOES: 0, OUTROS: 0 };
      historyGroups.forEach(o => {
        if (catCounts[o.category] != null) catCounts[o.category]++;
        else catCounts.OUTROS++;
      });
      let topCatKey = 'TEATRO';
      let maxCatCount = -1;
      for (const [c, n] of Object.entries(catCounts)) {
        if (n > maxCatCount) {
          maxCatCount = n;
          topCatKey = c;
        }
      }
      const catLabels = {
        MUSEU: 'Museus',
        TEATRO: 'Teatro',
        MUSICA: 'Música & Shows',
        FUTEBOL: 'Futebol',
        EXPOSICOES: 'Exposições',
        OUTROS: 'Benefícios'
      };
      const topCatLabel = catLabels[topCatKey] || 'Teatro';

      // Mês recorde
      const monthBuckets = new Map();
      historyGroups.forEach(o => {
        const ym = getOfferYearAndMonth(o);
        if (ym.year === 'unknown') return;
        const k = ym.month + '-' + ym.year;
        monthBuckets.set(k, (monthBuckets.get(k) || 0) + 1);
      });
      let recordMonthLabel = 'Sem data';
      let recordMonthCount = 0;
      monthBuckets.forEach((cnt, k) => {
        if (cnt > recordMonthCount) {
          recordMonthCount = cnt;
          const [m, y] = k.split('-');
          recordMonthLabel = `${MONTH_SHORT_LABELS[parseInt(m, 10)]} ${y}`;
        }
      });
      if (recordMonthCount > 0) {
        recordMonthLabel += ` (${recordMonthCount})`;
      }

      // Anos únicos no catálogo
      let years = Array.from(new Set(historyGroups.map(o => getOfferYearAndMonth(o).year)))
        .sort((a, b) => a === 'unknown' ? 1 : b === 'unknown' ? -1 : b - a);
      if (!years.includes(selectedHistoryYear)) years.push(selectedHistoryYear);
      years.sort((a, b) => a === 'unknown' ? 1 : b === 'unknown' ? -1 : b - a);

      // Constrói HTML do topo e Editor's Choice
      container.innerHTML = `
        <div class="history-view-header">
          <div class="history-title-row">
            <span class="history-view-title">Catálogo Histórico</span>
            <span class="history-badge-pill">Arquivo Completo</span>
          </div>
          <div class="history-view-subtitle">Registro cronológico de todas as campanhas e ofertas já catalogadas pelo Clube UOL</div>
        </div>

        <div class="history-metrics-row">
          <div class="metric-pill">
            <div class="metric-label">TOTAL CATALOGADO</div>
            <div class="metric-val">${totalCount} eventos</div>
          </div>
          <div class="metric-pill">
            <div class="metric-label">CATEGORIA TOP</div>
            <div class="metric-val">${topCatLabel}</div>
          </div>
          <div class="metric-pill">
            <div class="metric-label">MÊS RECORDE</div>
            <div class="metric-val">${recordMonthLabel}</div>
          </div>
        </div>

        <!-- SEÇÃO EDITOR'S CHOICE -->
        <div class="category-carousel-section" id="sectionEditorsChoice" style="margin-bottom: 26px;">
          <div class="category-carousel-header">
            <div class="category-carousel-title">
              <span style="color: #ffb300;">★</span>
              <span>Editor’s Choice</span>
              <span class="cat-badge" style="background: rgba(255, 179, 0, 0.16); color: #ffb300; border-color: rgba(255, 179, 0, 0.35);">Curadoria</span>
            </div>
          </div>
          <div class="category-carousel-track" id="editorsChoiceTrack"></div>
        </div>

        <!-- SEÇÃO CRONOLÓGICA (LINHA DO TEMPO: ANO -> MÊS -> OFERTAS) -->
        <div class="history-timeline-section">
          <div class="category-carousel-header" style="margin-bottom: 12px;">
            <div class="category-carousel-title">
              <span>Linha do Tempo</span>
            </div>
          </div>

          <div class="history-year-selector" id="historyYearSelector">
            ${years.map(y => `
              <button class="year-chip ${y === selectedHistoryYear ? 'active' : ''}" data-year="${y}" onclick="selectHistoryYear(${y === 'unknown' ? "'unknown'" : y})">${y === 'unknown' ? 'Sem data' : y}</button>
            `).join('')}
          </div>

          <input type="search" class="form-input" id="historySearchInput" placeholder="Buscar evento ou local" aria-label="Buscar no histórico" value="${htmlAttribute(historySearchQuery)}" oninput="setHistorySearch(this.value)" style="margin-bottom: 12px;" />

          <div class="history-month-selector" id="historyMonthSelector"></div>

          <div id="historyMonthsContainer"></div>
        </div>
      `;

      renderHistoryMonthSelector(selectedHistoryYear, historyGroups);
      renderHistoryCurated(allOffers);
      renderHistoryMonths(historyGroups, selectedHistoryYear, document.getElementById('historyMonthsContainer'));
    }

    function renderHistoryCurated(allOffers) {
      const editorsChoice = getEditorsChoiceOffers(allOffers.filter(historyMatchesSearch));
      const edTrack = document.getElementById('editorsChoiceTrack');
      const section = document.getElementById('sectionEditorsChoice');
      if (section) section.style.display = editorsChoice.length ? 'block' : 'none';
      if (edTrack) {
        edTrack.innerHTML = '';
        editorsChoice.forEach((item, idx) => {
          const card = createHistoryPosterCard(item, idx, true);
          edTrack.appendChild(card);
          alignHistoryDateStrips(card);
        });
      }

    }

    function selectHistoryYear(year) {
      selectedHistoryYear = year;
      document.querySelectorAll('.year-chip').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.year === String(year));
      });
      const allOffers = getAllCatalogOffers();
      const groups = buildHistoryGroups(allOffers);
      const today = saoPauloParts(Date.now());
      const months = groups.filter(o => getOfferYearAndMonth(o).year === year).map(o => getOfferYearAndMonth(o).month);
      selectedHistoryMonth = year === 'unknown' ? 'all' : year === today.year || !months.length ? today.month - 1 : Math.max(...months);
      renderHistoryMonthSelector(year, groups);
      renderHistoryCurated(allOffers);
      renderHistoryMonths(groups, year, document.getElementById('historyMonthsContainer'));
    }

    function renderHistoryMonthSelector(year, groups) {
      const monthSelector = document.getElementById('historyMonthSelector');
      if (!monthSelector) return;

      const yearOffers = groups.filter(o => getOfferYearAndMonth(o).year === year && historyMatchesSearch(o));
      const monthsMap = new Map();
      yearOffers.forEach(o => {
        const m = getOfferYearAndMonth(o).month;
        if (!monthsMap.has(m)) monthsMap.set(m, []);
        monthsMap.get(m).push(o);
      });

      if (Number.isInteger(selectedHistoryMonth) && !monthsMap.has(selectedHistoryMonth)) monthsMap.set(selectedHistoryMonth, []);
      const sortedMonths = Array.from(monthsMap.keys()).sort((a, b) => b - a);
      monthSelector.style.display = 'flex';

      let totalGroupCount = 0;
      const monthGroupsCounts = new Map();
      sortedMonths.forEach(m => {
        const mOffers = monthsMap.get(m);
        const count = mOffers.length;
        monthGroupsCounts.set(m, count);
        totalGroupCount += count;
      });

      monthSelector.innerHTML = `
        <button class="month-chip ${selectedHistoryMonth === 'all' ? 'active' : ''}" onclick="selectHistoryMonth('all')">
          Todos (${totalGroupCount})
        </button>
        ${sortedMonths.map(m => `
          <button class="month-chip ${selectedHistoryMonth === m ? 'active' : ''}" onclick="selectHistoryMonth(${m})">
            ${m === -1 ? 'Sem data' : MONTH_SHORT_LABELS[m]} (${monthGroupsCounts.get(m)})
          </button>
        `).join('')}
      `;
    }

    function selectHistoryMonth(m) {
      selectedHistoryMonth = m;
      const allOffers = getAllCatalogOffers();
      const groups = buildHistoryGroups(allOffers);
      renderHistoryMonthSelector(selectedHistoryYear, groups);
      renderHistoryCurated(allOffers);
      renderHistoryMonths(groups, selectedHistoryYear, document.getElementById('historyMonthsContainer'));
    }

    function renderHistoryMonths(groups, year, container) {
      if (!container) return;
      if (historyRenderState && historyRenderState.observer) historyRenderState.observer.disconnect();
      container.innerHTML = '';
      historyRenderState = null;

      const yearOffers = groups.filter(o => {
        const period = getOfferYearAndMonth(o);
        return period.year === year && (selectedHistoryMonth === 'all' || period.month === selectedHistoryMonth) && historyMatchesSearch(o);
      });

      if (yearOffers.length === 0) {
        const period = year === 'unknown' ? 'data de captura desconhecida' :
          selectedHistoryMonth === 'all' ? String(year) : MONTH_FULL_NAMES[selectedHistoryMonth] + ' de ' + year;
        container.innerHTML = '<div style="color: #64748b; font-size: 13px; text-align: center; padding: 32px 0;">Nenhuma oferta encontrada para ' + period + '.</div>';
        return;
      }

      // Agrupa por mês (0 a 11)
      const monthsMap = new Map();
      yearOffers.forEach(o => {
        const m = getOfferYearAndMonth(o).month;
        if (!monthsMap.has(m)) monthsMap.set(m, []);
        monthsMap.get(m).push(o);
      });

      // Ordena meses do mais recente para o mais antigo (11 down to 0)
      let sortedMonths = Array.from(monthsMap.keys()).sort((a, b) => b - a);

      if (selectedHistoryMonth !== 'all') {
        sortedMonths = sortedMonths.filter(m => m === selectedHistoryMonth);
      }

      const rows = [];
      sortedMonths.forEach(m => {
        const monthGroups = monthsMap.get(m).sort((a, b) =>
          (b.latestCapturedAt - a.latestCapturedAt) || String(a.key).localeCompare(String(b.key)));
        monthGroups.forEach((group, index) => rows.push({ month: m, group: group, index: index, total: monthGroups.length }));
      });

      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'month-chip';
      button.onclick = appendHistoryBatch;
      historyRenderState = { container: container, rows: rows, cursor: 0, year: year, tracks: new Map(), button: button, observer: null };
      appendHistoryBatch();
      if (historyRenderState.cursor < rows.length && typeof IntersectionObserver === 'function') {
        const state = historyRenderState;
        state.observer = new IntersectionObserver(entries => {
          if (historyRenderState === state && currentTab === 'history' && entries.some(entry => entry.isIntersecting)) appendHistoryBatch();
        }, { rootMargin: '160px' });
        state.observer.observe(button);
      }
    }

    let historyRenderState = null;
    function appendHistoryBatch() {
      const state = historyRenderState;
      if (!state) return;
      const start = state.cursor;
      const end = Math.min(start + 12, state.rows.length);
      for (; state.cursor < end; state.cursor++) {
        const row = state.rows[state.cursor];
        let track = state.tracks.get(row.month);
        if (!track) {
          const sec = document.createElement('div');
          sec.className = 'category-carousel-section';
          sec.style.marginBottom = '22px';
          const header = document.createElement('div');
          header.className = 'category-carousel-header';
          header.innerHTML = `<div class="category-carousel-title"><span>${row.month === -1 ? 'Sem data de captura' : MONTH_FULL_NAMES[row.month] + ' ' + state.year}</span><span class="cat-badge">${row.total} ${row.total === 1 ? 'evento' : 'eventos'}</span></div>`;
          sec.appendChild(header);
          track = document.createElement('div');
          track.className = 'category-carousel-track';
          sec.appendChild(track);
          state.container.insertBefore(sec, state.button.parentNode === state.container ? state.button : null);
          state.tracks.set(row.month, track);
        }
        currentRenderedGroups.set(row.group.key, row.group);
        const card = createHistoryPosterCard(row.group, state.cursor - start, false);
        track.appendChild(card);
        alignHistoryDateStrips(card);
      }
      const remaining = state.rows.length - state.cursor;
      if (remaining) {
        state.button.textContent = 'Carregar mais (' + remaining + ' restantes)';
        if (state.button.parentNode !== state.container) state.container.appendChild(state.button);
      } else {
        if (state.button.parentNode === state.container) state.button.remove();
        if (state.observer) state.observer.disconnect();
      }
    }

    function createHistoryPosterCard(target, index, isCuratedSection = false) {
      const isGroup = Boolean(target && (target.activeItems || target.soldOutItems || target.allItems));
      const group = isGroup ? target : {
        key: getGroupKey(target),
        category: target.category || 'OUTROS',
        activeItems: (historyData.active || []).some(a => a.id === target.id) ? [target] : [],
        soldOutItems: !(historyData.active || []).some(a => a.id === target.id) ? [target] : [],
        allItems: [target],
        parent: target,
        isHighlight: checkHighlight(target.title, target.id, target.description),
        isAllSoldOut: !(historyData.active || []).some(a => a.id === target.id),
        hasStory: isStoriesOnlyOffer(target) || Boolean(target.hasStory || target.source === 'Stories' || target.badge === 'Stories'),
        hasPortal: isStoriesOnlyOffer(target) ? false : (target.isPortal !== false),
        firstSeen: captureTime(target),
        latestCapturedAt: captureTime(target),
        norm: normalizeEventInfo([target])
      };
      currentRenderedGroups.set(group.key, group);

      const { activeItems, soldOutItems, allItems, norm, isAllSoldOut, hasStory, hasPortal, parent } = group;
      const activeCount = (activeItems || []).length;
      const soldOutCount = (soldOutItems || []).length;
      const totalSessions = activeCount + soldOutCount;
      const hasUnverifiedSessions = (allItems || []).some(item => item.cacheUnverified === true);
      const allSessionsUnverified = totalSessions > 0 && (allItems || []).every(item => item.cacheUnverified === true);

      const card = document.createElement('div');
      card.hunterHistoryDateKey = (isCuratedSection ? 'curated:' : 'timeline:') + String(group.key);
      const curated = isEditorsChoice(parent) || (allItems || []).some(x => isEditorsChoice(x)) || isCuratedSection;
      let cardClass = 'poster-card history-clean-card';
      if (curated) cardClass += ' card-editors-choice-poster';
      if (isAllSoldOut && !hasUnverifiedSessions) cardClass += ' card-sold-out-grouped';
      card.className = cardClass;
      card.style.setProperty('--delay', index);

      // Micro ícones de fontes sobrepostos
      let sourcesBadgeHtml = '';
      if (hasPortal && hasStory) {
        sourcesBadgeHtml = `
          <div class="source-circle-combo">
            <div class="source-mini-circle">${ICONS.uolColor}</div>
            <div class="source-mini-circle mini-insta overlap">${ICONS.instagram}</div>
          </div>
        `;
      } else if (hasPortal) {
        sourcesBadgeHtml = `<div class="source-circle-combo"><div class="source-mini-circle">${ICONS.uolColor}</div></div>`;
      } else if (hasStory) {
        sourcesBadgeHtml = `<div class="source-circle-combo"><div class="source-mini-circle mini-insta">${ICONS.instagram}</div></div>`;
      }

      // Normalização semântica completa de título e local (idêntica à Home)
      const venueText = norm.venue || (parent && (parent.venue || parent.partner || parent.location_summary)) || 'Clube UOL';
      const eventTitle = norm.eventTitle || toSentenceCase(((parent && (parent.rawTitle || parent.title)) || '').replace(/^2\s*ingressos\s*:\s*/i, '').trim());

      const capturedAt = group.latestCapturedAt || captureTime(parent);
      const relTime = capturedAt ? formatRelativeTime(capturedAt, Date.now()) : 'Data não confirmada';
      const timePillHtml = `<span class="poster-pill-badge">${relTime}</span>`;

      // Status pill à direita: ★ Curadoria se for Editor's Choice, senão sigla do estado
      let rightPillHtml = '';
      if (allSessionsUnverified) {
        rightPillHtml = `<span class="poster-status-pill pill-state">Não verificado</span>`;
      } else if (curated) {
        rightPillHtml = `<span class="pill-editors-choice">★ Curadoria</span>`;
      } else {
        const stateCode = extractStateCode(parent, { venue: venueText });
        rightPillHtml = `<span class="poster-status-pill pill-state">${htmlAttribute(stateCode)}</span>`;
      }

      // Container de datas micro inline [ 📅 DATA ]
      let microDatePillsHtml = renderSessionDatePills(activeItems, soldOutItems);

      if (!microDatePillsHtml) {
        if (allSessionsUnverified) {
          microDatePillsHtml = `
            <span class="micro-date-pill pill-unverified">
              ${ICONS.calendar}
              <span>Não verificado</span>
            </span>
          `;
        } else if (isAllSoldOut) {
          microDatePillsHtml = `
            <span class="micro-date-pill pill-soldout">
              ${ICONS.calendar}
              <span>${group.isAllUnavailable ? 'Indisponível' : 'Esgotado'}</span>
            </span>
          `;
        } else if (activeCount > 0) {
          microDatePillsHtml = `
            <span class="micro-date-pill">
              ${ICONS.calendar}
              <span>${activeCount} ${activeCount === 1 ? 'sessão' : 'sessões'}</span>
            </span>
          `;
        }
      }

      const imageMarkup = posterImageMarkup([...(activeItems || []), ...(soldOutItems || [])]);

      // Amigos que resgataram
      const redeemedUsers = getRedeemedUsersForGroup(group);
      const redeemedFriendsHtml = renderRedeemedFriendsPill(redeemedUsers);

      if (totalSessions > 1) {
        card.onclick = () => openGroupSessionsModal(group.key);
      } else {
        const singleItem = (activeItems && activeItems[0]) || (soldOutItems && soldOutItems[0]) || parent;
        if (singleItem && singleItem.id) {
          card.onclick = () => openDetails(singleItem.id);
        }
      }

      card.innerHTML = `
        ${imageMarkup}
        <div class="poster-scrim"></div>
        <div class="poster-top-bar">
          <div style="display: flex; align-items: center; gap: 5px; flex-wrap: nowrap; min-width: 0;">
            ${sourcesBadgeHtml}
            ${timePillHtml}
          </div>
          <div>${rightPillHtml}</div>
        </div>
        <div class="poster-bottom-content">
          <div class="poster-venue">${htmlAttribute(venueText)}</div>
          <div class="poster-title">${htmlAttribute(eventTitle)}</div>
          ${dateStrip(microDatePillsHtml)}
          ${redeemedFriendsHtml}
        </div>
      `;

      wireDateStrips(card, true);
      wirePosterImages(card);

      return card;
    }

    // ===================================================================
    // FORMATAÇÃO DE TÍTULO E DATA DE RESGATE (SEM NORMALIZAÇÃO DE TEATRO)
    // ===================================================================
    function formatRedemptionTitle(str) {
      if (!str) return '';
      str = String(str).trim();
      const lowers = new Set(['de', 'da', 'do', 'dos', 'das', 'e', 'em', 'no', 'na', 'nos', 'nas', 'para', 'com', 'por']);
      const uppers = new Set(['sp', 'rj', 'ba', 'pr', 'mg', 'rs', 'sc', 'df', 'uol', 'off', 'pdf', 'c6']);
      const proper = {
        'nubank': 'Nubank',
        'joao': 'João',
        'joão': 'João',
        'caetano': 'Caetano',
        'interlagos': 'Interlagos',
        'ribeirao': 'Ribeirão',
        'ribeirão': 'Ribeirão',
        'preto': 'Preto',
        'maroon': 'Maroon',
        'zayn': 'Zayn',
        'villa': 'Villa',
        'lobos': 'Lobos',
        'coala': 'Coala',
        'montreux': 'Montreux',
        'jazz': 'Jazz',
        'brasil': 'Brasil',
        'teatro': 'Teatro',
        'ingressos': 'Ingressos',
        'ingresso': 'Ingresso',
        'autodromo': 'Autódromo',
        'autódromo': 'Autódromo',
        'festival': 'Festival',
        'brinde': 'Brinde'
      };

      const words = str.split(/\s+/);
      return words.map((w, idx) => {
        const match = w.match(/^([^a-zA-Z0-9À-ÿ]*)(.*?)([^a-zA-Z0-9À-ÿ]*)$/);
        if (!match) return w;
        const pre = match[1];
        const core = match[2];
        const post = match[3];
        const lowerCore = core.toLowerCase();

        if (uppers.has(lowerCore)) {
          return pre + lowerCore.toUpperCase() + post;
        }
        if (proper[lowerCore]) {
          return pre + proper[lowerCore] + post;
        }
        if (idx > 0 && lowers.has(lowerCore)) {
          return pre + lowerCore + post;
        }
        return pre + (core.charAt(0).toUpperCase() + core.slice(1).toLowerCase()) + post;
      }).join(' ');
    }

    function formatRedemptionDateTime(str) {
      if (!str) return '';
      str = String(str).trim();
      if (/\d{2}\/\d{2}\/\d{4}\s+\d{2}:\d{2}/.test(str)) {
        return str.replace(/\s+/, ' às ');
      }
      return str;
    }

    let currentRedemptionsFilter = 'all'; // 'all' | 'gaoriel' | 'leosaquetto'
    let currentRedemptionsList = [];
    let activeRedemptionIndex = -1;

    function setRedemptionsFilter(filter) {
      currentRedemptionsFilter = filter;
      renderRedemptionsScreen();
    }

    // REDEMPTION_DATE_START
    function parseRedemptionMoment(value) {
      if (typeof value !== 'string' || !value.trim()) return null;
      const text = value.trim();
      const brazilian = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:(?:\s+às\s+|,\s*|\s+)(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/i);
      const iso = text.match(/^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d{1,3})?)?(Z|[+-]\d{2}:\d{2}))?$/i);
      if (!brazilian && !iso) return null;
      const year = Number(brazilian ? brazilian[3] : iso[1]);
      const month = Number(brazilian ? brazilian[2] : iso[2]);
      const day = Number(brazilian ? brazilian[1] : iso[3]);
      const hour = Number((brazilian || iso)[4] || 0);
      const minute = Number((brazilian || iso)[5] || 0);
      const second = Number((brazilian || iso)[6] || 0);
      if (!validCalendarDate(year, month, day) || hour > 23 || minute > 59 || second > 59) return null;
      const precision = (brazilian || iso)[4] == null ? 'day' : 'time';
      let timestamp;
      if (iso && precision === 'time') timestamp = Date.parse(text);
      else {
        // Resolve a data civil no fuso da fonte, independentemente do fuso do aparelho.
        const civil = Date.UTC(year, month - 1, day, hour, minute, second);
        timestamp = civil;
        for (let i = 0; i < 3; i++) {
          const parts = saoPauloParts(timestamp);
          const represented = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
          const adjustment = civil - represented;
          timestamp += adjustment;
          if (!adjustment) break;
        }
        const roundtrip = saoPauloParts(timestamp);
        if (roundtrip.year !== year || roundtrip.month !== month || roundtrip.day !== day || roundtrip.hour !== hour) return null;
      }
      if (!Number.isFinite(timestamp) || timestamp <= 0 || timestamp > Date.now() + 300000) return null;
      return { timestamp: timestamp, precision: precision,
        label: String(day).padStart(2, '0') + '/' + String(month).padStart(2, '0') + '/' + year };
    }

    function parseRedemptionDate(value) {
      const moment = parseRedemptionMoment(value);
      return moment ? moment.timestamp : 0;
    }

    function timelineTimeLabel(event, now = Date.now()) {
      if (!event.timestamp) return 'Data não informada';
      if (event.datePrecision === 'day') return event.dateLabel;
      if (now - event.timestamp > 7 * 86400000) {
        const parts = saoPauloParts(event.timestamp);
        return String(parts.day).padStart(2, '0') + '/' + String(parts.month).padStart(2, '0') + '/' + String(parts.year).slice(-2);
      }
      return formatRelativeTime(event.timestamp, now);
    }
    // REDEMPTION_DATE_END

    // ===================================================================
    // TELA NATIVA MEUS RESGATES (HISTÓRICO UNIFICADO)
    // ===================================================================
    function escapeReceiptHtml(value) {
      return String(value == null ? '' : value).replace(/[&<>"']/g, character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[character]));
    }

    function receiptSafeUrl(value, mode = 'image') {
      if (typeof value !== 'string' || /[\u0000-\u0020\u007f\\<>"']/.test(value)) return '';
      const inline = /^data:(image\/(?:png|jpe?g|webp|gif)|application\/pdf);base64,[A-Za-z0-9+/=]+$/i.exec(value);
      if (inline) return mode === 'image' && inline[1].startsWith('image/') || mode === 'pdf' && inline[1] === 'application/pdf' ? value : '';
      try {
        const url = new URL(value);
        return url.protocol === 'https:' && !url.username && !url.password ? url.href : '';
      } catch (_) { return ''; }
    }

    function isVerifiedReceipt(record) {
      const key = redemptionIdentity(record);
      if (!key) return false;
      const native = typeof window !== 'undefined' && window.hunterVerifiedRedemptions instanceof Map
        && window.hunterVerifiedRedemptions.has(key);
      const initial = typeof INITIAL !== 'undefined' && Array.isArray(INITIAL.realRedemptions)
        && INITIAL.realRedemptions.some(row => row.receiptVerified === true && redemptionIdentity(row) === key);
      return Boolean(native || initial);
    }

    function renderRedemptionsScreen() {
      const container = document.getElementById('viewRedemptions');
      if (!container) return;

      const isLeo = (settings.currentUser !== 'gaoriel');
      const activeDisplayName = isLeo ? 'Leonardo Saquetto' : 'Gabriel';
      const activeHandle = isLeo ? 'leosaquetto' : 'gaoriel';
      const activeAvatarSrc = getAvatarUrl(activeHandle);

      // 1. Consolida todos os resgates sem perder nenhum perfil, filtrando exclusivamente "Campanhas de ingressos"
      const rawAllList = getAllRedemptionsList();
      const allList = rawAllList.filter(r => (r.partner || '').trim().toLowerCase() === 'campanhas de ingressos');

      // 2. Contagens por perfil
      const totalCount = allList.length;
      const gaoCount = allList.filter(r => (r.user || 'leosaquetto') === 'gaoriel').length;
      const leoCount = allList.filter(r => (r.user || 'leosaquetto') === 'leosaquetto').length;

      // 3. Aplica filtro selecionado
      let list = allList;
      if (currentRedemptionsFilter === 'gaoriel') {
        list = allList.filter(r => (r.user || 'leosaquetto') === 'gaoriel');
      } else if (currentRedemptionsFilter === 'leosaquetto') {
        list = allList.filter(r => (r.user || 'leosaquetto') === 'leosaquetto');
      }

      // 4. Ordena por data mais recente
      list = [...list].sort((a, b) => {
        const da = parseRedemptionDate(a.redeemedAt);
        const db = parseRedemptionDate(b.redeemedAt);
        return db - da;
      });

      currentRedemptionsList = list;

      let cardsHtml = '';
      if (list.length > 0) {
        list.forEach((r, idx) => {
          const verified = isVerifiedReceipt(r);
          const isCardLeo = ((r.user || 'leosaquetto') === 'leosaquetto');
          const cardOwnerName = isCardLeo ? 'Leonardo' : 'Gabriel';
          const cardOwnerHandle = isCardLeo ? 'leosaquetto' : 'gaoriel';

          const thumbnail = receiptSafeUrl(r.thumb);
          const thumbHtml = thumbnail
            ? `<img src="${escapeReceiptHtml(thumbnail)}" class="redemption-thumb" />`
            : `<div class="redemption-thumb" style="display:flex;align-items:center;justify-content:center;color:#00e676;font-size:24px;">🎟️</div>`;

          const timeText = verified
            ? r.redeemedAt ? `Resgatado em ${formatRedemptionDateTime(r.redeemedAt)}` : 'Data não informada'
            : r.redeemedAt ? `Registro local • ${formatRedemptionDateTime(r.redeemedAt)}` : 'Registro local não verificado';

          const ownerBadgeHtml = `
            <div class="redemption-owner-pill ${isCardLeo ? 'owner-leo' : 'owner-gaoriel'}">
              <img src="${getAvatarUrl(cardOwnerHandle)}" />
              <span>${cardOwnerName}</span>
            </div>
          `;

          cardsHtml += `
            <div class="redemption-card">
              <div class="redemption-card-main">
                ${thumbHtml}
                <div class="redemption-info">
                  ${ownerBadgeHtml}
                  <div class="redemption-title">${escapeReceiptHtml(formatRedemptionTitle(r.title))}</div>
                  <div class="redemption-partner">${escapeReceiptHtml(r.partner || 'Campanhas de Ingressos')}</div>
                  <div class="redemption-time">
                    <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
                    <span>${escapeReceiptHtml(timeText)}</span>
                  </div>
                </div>
              </div>
              <div class="redemption-actions-row">
                <button class="btn-voucher-action btn-voucher-details" data-redemption-details="${idx}">
                  <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
                  Ver Detalhes
                </button>
                <button class="btn-voucher-action btn-voucher-primary" data-redemption-voucher="${idx}">
                  ${ICONS.tickets}
                  Ver Voucher
                </button>
              </div>
            </div>
          `;
        });
      }

      const isSessionValid = (currentSessionStatus === 'valid');
      const isSessionInvalid = (currentSessionStatus === 'invalid');

      container.innerHTML = `
        <div class="redemptions-view-header">
          <!-- Card de Conta Ativa para Novos Resgates -->
          <div class="redemptions-profile-card">
            <div class="redemptions-profile-left">
              <img class="redemptions-avatar" src="${activeAvatarSrc}" />
              <div class="redemptions-user-info">
                <div class="redemptions-user-name">${activeDisplayName}</div>
                <div class="redemptions-user-handle">Conta Ativa para Novos Resgates</div>
              </div>
            </div>
            <button class="btn-switch-account" data-switch-account="${isLeo ? 'gaoriel' : 'leosaquetto'}">
              <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l5 5"/></svg>
              Alternar
            </button>
          </div>

          <!-- Status da Sessão ao Vivo & Botão de Reconectar -->
          <div class="session-status-card">
            <div class="session-status-indicator" id="redemptionsSessionStatusInd">
              <span class="session-dot ${uolSessionPresentation().dot}"></span>
              <span id="redemptionsSessionStatusLabel">${uolSessionPresentation().label}</span>
            </div>
            <button class="btn-reconnect-session" data-open-auth>
              <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 2v6h-6M3 12a9 9 0 0 1 15-6.7L21 8M3 22v-6h6M21 12a9 9 0 0 1-15 6.7L3 16"/></svg>
              Verificar sessão
            </button>
          </div>

          ${isSessionInvalid ? `
            <div class="session-alert-banner">
              <div class="session-alert-text">
                <strong>⚠️ Sessão de ${activeDisplayName} não conectada:</strong><br/>
                Para sincronizar os últimos ingressos e realizar resgates na conta de ${activeDisplayName}, conecte-se ao Clube UOL.
              </div>
              <button class="btn-connect-account" data-open-auth>
                Conectar
              </button>
            </div>
          ` : ''}

          <!-- Filtros de Ingressos (Todos / Gabriel / Leonardo) -->
          <div class="redemptions-filter-bar">
            <div class="redemptions-filter-pills">
              <button class="redemptions-filter-pill ${currentRedemptionsFilter === 'all' ? 'active' : ''}" data-redemptions-filter="all">
                Todos (${totalCount})
              </button>
              <button class="redemptions-filter-pill ${currentRedemptionsFilter === 'gaoriel' ? 'active' : ''}" data-redemptions-filter="gaoriel">
                <span class="filter-dot dot-gaoriel"></span> Gabriel (${gaoCount})
              </button>
              <button class="redemptions-filter-pill ${currentRedemptionsFilter === 'leosaquetto' ? 'active' : ''}" data-redemptions-filter="leosaquetto">
                <span class="filter-dot dot-leo"></span> Leonardo (${leoCount})
              </button>
            </div>
          </div>

          <div class="redemptions-status-bar" style="margin-top: 10px;">
            <div class="sync-indicator">
              <span class="sync-dot"></span>
              <span>${list.length > 0 ? 'Histórico Unificado de Resgates' : 'Nenhum resgate encontrado'}</span>
            </div>
            <div class="vouchers-count-pill">${list.length} ${list.length === 1 ? 'resgate' : 'resgates'}</div>
          </div>
        </div>

        ${list.length > 0 ? `<div class="redemptions-cards-list">${cardsHtml}</div>` : `
          <div class="redemptions-empty-box">
            <div class="empty-icon-circle">
              ${ICONS.ticketCheck}
            </div>
            <div class="empty-title">Nenhum resgate neste filtro</div>
            <div class="empty-desc">
              Não encontramos resgates cadastrados para a opção selecionada. Toque em "Todos" para visualizar todos os vouchers.
            </div>
            <button class="btn-empty-explore" data-redemptions-filter="all">
              Ver Todos os Resgates
            </button>
          </div>
        `}
      `;
      if (typeof container.querySelectorAll === 'function') {
        container.querySelectorAll('[data-redemption-details]').forEach(button => button.addEventListener('click', () =>
          openRedemptionDetailsModal(Number(button.dataset.redemptionDetails))));
        container.querySelectorAll('[data-redemption-voucher]').forEach(button => button.addEventListener('click', () =>
          openVoucherModal(Number(button.dataset.redemptionVoucher))));
        container.querySelectorAll('[data-redemptions-filter]').forEach(button => button.addEventListener('click', () =>
          setRedemptionsFilter(button.dataset.redemptionsFilter)));
        container.querySelector('[data-switch-account]')?.addEventListener?.('click', () => requestUserSwitch(isLeo ? 'gaoriel' : 'leosaquetto'));
        container.querySelectorAll('[data-open-auth]').forEach(button => button.addEventListener('click', openAuthAssistantModal));
        container.querySelectorAll('img').forEach(image => image.addEventListener('error', () => {
          image.src = 'https://i.imgur.com/UdIgTfI.png';
        }, { once: true }));
      }
    }

    let currentVoucherBlob = null;
    let currentVoucherFileName = 'voucher-clube-uol.pdf';
    let voucherGeneration = 0;
    let pendingNativeVoucher = null;
    const ownedModalOrigins = new WeakMap();

    function enterOwnedModal(modal, close) {
      if (!modal) return;
      const focused = document.activeElement;
      if (focused && focused !== document.body && !modal.contains?.(focused)) ownedModalOrigins.set(modal, focused);
      modal.setAttribute?.('role', 'dialog');
      modal.setAttribute?.('aria-modal', 'true');
      modal.tabIndex = -1;
      const focusGeneration = (modal._hunterFocusGeneration || 0) + 1;
      modal._hunterFocusGeneration = focusGeneration;
      modal.onkeydown = event => {
        if (event.key === 'Escape') { event.preventDefault(); close(event); }
      };
      setTimeout(() => {
        if (modal._hunterFocusGeneration === focusGeneration && modal.style.display === 'flex')
          (modal.querySelector?.('.modal-close-icon, .btn-modal-cancel, [role="tab"]') || modal).focus?.();
      }, 0);
    }

    function leaveOwnedModal(modal) {
      if (!modal) return;
      modal.onkeydown = null;
      modal._hunterFocusGeneration = (modal._hunterFocusGeneration || 0) + 1;
      const origin = ownedModalOrigins.get(modal);
      ownedModalOrigins.delete(modal);
      if (origin && origin.isConnected !== false) origin.focus?.();
    }

    // ===================================================================
    // MODAIS DE VOUCHER & DETALHES DO RESGATE (IN-APP)
    // ===================================================================
    async function openVoucherModal(index) {
      activeRedemptionIndex = index;
      const r = currentRedemptionsList[index];
      if (!r) return;
      const generation = ++voucherGeneration;
      const account = r.user || r.accountId || '';
      const isCurrent = () => generation === voucherGeneration &&
        activeRedemptionIndex === index;

      const vmTitle = document.getElementById('vmTitle');
      if (vmTitle) vmTitle.textContent = formatRedemptionTitle(r.title);

      const isCardLeo = ((r.user || 'leosaquetto') === 'leosaquetto');
      const displayName = isCardLeo ? 'Leonardo Saquetto' : 'Gabriel';
      const vmUser = document.getElementById('vmUser');
      if (vmUser) vmUser.textContent = displayName;

      const vmDate = document.getElementById('vmDate');
      if (vmDate) vmDate.textContent = r.redeemedAt ? formatRedemptionDateTime(r.redeemedAt) : 'Ativo';

      const vmBtnDetails = document.getElementById('vmBtnDetails');
      if (vmBtnDetails) {
        vmBtnDetails.onclick = () => {
          closeVoucherModal();
          openRedemptionDetailsModal(index);
        };
      }

      const modal = document.getElementById('voucherModal');
      if (modal) {
        const statusPill = modal.querySelector?.('.ticket-status-pill');
        if (statusPill) statusPill.textContent = isVerifiedReceipt(r) ? '✓ Resgate Confirmado' : 'Registro local não verificado';
        modal.style.display = 'flex';
        enterOwnedModal(modal, closeVoucherModal);
      }

      if (INITIAL.nativeAccounts && (r.hasNativeVoucher || isVerifiedReceipt(r)) && Number.isInteger(r.nativeVoucherIndex) &&
          ['leosaquetto', 'gaoriel'].includes(account) && /^[A-Za-z0-9_-]{1,200}$/.test(String(r.id || ''))) {
        const id = String(r.id);
        pendingNativeVoucher = { generation, account, id, index: r.nativeVoucherIndex, record: r };
        const container = document.getElementById('voucherViewerContainer');
        if (container) container.innerHTML = '<div class="voucher-loading-state">Carregando voucher salvo neste iPhone…</div>';
        window.location.href = 'hunter-voucher://get?index=' + r.nativeVoucherIndex + '&account=' + account + '&id=' + encodeURIComponent(id) + '&request=' + generation;
        return;
      }
      loadVoucherContent(r, isCurrent);
    }

    function closeVoucherModal(e) {
      if (e && e.stopPropagation) e.stopPropagation();
      voucherGeneration++;
      pendingNativeVoucher = null;
      const modal = document.getElementById('voucherModal');
      if (modal) {
        modal.style.display = 'none';
        leaveOwnedModal(modal);
      }
      currentVoucherBlob = null;
    }

    function handleNativeVoucherResult(account, index, result) {
      const pending = pendingNativeVoucher;
      const r = pending?.record;
      if (!pending || pending.generation !== voucherGeneration || result?.request !== pending.generation ||
          pending.account !== account || pending.index !== index || result?.id !== pending.id ||
          document.getElementById('voucherModal')?.style.display !== 'flex') return;
      pendingNativeVoucher = null;
      const safeDataUrl = result?.status === 'available'
        ? receiptSafeUrl(result.dataUrl, result.mime === 'application/pdf' ? 'pdf' : 'image') : '';
      const blob = safeDataUrl ? voucherBlobFromDataUrl(safeDataUrl) : null;
      if (blob && (!result.mime || result.mime === blob.type)) {
        currentVoucherBlob = blob;
        currentVoucherFileName = voucherFileName(blob.type);
        const container = document.getElementById('voucherViewerContainer');
        container.innerHTML = blob.type === 'application/pdf'
          ? '<iframe class="voucher-pdf-frame" src="' + escapeReceiptHtml(safeDataUrl) + '" title="Voucher Oficial"></iframe>'
          : '<img class="voucher-preview-img" src="' + escapeReceiptHtml(safeDataUrl) + '" alt="Voucher Oficial" />';
        const share = document.getElementById('btnShareVoucher');
        if (share) share.style.display = 'flex';
        return;
      }
      const generation = voucherGeneration;
      const selectedIndex = activeRedemptionIndex;
      loadVoucherContent(r, () => generation === voucherGeneration && activeRedemptionIndex === selectedIndex);
    }

    async function extractVoucherImage(arrayBuffer) {
      try {
        const bytes = new Uint8Array(arrayBuffer);

        // 1. Procura JPEG direto no buffer (0xFF 0xD8 0xFF ... 0xFF 0xD9)
        let start = -1;
        for (let i = 0; i < bytes.length - 2; i++) {
          if (bytes[i] === 0xFF && bytes[i+1] === 0xD8 && bytes[i+2] === 0xFF) {
            start = i;
            break;
          }
        }
        if (start !== -1) {
          let end = -1;
          for (let i = bytes.length - 2; i > start; i--) {
            if (bytes[i] === 0xFF && bytes[i+1] === 0xD9) {
              end = i + 2;
              break;
            }
          }
          if (end !== -1) {
            return { type: 'image/jpeg', data: bytes.slice(start, end) };
          }
        }

        // 2. Procura streams comprimidos com deflate em PDFs (ex: Nova Brasil A4)
        if (typeof DecompressionStream !== 'undefined') {
          let binaryStr = '';
          const len = bytes.byteLength;
          for (let i = 0; i < len; i += 8192) {
            binaryStr += String.fromCharCode.apply(null, bytes.subarray(i, Math.min(i + 8192, len)));
          }
          const streamRegex = /stream\r?\n([\s\S]*?)\r?\nendstream/g;
          let match;
          while ((match = streamRegex.exec(binaryStr)) !== null) {
            const rawStr = match[1];
            const rawBytes = new Uint8Array(rawStr.length);
            for (let j = 0; j < rawStr.length; j++) {
              rawBytes[j] = rawStr.charCodeAt(j);
            }
            try {
              const ds = new DecompressionStream('deflate');
              const writer = ds.writable.getWriter();
              writer.write(rawBytes);
              writer.close();
              const reader = ds.readable.getReader();
              const chunks = [];
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                chunks.push(value);
              }
              let totalLen = 0;
              for (const c of chunks) totalLen += c.length;
              const decomp = new Uint8Array(totalLen);
              let offset = 0;
              for (const c of chunks) {
                decomp.set(c, offset);
                offset += c.length;
              }
              if (decomp[0] === 0xFF && decomp[1] === 0xD8 && decomp[2] === 0xFF) {
                return { type: 'image/jpeg', data: decomp };
              }
            } catch(e) {}
          }
        }
      } catch(e) {
        console.log('extractVoucherImage aviso:', e.message);
      }
      return null;
    }

    // ===================================================================
    // SISTEMA DE CACHE PERSISTENTE DE VOUCHERS (OFFLINE-FIRST)
    // ===================================================================
    const voucherSessionCache = new Map();

    function getVoucherCacheKey(r) {
      const account = String(r.user || r.accountId || '').toLowerCase();
      if (!['leosaquetto', 'gaoriel'].includes(account)) return '';
      const identifier = r.id || r.url || r.cupomCode || r.downloadUrl;
      return identifier ? 'uol_vc_v2_' + account + '_' + encodeURIComponent(String(identifier)) : '';
    }

    function voucherFileName(mime) {
      const extension = mime === 'application/pdf' ? 'pdf' : mime === 'image/png' ? 'png'
        : mime === 'image/jpeg' ? 'jpg' : mime === 'image/webp' ? 'webp' : 'gif';
      return 'voucher-clube-uol.' + extension;
    }

    function voucherBlobFromDataUrl(dataUrl) {
      const match = /^data:(image\/(?:png|jpe?g|webp|gif)|application\/pdf);base64,([A-Za-z0-9+/=]+)$/i.exec(dataUrl || '');
      if (!match) return null;
      try {
        const bytes = base64ToUint8Array(match[2]);
        if (bytes.byteLength > 5 * 1024 * 1024) return null;
        const claimed = match[1].toLowerCase().replace('image/jpg', 'image/jpeg');
        if (voucherMimeFromBytes(bytes.buffer) !== claimed) return null;
        return new Blob([bytes], { type: claimed });
      } catch (_) { return null; }
    }

    function voucherMimeFromBytes(buffer) {
      const b = new Uint8Array(buffer);
      if (b.length >= 5 && b[0] === 0x25 && b[1] === 0x50 && b[2] === 0x44 && b[3] === 0x46 && b[4] === 0x2d) return 'application/pdf';
      if (b.length >= 8 && [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a].every((v, i) => b[i] === v)) return 'image/png';
      if (b.length >= 3 && b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff) return 'image/jpeg';
      if (b.length >= 12 && String.fromCharCode(...b.slice(0, 4)) === 'RIFF' && String.fromCharCode(...b.slice(8, 12)) === 'WEBP') return 'image/webp';
      if (b.length >= 6 && (String.fromCharCode(...b.slice(0, 6)) === 'GIF87a' || String.fromCharCode(...b.slice(0, 6)) === 'GIF89a')) return 'image/gif';
      return '';
    }

    function trustedVoucherDownloadUrl(value) {
      const safe = receiptSafeUrl(value, 'download');
      if (!safe) return '';
      try {
        const host = new URL(safe).hostname.toLowerCase();
        return host === 'uol.com.br' || host.endsWith('.uol.com.br') ? safe : '';
      } catch (_) { return ''; }
    }

    function arrayBufferToBase64(buffer) {
      let binary = '';
      const bytes = new Uint8Array(buffer);
      const len = bytes.byteLength;
      for (let i = 0; i < len; i += 8192) {
        binary += String.fromCharCode.apply(null, bytes.subarray(i, Math.min(i + 8192, len)));
      }
      return btoa(binary);
    }

    function base64ToUint8Array(base64) {
      const binary = atob(base64);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
      }
      return bytes;
    }

    function getStoredVoucher(r) {
      const key = getVoucherCacheKey(r);
      if (!key) return null;
      // 1. Memória rápida da sessão ativa (0 ms)
      if (voucherSessionCache.has(key)) {
        return voucherSessionCache.get(key);
      }
      // Native receipts and saved files are read through the on-demand bridge;
      // keep newly downloaded bytes in memory for this WebView session only.
      if (INITIAL.nativeAccounts) return null;
      // 2. LocalStorage persistente do dispositivo (0 ms, offline)
      try {
        const raw = localStorage.getItem(key);
        if (raw) {
          const parsed = JSON.parse(raw);
          if (parsed && parsed.dataUrl && receiptSafeUrl(parsed.dataUrl, parsed.mimeType === 'application/pdf' ? 'pdf' : 'image')) {
            const blob = voucherBlobFromDataUrl(parsed.dataUrl);
            if (!blob || blob.type !== parsed.mimeType) return null;
            const entry = {
              dataUrl: parsed.dataUrl,
              blob,
              fileName: voucherFileName(blob.type),
              fromCache: true
            };
            voucherSessionCache.set(key, entry);
            return entry;
          }
        }
      } catch(e) {}
      return null;
    }

    function storeVoucherInCache(r, dataUrl, blob) {
      const key = getVoucherCacheKey(r);
      if (!key || !blob || !receiptSafeUrl(dataUrl, blob.type === 'application/pdf' ? 'pdf' : 'image')) return;
      const entry = { dataUrl, blob, fileName: voucherFileName(blob.type), fromCache: true };
      voucherSessionCache.set(key, entry);
      if (INITIAL.nativeAccounts) return;

      // Persist only the validated original bytes; the preview is never a second copy.
      try {
        localStorage.setItem(key, JSON.stringify({
          dataUrl,
          mimeType: blob.type,
          savedAt: Date.now()
        }));
      } catch(e) {
        console.log('localStorage quota notice:', e.message);
      }
    }

    async function loadVoucherContent(r, isCurrent = () => true) {
      const container = document.getElementById('voucherViewerContainer');
      const btnShare = document.getElementById('btnShareVoucher');
      if (!container || !isCurrent()) return;

      currentVoucherBlob = null;
      if (btnShare) btnShare.style.display = 'none';

      // 0. Prioridade máxima: Imagem de alta resolução vinda direto do Banco de Dados Permanente (Zero rede, Offline)
      const storedImage = receiptSafeUrl(r.voucherImageDataUrl);
      const storedBlob = storedImage ? voucherBlobFromDataUrl(storedImage) : null;
      if (storedBlob && storedBlob.type.startsWith('image/')) {
        currentVoucherFileName = voucherFileName(storedBlob.type);
        currentVoucherBlob = storedBlob;

        container.innerHTML = `
          <img class="voucher-preview-img" src="${escapeReceiptHtml(storedImage)}" alt="Voucher Oficial" />
        `;
        if (btnShare && currentVoucherBlob) btnShare.style.display = 'flex';
        return;
      }

      const cached = getStoredVoucher(r);
      if (cached && receiptSafeUrl(cached.dataUrl, cached.blob.type === 'application/pdf' ? 'pdf' : 'image')) {
        currentVoucherBlob = cached.blob;
        currentVoucherFileName = cached.fileName;
        container.innerHTML = cached.blob.type === 'application/pdf'
          ? `<iframe class="voucher-pdf-frame" src="${escapeReceiptHtml(cached.dataUrl)}" title="Voucher Oficial"></iframe>`
          : `<img class="voucher-preview-img" src="${escapeReceiptHtml(cached.dataUrl)}" alt="Voucher Oficial" />`;
        if (btnShare) btnShare.style.display = 'flex';
        return;
      }

      // 1. Caso seja voucher com arquivo (PDF ou imagem)
      const downloadUrl = trustedVoucherDownloadUrl(r.downloadUrl);
      if (downloadUrl) {

        // Se ainda não estiver em cache, faz o download apenas uma vez
        container.innerHTML = `
          <div class="voucher-loading-state">
            <div class="status-dot"></div>
            <div style="font-size: 12.5px; color: #94a3b8; font-weight: 600;">Carregando voucher oficial...</div>
          </div>
        `;
        container.style.display = 'flex';

        try {
          const res = await fetch(downloadUrl);
          if (!isCurrent()) return;
          if (!res.ok) throw new Error('Status HTTP ' + res.status);
          if (res.url && !trustedVoucherDownloadUrl(res.url)) throw new Error('Origem do voucher não verificada');
          const arrayBuffer = await res.arrayBuffer();
          if (!isCurrent()) return;
          if (arrayBuffer.byteLength > 5 * 1024 * 1024) throw new Error('Arquivo acima do limite');
          const mime = voucherMimeFromBytes(arrayBuffer);
          if (!mime) throw new Error('Formato do voucher não reconhecido');
          const blob = new Blob([arrayBuffer], { type: mime });
          const originalDataUrl = 'data:' + mime + ';base64,' + arrayBufferToBase64(arrayBuffer);
          let previewDataUrl = originalDataUrl;
          if (mime === 'application/pdf') {
            const extracted = await extractVoucherImage(arrayBuffer);
            if (!isCurrent()) return;
            if (extracted?.type === 'image/jpeg') {
              previewDataUrl = 'data:image/jpeg;base64,' + arrayBufferToBase64(extracted.data.buffer.slice(
                extracted.data.byteOffset, extracted.data.byteOffset + extracted.data.byteLength));
            }
          }
          currentVoucherBlob = blob;
          currentVoucherFileName = voucherFileName(mime);
          storeVoucherInCache(r, originalDataUrl, blob);
          container.innerHTML = previewDataUrl.startsWith('data:application/pdf;')
            ? `<iframe class="voucher-pdf-frame" src="${escapeReceiptHtml(previewDataUrl)}" title="Voucher Oficial"></iframe>`
            : `<img class="voucher-preview-img" src="${escapeReceiptHtml(previewDataUrl)}" alt="Voucher Oficial" />`;
          if (btnShare) btnShare.style.display = 'flex';

        } catch (e) {
          if (!isCurrent()) return;
          console.log('Aviso ao carregar voucher:', e.message);
          container.innerHTML = `
            <div style="padding: 24px 16px; text-align: center;">
              <div style="font-size: 13px; color: #fbbf24; font-weight: 700; margin-bottom: 6px;">Voucher não carregado</div>
              <div style="font-size: 11.5px; color: #94a3b8; line-height: 1.4;">
                Não foi possível abrir o arquivo agora. Consulte os detalhes do resgate ou tente novamente.
              </div>
            </div>
          `;
        }
      } else if (r.cupomCode && !r.cupomCode.includes('.pdf') && !r.cupomCode.includes('.png') && !r.cupomCode.includes('.jpg')) {
        // 2. Caso seja cupom de texto promocional (ex: clubeuol50)
        container.innerHTML = `
          <div class="promo-code-card" style="width: 100%;">
            <div class="promo-code-label">CÓDIGO PROMOCIONAL</div>
            <div class="promo-code-val">${escapeReceiptHtml(r.cupomCode)}</div>
            <button class="btn-copy-promo" id="btnCopyPromo">
              <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
              <span id="btnCopyPromoText">Copiar Cupom</span>
            </button>
          </div>
        `;
        const copyButton = document.getElementById('btnCopyPromo');
        if (copyButton) copyButton.onclick = () => copyPromoCode(String(r.cupomCode));
        container.style.display = 'block';
      } else {
        // 3. Fallback informativo
        const verified = isVerifiedReceipt(r);
        container.innerHTML = `
          <div style="padding: 24px 16px; text-align: center;">
            <div style="font-size: 13px; color: ${verified ? '#00e676' : '#fbbf24'}; font-weight: 700; margin-bottom: 6px;">${verified ? '✓ Benefício Ativo' : 'Registro local não verificado'}</div>
            <div style="font-size: 11.5px; color: #94a3b8; line-height: 1.4;">
              ${verified ? 'Consulte as instruções oficiais do benefício antes de apresentar o comprovante.' : 'Confirme este registro no histórico oficial da conta antes de utilizá-lo.'}
            </div>
          </div>
        `;
        container.style.display = 'block';
      }
    }

    async function shareVoucherAction() {
      if (!currentVoucherBlob) return;
      try {
        const mime = currentVoucherBlob.type;
        if (!['application/pdf', 'image/png', 'image/jpeg', 'image/webp', 'image/gif'].includes(mime)) return;
        const file = new File([currentVoucherBlob], voucherFileName(mime), { type: mime });

        if (navigator.canShare && navigator.canShare({ files: [file] })) {
          await navigator.share({
            files: [file],
            title: 'Voucher Clube UOL'
          });
        } else {
          const blobUrl = URL.createObjectURL(currentVoucherBlob);
          window.open(blobUrl, '_blank');
        }
      } catch (e) {
        console.log('Share cancelado:', e.message);
      }
    }

    function copyPromoCode(code) {
      const btnText = document.getElementById('btnCopyPromoText');
      const setSuccess = () => {
        if (btnText) btnText.textContent = 'Copiado!';
        setTimeout(() => {
          if (btnText) btnText.textContent = 'Copiar Cupom';
        }, 2000);
      };

      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(code).then(setSuccess).catch(() => {
          fallbackCopyText(code, setSuccess);
        });
      } else {
        fallbackCopyText(code, setSuccess);
      }
    }

    function fallbackCopyText(text, cb) {
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      try {
        document.execCommand('copy');
        if (cb) cb();
      } catch (err) {}
      document.body.removeChild(ta);
    }

    function openRedemptionDetailsModal(index) {
      activeRedemptionIndex = index;
      const r = currentRedemptionsList[index];
      if (!r) return;

      const rdmTitle = document.getElementById('rdmTitle');
      if (rdmTitle) rdmTitle.textContent = formatRedemptionTitle(r.title);

      const rdmImg = document.getElementById('rdmImg');
      if (rdmImg) {
        const thumbnail = receiptSafeUrl(r.thumb);
        if (thumbnail) {
          rdmImg.src = thumbnail;
          rdmImg.style.display = 'block';
        } else {
          rdmImg.style.display = 'none';
        }
      }

      const rdmTagRow = document.getElementById('rdmTagRow');
      if (rdmTagRow) {
        const isCardLeo = ((r.user || 'leosaquetto') === 'leosaquetto');
        const cardOwnerName = isCardLeo ? 'Leonardo' : 'Gabriel';
        rdmTagRow.innerHTML = `
          <span class="card-tag tag-portal">${ICONS.globe} Clube UOL</span>
          <span class="card-tag tag-category tag-cat-MUSICA">🎟️ Ingressos</span>
          <span class="card-tag tag-age">👤 ${cardOwnerName}</span>
          <span class="card-tag tag-partner">${escapeReceiptHtml(r.partner || 'Campanhas de Ingressos')}</span>
        `;
      }

      const rdmTimeBadge = document.getElementById('rdmTimeBadge');
      if (rdmTimeBadge) {
        if (r.redeemedAt) {
          rdmTimeBadge.innerHTML = `
            <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
            <span>${isVerifiedReceipt(r) ? 'Resgatado em' : 'Registro local em'} ${escapeReceiptHtml(formatRedemptionDateTime(r.redeemedAt))}</span>
          `;
          rdmTimeBadge.style.display = 'inline-flex';
        } else {
          rdmTimeBadge.style.display = 'none';
        }
      }

      const rdmDesc = document.getElementById('rdmDesc');
      if (rdmDesc) {
        rdmDesc.textContent = r.description || (isVerifiedReceipt(r)
          ? 'Resgate confirmado no Clube UOL. Consulte o comprovante e as instruções oficiais do benefício.'
          : 'Registro local ainda não confirmado no histórico oficial. Consulte os detalhes do benefício.');
      }

      const rdmRulesArea = document.getElementById('rdmRulesArea');
      const rdmRulesContent = document.getElementById('rdmRulesContent');
      if (rdmRulesArea && rdmRulesContent) {
        if (r.rules && r.rules.trim().length > 0) {
          rdmRulesContent.textContent = r.rules.trim();
          rdmRulesArea.style.display = 'block';
        } else {
          rdmRulesArea.style.display = 'none';
        }
      }

      const rdmBtnVoucher = document.getElementById('rdmBtnVoucher');
      if (rdmBtnVoucher) {
        rdmBtnVoucher.onclick = () => {
          closeRedemptionDetailsModal();
          openVoucherModal(index);
        };
      }

      const modal = document.getElementById('redemptionDetailsModal');
      if (modal) { modal.style.display = 'flex'; enterOwnedModal(modal, closeRedemptionDetailsModal); }
    }

    function closeRedemptionDetailsModal(e) {
      if (e && e.stopPropagation) e.stopPropagation();
      const modal = document.getElementById('redemptionDetailsModal');
      if (modal) { modal.style.display = 'none'; leaveOwnedModal(modal); }
    }

    function updateCategoryCounts(list) {
      const counts = { EXPOSICOES: 0, MUSEU: 0, MUSICA: 0, TEATRO: 0, FUTEBOL: 0, OUTROS: 0 };
      const visible = homeOfferSets(list, historyData.history);
      const countItems = isGrouped ? buildOfferGroups(visible.activeItems, visible.soldOutItems) : [...visible.activeItems, ...visible.soldOutItems];
      countItems.forEach(o => {
        if (counts[o.category] != null) counts[o.category]++;
        else counts.OUTROS++;
      });

      const container = document.getElementById('categoryChips');
      if (!container) return;
      container.innerHTML = '';

      // Se o filtro ativo ficou zerado na nova contagem, reseta para todas
      if (activeFilter && (!counts[activeFilter] || counts[activeFilter] === 0)) {
        activeFilter = null;
      }

      // Filtra apenas categorias com contagem > 0 e ordena alfabeticamente pelo label
      const activeCats = Object.keys(counts)
        .filter(catId => counts[catId] > 0)
        .map(catId => ({
          id: catId,
          label: (CATEGORY_MAP[catId] || CATEGORY_MAP.OUTROS).label,
          count: counts[catId]
        }))
        .sort((a, b) => a.label.localeCompare(b.label, 'pt-BR'));

      activeCats.forEach(cat => {
        const chip = document.createElement('div');
        chip.className = 'chip' + (activeFilter === cat.id ? ' active' : '');
        chip.id = 'chip_' + cat.id;
        chip.textContent = `${cat.label} ${cat.count}`;
        chip.onclick = () => toggleCategoryFilter(cat.id);
        container.appendChild(chip);
      });
    }

    function toggleCategoryFilter(catId) {
      if (activeFilter === catId) {
        activeFilter = null; // desapertar de novo volta pra todas
      } else {
        activeFilter = catId;
      }

      const container = document.getElementById('categoryChips');
      if (container) {
        const chips = container.querySelectorAll('.chip');
        chips.forEach(c => {
          if (activeFilter && c.id === 'chip_' + activeFilter) {
            c.classList.add('active');
          } else {
            c.classList.remove('active');
          }
        });
      }

      renderOffers(historyData.active || []);
    }

    function toggleGroupingMode() {
      isGrouped = !isGrouped;
      settings.groupedView = isGrouped;
      saveSettingsToStorage();
      updateViewToggleButton();
      renderOffers(historyData.active || []);
    }

    function updateViewToggleButton() {
      const btn = document.getElementById('btnViewToggle');
      if (!btn) return;
      if (isGrouped) {
        btn.innerHTML = '📋 Lista Individual';
        btn.classList.add('active');
      } else {
        btn.innerHTML = '🧩 Carrossel Agrupado';
        btn.classList.remove('active');
      }
    }

    // ===================================================================
    // DETALHES COMPLETOS SOB DEMANDA
    // ===================================================================
    function findOffer(id) {
      if (typeof CURATED_EDITORS_CHOICE_SEEDS !== 'undefined') {
        const fromCurated = CURATED_EDITORS_CHOICE_SEEDS.find(x => x.id === id);
        if (fromCurated) return fromCurated;
      }
      const fromActive = (historyData.active || []).find(x => x.id === id);
      if (fromActive) return fromActive;
      return (historyData.history || []).find(x => x.id === id) || null;
    }

    let detailsGeneration = 0;
    function isOfferUnavailableForRedeem(offer) {
      return Boolean(offer && (offer.expiredAt || offer.isExpired || offer.unavailableAt ||
        typeof linkUnavailableAt === 'function' && linkUnavailableAt(offer)));
    }

    async function openDetails(id) {
      const offer = findOffer(id);
      if (!offer) return;
      activeOffer = offer;
      const generation = ++detailsGeneration;

      const isStoriesOnly = isStoriesOnlyOffer(activeOffer);
      const isStory = isStoriesOnly || Boolean(activeOffer.source === 'Stories' || activeOffer.hasStory || activeOffer.badge === 'Stories');
      const isPortal = isStoriesOnly ? false : (activeOffer.isPortal !== false);
      const catDef = CATEGORY_MAP[activeOffer.category] || CATEGORY_MAP.OUTROS;
      const capturedAt = captureTime(activeOffer);
      const firstDateStr = capturedAt ? new Date(capturedAt).toLocaleDateString('pt-BR') : 'Data não informada';

      let dtTagsHtml = '';
      if (isEditorsChoice(activeOffer)) {
        dtTagsHtml += '<span class="pill-editors-choice" style="font-size: 11px; padding: 3px 8px; margin-right: 4px;">★ Curadoria</span>';
      }
      if (activeOffer.code) {
        dtTagsHtml += '<span class="card-tag tag-code">[' + escapeReceiptHtml(activeOffer.code) + ']</span>';
      }
      if (isPortal) {
        dtTagsHtml += '<span class="card-tag tag-portal">' + ICONS.globe + ' Portal</span>';
      }
      if (isStory) {
        dtTagsHtml += '<span class="card-tag tag-stories">' + ICONS.instagram + ' Stories</span>';
      }
      dtTagsHtml += '<span class="card-tag tag-category tag-cat-' + escapeReceiptHtml(catDef.id) + '">' + catDef.icon + ' ' + escapeReceiptHtml(catDef.label) + '</span>';
      dtTagsHtml += '<span class="card-tag tag-age">' + escapeReceiptHtml(firstDateStr) + '</span>';

      const redeemedUsers = getRedeemedUsersForOffer(activeOffer);
      const redeemedFriendsHtml = renderRedeemedFriendsPill(redeemedUsers);
      if (redeemedFriendsHtml) {
        dtTagsHtml += '<div style="margin-top: 6px; width: 100%;">' + redeemedFriendsHtml + '</div>';
      }

      const tagRowEl = document.getElementById('dtTagRow');
      if (tagRowEl) tagRowEl.innerHTML = dtTagsHtml;

      const normDt = normalizeEventInfo([activeOffer]);
      document.getElementById('dtTitle').textContent = normDt.eventTitle || activeOffer.title;
      document.getElementById('dtImg').src = receiptSafeUrl(activeOffer.imageUrl) || 'https://i.imgur.com/UdIgTfI.png';

      // Se há outras sessões (esgotadas ou ativas) do mesmo benefício
      const gKey = getGroupKey(activeOffer);
      const g = currentRenderedGroups.get(gKey);
      const relEl = document.getElementById('dtRelatedSessions');
      if (relEl) {
        if (g && (g.activeItems.length + g.soldOutItems.length > 1)) {
          let relatedHtml = '<div class="dt-related-sessions"><div class="dt-related-title">Outras sessões deste benefício:</div><div class="dt-related-list">';
          const otherItems = [...g.activeItems, ...g.soldOutItems].filter(x => x.id !== activeOffer.id);
          otherItems.forEach(ot => {
            const dLabel = getSessionDateLabel(ot.rawTitle || ot.title);
            const linkUnavailable = typeof linkUnavailableAt === 'function' && linkUnavailableAt(ot);
            const isOtExpired = Boolean(ot.expiredAt || ot.isExpired || ot.unavailableAt || linkUnavailable);
            const stText = ot.unavailableAt || linkUnavailable ? 'Indisponível' : isOtExpired ? 'Esgotado' : 'Disponível';
            const stClass = isOtExpired ? 'st-soldout' : 'st-active';
            relatedHtml += `<div class="dt-related-item ${stClass}"><span>${escapeReceiptHtml(dLabel)}</span><span class="dt-related-status">${stText}</span></div>`;
          });
          relatedHtml += '</div></div>';
          relEl.innerHTML = relatedHtml;
          relEl.style.display = 'block';
        } else {
          relEl.innerHTML = '';
          relEl.style.display = 'none';
        }
      }

      // Se a oferta estiver esgotada, no catálogo histórico ou for curadoria, oculta o botão de resgatar no modal
      const btnRedeemModal = document.getElementById('dtBtnRedeem');
      if (btnRedeemModal) {
        const isCurated = Boolean(activeOffer.id && String(activeOffer.id).startsWith('curated-'));
        if (isOfferUnavailableForRedeem(activeOffer) || currentTab === 'history' || isCurated) {
          btnRedeemModal.style.display = 'none';
        } else {
          btnRedeemModal.style.display = 'flex';
        }
      }

      const detailsModal = document.getElementById('detailsModal');
      detailsModal.style.display = 'flex';
      enterOwnedModal(detailsModal, closeDetailsModal);

      // 1. Verifica cache rápido de descrição
      if (!activeOffer.description || activeOffer.description.length < 50) {
        const cached = getCachedDescription(activeOffer.id);
        if (cached && cached.length >= 50) {
          activeOffer.description = cached;
        }
      }

      // Se já temos a descrição, exibe imediatamente sem requisição de rede
      if (activeOffer.description && activeOffer.description.length >= 30) {
        renderModalDescription(activeOffer.description);
        return;
      }

      const detailCampaign = typeof catalogCampaign === 'function' ? catalogCampaign(offer.link) : null;
      if (!detailCampaign) {
        renderModalDescription(activeOffer.description || 'Benefício exclusivo para assinantes Clube UOL.');
        return;
      }

      // 2. Se não estiver em cache, busca na web
      document.getElementById('dtDesc').innerHTML = '<span class="status-dot"></span> Buscando texto oficial completo e regras...';
      try {
        const res = await fetch(detailCampaign.link);
        if (res.ok) {
          const html = await res.text();
          const mAssinante = html.match(/<p>\s*<p><strong>Assinante UOL[\s\S]*?<\/p>\s*<\/p>/i);
          if (mAssinante) {
            let clean = mAssinante[0]
              .replace(/<\/p>\s*<p>/gi, '\n\n')
              .replace(/<br\s*\/?>/gi, '\n')
              .replace(/<[^>]+>/g, '')
              .replace(/&amp;/g, '&')
              .replace(/&quot;/g, '"')
              .replace(/&#34;/g, '"')
              .replace(/&#39;/g, "'")
              .replace(/&bull;/g, '•')
              .trim();
            offer.description = clean;
          } else {
            const mMeta = html.match(/<meta\s+property="og:description"\s+content="([^"]+)"/i);
            if (mMeta && !mMeta[1].toLowerCase().includes('vantagens exclusivas')) {
              offer.description = mMeta[1];
            }
          }
        }
      } catch (e) {}

      if (generation !== detailsGeneration || activeOffer !== offer) return;
      if (!offer.description || offer.description.length < 20) {
        offer.description = 'Benefício exclusivo para assinantes Clube UOL.\n\nToque em "Abrir oferta no navegador" para visualizar todos os detalhes.';
      }

      // Salva no cache permanente e no histórico local
      setCachedDescription(offer.id, offer.description);
      saveHistoryToStorage();

      renderModalDescription(offer.description);
    }

    function renderModalDescription(text) {
      document.getElementById('dtDesc').innerHTML = escapeReceiptHtml(text || '')
        .replace(/\n/g, '<br/>')
        .replace(/(Assinante UOL[\s\S]*?:)/g, '<strong>$1</strong>');
      if (activeOffer) {
        const category = classifyCategory(activeOffer.rawTitle || activeOffer.title, activeOffer.link || activeOffer.id, activeOffer.description || text);
        if (category !== activeOffer.category) {
          activeOffer.category = category;
          const tag = document.querySelector('#dtTagRow .tag-category');
          const definition = CATEGORY_MAP[category] || CATEGORY_MAP.OUTROS;
          if (tag) { tag.className = 'card-tag tag-category tag-cat-' + definition.id; tag.innerHTML = definition.icon + ' ' + escapeReceiptHtml(definition.label); }
          saveHistoryToStorage();
          updateCategoryCounts(historyData.active || []);
          if (currentTab === 'home' || currentTab === 'tickets') renderOffers(historyData.active || []);
          else if (currentTab === 'history') renderHistoryScreen();
          updateTimelineBadgeDot();
        }
      }
    }

    function closeDetailsModal(e) {
      if (e && e.stopPropagation) e.stopPropagation();
      detailsGeneration++;
      const modal = document.getElementById('detailsModal');
      modal.style.display = 'none';
      leaveOwnedModal(modal);
    }

    function proceedToRedeemFromDetails() {
      closeDetailsModal();
      if (activeOffer) {
        openConfirm(activeOffer.id);
      }
    }

    function openSafariCurrent(external = false) {
      if (!activeOffer) return;
      const url = activeOffer.link || activeOffer.url || 'https://clubeuol.uol.com.br';
      try {
        const target = new URL(url, 'https://clube.uol.com.br');
        if (target.protocol !== 'https:' || target.username || target.password ||
            !['clube.uol.com.br', 'clubeuol.uol.com.br'].includes(target.hostname) || target.port) return;
        if (INITIAL.nativeBrowser) {
          const route = external ? 'hunter-browser://external?url=' : 'hunter-browser://open?url=';
          window.location.href = route + encodeURIComponent(target.href);
        } else {
          window.open(target.href, '_blank', 'noopener,noreferrer');
        }
      } catch (_) { showToast('Link da oferta indisponível.'); }
    }

    // ===================================================================
    // CONFIRMAÇÃO & CICLO DE RESGATE
    // ===================================================================
    function openConfirm(id) {
      activeOffer = findOffer(id);
      if (!activeOffer) return;
      detailsGeneration++;
      if (isOfferUnavailableForRedeem(activeOffer)) { showToast('Esta sessão não está disponível para resgate.'); return; }

      document.getElementById('cfEventTitle').textContent = activeOffer.title + ' • Conta: ' + ((settings.currentUser || 'leosaquetto') === 'gaoriel' ? 'Gabriel' : 'Leonardo');
      document.getElementById('cfFeedbackArea').className = 'feedback-banner feedback-warning';

      const gKey = getGroupKey(activeOffer);
      const g = currentRenderedGroups.get(gKey);
      let sessionNote = '';
      if (g && g.soldOutItems.length > 0) {
        const soldDates = g.soldOutItems.map(extractSessionDate).filter(Boolean);
        if (soldDates.length > 0) {
          sessionNote = `<div style="margin-top: 6px; font-size: 11.5px; color: #f87171;">Nota: Outra(s) sessão(ões) deste benefício (${soldDates.map(escapeReceiptHtml).join(', ')}) já esgotaram.</div>`;
        }
      }

      document.getElementById('cfFeedbackArea').innerHTML = '<strong>Atenção à cota mensal:</strong> O Clube UOL permite o resgate de apenas <strong>1 par de ingressos por mês</strong> por CPF. Se você já resgatou este mês, o benefício não será concedido.' + sessionNote;

      const btn = document.getElementById('btnConfirmAction');
      btn.style.display = 'flex';
      btn.innerHTML = ICONS.tickets + ' Sim, Resgatar Agora';
      document.getElementById('btnConfirmOpenWeb').style.display = 'none';

      const confirmModal = document.getElementById('confirmModal');
      confirmModal.style.display = 'flex';
      enterOwnedModal(confirmModal, closeConfirmModal);
    }

    function closeConfirmModal(e) {
      if (e && e.stopPropagation) e.stopPropagation();
      const modal = document.getElementById('confirmModal');
      modal.style.display = 'none';
      leaveOwnedModal(modal);
    }

    let nativeRedemptionPending = null;
    const nativeHistoryPending = new Set();
    async function executeRedemption() {
      if (!activeOffer || nativeRedemptionPending) return;
      if (isOfferUnavailableForRedeem(activeOffer)) { showToast('Esta sessão não está disponível para resgate.'); return; }
      const account = settings.currentUser || 'leosaquetto';
      if (!INITIAL.nativeAccounts) { showToast('Resgate seguro disponível no Scriptable.'); return; }
      let url;
      try {
        url = new URL(activeOffer.link);
        if (url.origin !== 'https://clube.uol.com.br' || !/^\/campanhasdeingresso\/[A-Za-z0-9][A-Za-z0-9_-]*$/.test(url.pathname) || url.search || url.hash) return;
      } catch (_) { return; }
      nativeRedemptionPending = account;
      const btn = document.getElementById('btnConfirmAction');
      btn.disabled = true;
      btn.innerHTML = '<span class="status-dot"></span> Conferindo conta e recibo…';
      window.location.href = 'hunter-account://redeem?account=' + account + '&url=' + encodeURIComponent(url.href);
    }

    let currentSessionStatus = 'unknown';
    let currentSessionReason = '';
    function uolSessionPresentation() {
      const messages = {
        history_parser_unavailable: 'Não foi possível interpretar a resposta neste aparelho.',
        network_error: 'Não foi possível consultar o Clube UOL agora.',
        challenge_required: 'O Clube UOL solicitou uma verificação de acesso.',
        identity_mismatch: 'A conta confirmada não corresponde ao perfil selecionado. Nenhuma outra conta foi autorizada.',
        account_access_required: 'Este perfil ainda não foi liberado para este aparelho.',
        cookies_invalid: 'Não foi possível ler a sessão salva desta conta.',
        history_incomplete: 'O histórico recebido está incompleto.',
        redirect_not_controlled: 'Não foi possível confirmar o redirecionamento do Clube UOL.',
        busy: 'Outra operação está em andamento. Toque em verificar ao terminar.',
        login_unavailable: 'Não foi possível abrir o login oficial. A sessão salva foi preservada.',
        login_callback_invalid: 'Não foi possível confirmar o retorno do login. A sessão salva foi preservada.',
        login_expired: 'O login demorou mais de dez minutos. Abra uma nova conexão.',
        cancelled: 'Login cancelado. A sessão salva foi preservada.'
      };
      if (currentSessionStatus === 'valid') return { label: 'Conectado', detail: 'Conta confirmada pelo histórico oficial.', dot: 'dot-green' };
      if (currentSessionStatus === 'checking') return { label: 'Verificando…', detail: 'Conferindo a conta sem fazer resgate.', dot: 'dot-amber' };
      if (currentSessionStatus === 'invalid') return { label: 'Sessão expirada', detail: 'O Clube UOL pediu autenticação para esta sessão.', dot: 'dot-amber' };
      if (currentSessionStatus === 'unavailable') return { label: 'Verificação indisponível', detail: messages[currentSessionReason] || 'Não foi possível confirmar a sessão. Toque em verificar para consultar novamente.', dot: 'dot-amber' };
      return { label: 'Não verificada', detail: messages[currentSessionReason] || 'O acesso do aparelho está separado da sessão de cada perfil. Toque em verificar para conferir esta conta.', dot: 'dot-amber' };
    }
    async function fetchRealRedemptions() {
      const account = settings.currentUser || 'leosaquetto';
      if (!INITIAL.nativeAccounts || nativeHistoryPending.has(account)) return;
      nativeHistoryPending.add(account);
      currentSessionStatus = 'checking'; currentSessionReason = ''; updateSessionStatusUI();
      window.location.href = 'hunter-account://history?account=' + account;
    }

    function mergeNativeAccountRecords(account, records) {
      if (!Array.isArray(records)) return;
      const publicFields = new Set(['id', 'url', 'title', 'user', 'accountId', 'partner', 'thumb',
        'redeemedAt', 'description', 'rules', 'downloadUrl', 'cupomCode', 'code', 'category',
        'imageUrl', 'offerUrl', 'redemptionDate', 'date', 'nativeVoucherIndex', 'hasNativeVoucher']);
      const publicRecord = record => Object.fromEntries(Object.entries(record || {}).filter(([field, value]) =>
        publicFields.has(field) && (
          typeof value === 'string' && !/^data:/i.test(value) && value.length <= (field === 'description' || field === 'rules' ? 20000 : 4096) ||
          field === 'nativeVoucherIndex' && Number.isSafeInteger(value) && value >= 0 ||
          field === 'id' && Number.isSafeInteger(value) ||
          field === 'hasNativeVoucher' && typeof value === 'boolean'
        )));
      const liveVerified = window.hunterVerifiedRedemptions instanceof Map
        ? window.hunterVerifiedRedemptions : new Map();
      window.hunterVerifiedRedemptions = liveVerified;
      const merged = new Map();
      (settings.realRedemptions || []).forEach((record, index) => {
        const safe = publicRecord(record);
        delete safe.nativeVoucherIndex;
        delete safe.hasNativeVoucher;
        merged.set(redemptionIdentity(safe) || 'legacy:' + index, safe);
      });
      for (const incoming of records) {
        if (!incoming || (incoming.user && incoming.user !== account)) continue;
        const source = publicRecord(incoming);
        const verified = { ...source, user: account,
          thumb: source.thumb || source.imageUrl || '',
          partner: source.partner || 'Campanhas de Ingressos' };
        const key = redemptionIdentity(verified);
        if (!key) continue;
        const old = merged.get(key) || {};
        const priorVerified = liveVerified.get(key) || (Array.isArray(INITIAL.realRedemptions)
          ? INITIAL.realRedemptions.find(record => redemptionIdentity(record) === key) : null) || {};
        // Native verified metadata wins. Legacy values fill only absent presentation fields.
        const presentVerified = Object.fromEntries(Object.entries(verified).filter(([, value]) => value !== '' && value != null));
        const authoritative = { ...publicRecord(priorVerified), ...presentVerified };
        authoritative.redeemedAt = [verified.redeemedAt, verified.redemptionDate,
          priorVerified.redeemedAt, priorVerified.redemptionDate].find(value => parseRedemptionMoment(value)) || '';
        authoritative.date = [verified.date, priorVerified.date].find(value => parseRedemptionMoment(value)) || '';
        const record = { ...old, ...authoritative };
        liveVerified.set(key, authoritative);
        merged.set(key, record);
      }
      settings.realRedemptions = Array.from(merged.values());
      saveSettingsToStorage();
      updateTimelineBadgeDot();
      if (currentTab === 'redemptions') renderRedemptionsScreen();
    }

    function handleNativeAccountResult(action, account, result) {
      if (!['leosaquetto', 'gaoriel'].includes(account) || result?.account !== account) return;
      if (action === 'history' || action === 'reconnect') {
        nativeHistoryPending.delete(account);
        if ((settings.currentUser || 'leosaquetto') === account) {
          currentSessionStatus = result.status === 'authenticated' ? 'valid' : result.status === 'auth_required' ? 'invalid' : ['busy', 'cancelled'].includes(result.status) ? 'unknown' : 'unavailable';
          currentSessionReason = result.status === 'busy' ? 'busy' : result.reason || result.status;
          updateSessionStatusUI();
        }
        if (result.status === 'authenticated') mergeNativeAccountRecords(account, result.records);
        return;
      }
      if (action !== 'redeem' || nativeRedemptionPending !== account) return;
      nativeRedemptionPending = null;
      const btn = document.getElementById('btnConfirmAction'), fb = document.getElementById('cfFeedbackArea');
      btn.disabled = false; btn.style.display = 'none';
      const messages = {
        confirmed: 'Voucher confirmado no histórico da conta. Resgate concluído.',
        quota_used: 'A cota mensal desta conta já foi utilizada. Nenhum novo voucher confirmado.',
        sold_out: 'Benefício indisponível para resgate.',
        auth_required: 'A sessão desta conta precisa ser renovada. Nenhum resgate confirmado.',
        identity_mismatch: 'A sessão pertence a outra conta. Resgate bloqueado.',
        account_access_required: 'Esta conta não está liberada para este aparelho. Peça ao proprietário para revisar as permissões.',
        uncertain: 'Resultado ainda incerto. A tentativa foi preservada; uma nova execução só confere o histórico, sem repetir o resgate.',
        busy: 'Outra operação está em andamento. Aguarde antes de tentar.',
        unavailable: 'Não foi possível validar a conta ou a oferta. Nenhum sucesso confirmado.',
        cancelled: 'Resgate cancelado.'
      };
      fb.className = 'feedback-banner ' + (result.status === 'confirmed' ? 'feedback-success' : 'feedback-warning');
      fb.textContent = messages[result.status] || messages.unavailable;
      if (result.status === 'confirmed' && result.receipt) mergeNativeAccountRecords(account, [result.receipt]);
      if (['cancelled', 'busy'].includes(result.status)) { btn.style.display = 'flex'; btn.innerHTML = ICONS.tickets + ' Sim, Resgatar Agora'; }
      document.getElementById('btnConfirmOpenWeb').style.display = result.status === 'auth_required' ? 'flex' : 'none';
    }

    function updateSessionStatusUI() {
      const state = uolSessionPresentation();
      const label = document.getElementById('redemptionsSessionStatusLabel');
      const ind = document.getElementById('redemptionsSessionStatusInd');
      if (ind) {
        const dot = ind.querySelector('.session-dot');
        if (dot) dot.className = 'session-dot ' + state.dot;
      }
      if (label) label.textContent = state.label;
      const pill = document.getElementById('authModalStatusPill');
      if (pill) pill.innerHTML = '<span class="session-dot ' + state.dot + '"></span><span>' + state.label + '</span>';
      const detail = document.getElementById('authModalSessionDetail');
      if (detail) detail.textContent = state.detail;
      const button = document.getElementById('btnVerifyUolSession');
      if (button) button.disabled = currentSessionStatus === 'checking';
      const connect = document.getElementById('btnConnectHunterSession');
      if (connect) connect.disabled = currentSessionStatus === 'checking';
      updateCredsStatusUI();
    }

    function openMyRedemptionsModal() {
      switchTab('redemptions');
    }

    function closeMyRedemptionsModal(e) {
      if (e && e.stopPropagation) e.stopPropagation();
      const el = document.getElementById('myRedemptionsModal');
      if (el) el.style.display = 'none';
    }

    // ===================================================================
    // AJUSTES & FILTROS MODAL
    // ===================================================================
    function hunterAccessState() {
      const collection = INITIAL.hunterCollection || {};
      const access = INITIAL.hunterAccess || collection.access || {};
      const aliases = { active: 'approved', approved: 'approved', pending: 'access_pending', access_pending: 'access_pending',
        revoked: 'access_revoked', access_revoked: 'access_revoked', expired: 'access_expired', access_expired: 'access_expired',
        activation_required: 'activation_required', not_configured: 'activation_required', setup_unavailable: 'setup_unavailable' };
      const state = Object.prototype.hasOwnProperty.call(aliases, access.status) ? aliases[access.status]
        : Object.prototype.hasOwnProperty.call(aliases, collection.status) ? aliases[collection.status] : 'activation_required';
      return { state: state, role: access.role === 'owner' ? 'owner' : 'member' };
    }

    let hunterInstagramAction = null;

    function renderHunterAccessUI() {
      const access = hunterAccessState();
      const labels = {
        approved: ['Acesso aprovado', 'Este iPhone pode descobrir novidades e encaminhá-las ao servidor.'],
        access_pending: ['Aguardando aprovação', 'Compartilhe com o responsável o código mostrado ao solicitar acesso. Depois, verifique a aprovação.'],
        access_revoked: ['Acesso suspenso', 'A coleta está pausada neste iPhone. Solicite acesso novamente ao responsável.'],
        access_expired: ['Acesso expirado', 'Solicite uma nova aprovação para retomar a coleta neste iPhone.'],
        activation_required: ['Ativação necessária', 'Solicite acesso e compartilhe o código de aprovação com o responsável.'],
        setup_unavailable: ['Verificação indisponível', 'Não foi possível verificar o acesso agora. Tente novamente; seus dados foram preservados.']
      };
      const text = labels[access.state];
      const status = document.getElementById('hunterAccessStatus');
      const description = document.getElementById('hunterAccessDescription');
      const button = document.getElementById('btnHunterAccess');
      if (status) status.textContent = text[0];
      if (description) description.textContent = access.role === 'owner'
        ? 'Aprove ou suspenda aparelhos que podem colaborar com a coleta.' : text[1];
      if (button) {
        button.textContent = access.role === 'owner' ? 'Gerenciar aparelhos'
          : access.state === 'access_pending' ? 'Verificar aprovação'
          : access.state === 'approved' ? 'Verificar acesso' : 'Solicitar acesso';
        button.disabled = !INITIAL.nativeBrowser;
      }
      ['btnReconnectInstagram', 'btnCollectInstagram'].forEach(id => {
        const instagram = document.getElementById(id);
        if (!instagram) return;
        instagram.hidden = access.state !== 'approved' || !INITIAL.nativeBrowser;
        instagram.disabled = access.state !== 'approved' || !INITIAL.nativeBrowser || hunterInstagramAction !== null;
        instagram.textContent = id === 'btnReconnectInstagram'
          ? hunterInstagramAction === 'connect' ? 'Conectando…' : 'Conectar Instagram neste iPhone'
          : hunterInstagramAction === 'collect' ? 'Coletando Stories…' : 'Coletar Stories agora';
      });
      const diagnostic = document.getElementById('hunterInstagramStatus');
      if (diagnostic) {
        const source = INITIAL.hunterCollection && INITIAL.hunterCollection.sources && INITIAL.hunterCollection.sources.instagram;
        diagnostic.hidden = access.state !== 'approved';
        diagnostic.textContent = hunterInstagramAction !== null ? (hunterInstagramAction === 'connect' ? 'Instagram: aguardando login e consulta…' : 'Instagram: consultando Stories…')
          : source ? 'Instagram: ' + hunterStatusPresentation(source.status, source.found, source.reason, source).detail + '.'
          : 'Instagram: sessão ainda não verificada. Conecte uma vez; a coleta usa a sessão salva neste iPhone.';
      }
    }

    function manageHunterAccess() {
      if (!INITIAL.nativeBrowser) { showToast('Abra o Clube UOL Premium no Scriptable para gerenciar o acesso.'); return; }
      window.location.href = 'hunter-access://manage';
    }

    function reconnectHunterInstagram() {
      runHunterInstagramAction('connect');
    }

    function collectHunterInstagram() {
      runHunterInstagramAction('collect');
    }

    function runHunterInstagramAction(action) {
      if (!['connect', 'collect'].includes(action) || !INITIAL.nativeBrowser || hunterAccessState().state !== 'approved' || hunterInstagramAction !== null) return;
      if (action === 'collect' && INITIAL.hunterCollection?.sources?.instagram?.status === 'auth_required') {
        showHunterInstagramToast('Toque em “Conectar Instagram neste iPhone” antes de coletar Stories.', false);
        renderHunterAccessUI(); return;
      }
      hunterInstagramAction = action;
      renderHunterAccessUI();
      window.location.href = 'hunter-instagram://' + action;
    }

    function handleNativeInstagramResult(action, result) {
      if (!['connect', 'collect'].includes(action) || !result || typeof result !== 'object') return;
      hunterInstagramAction = null;
      if (['access_revoked', 'access_expired', 'access_pending', 'activation_required'].includes(result.status)) {
        INITIAL.hunterAccess = { ...(INITIAL.hunterAccess || {}), status: result.status };
      }
      const source = result.sources && result.sources.instagram;
      if (source) {
        const previous = INITIAL.hunterCollection || {};
        INITIAL.hunterCollection = { ...previous, ...result, sources: { ...(previous.sources || {}), ...result.sources } };
        setHunterCollectionStatus(INITIAL.hunterCollection);
      }
      renderHunterAccessUI();
      const verified = source && ['found', 'empty'].includes(source.status);
      const detail = source && hunterStatusPresentation(source.status, source.found, source.reason, source).detail;
      showHunterInstagramToast(verified ? 'Consulta ao Instagram concluída neste iPhone.'
        : source ? 'Instagram: ' + detail + '.'
        : result.status === 'busy' ? 'Outra operação está em andamento. Tente ao concluir.'
        : result.status === 'cancelled' ? 'Conexão cancelada. Captura não confirmada.'
        : result.status === 'auth_required' ? 'Entre no Instagram neste iPhone em Ajustes.'
        : 'Captura não confirmada. Confira o diagnóstico e o acesso deste iPhone.', verified);
      if (verified) streamLiveOffers(true);
    }

    function showHunterInstagramToast(message, verified) {
      showToast(message);
      const icon = document.getElementById('hubToast')?.querySelector?.('svg');
      if (icon && !verified) icon.innerHTML = '<path d="M12 5v9M12 18v1"/>';
    }

    // NAVIGATION_PERSONALIZATION_START
    const NAVIGATION_THEMES = [
      { id: 'default', label: 'Padrão', description: 'Vidro escuro · original' },
      { id: 'purple', label: 'Roxo', description: 'Vidro roxo · ícone laranja' },
      { id: 'orange', label: 'Laranja', description: 'Vidro laranja · ícone leitoso' },
      { id: 'white', label: 'Branco', description: 'Vidro branco · ícone roxo' }
    ];
    let savedNavigationTheme = 'default';
    let pendingNavigationTheme = null;

    function normalizeNavigationTheme(theme) {
      return NAVIGATION_THEMES.some(item => item.id === theme) ? theme : 'default';
    }

    function applyNavigationTheme(theme) {
      const value = normalizeNavigationTheme(theme);
      settings.navigationBarTheme = value;
      document.getElementById('bottomNav').setAttribute('data-navigation-theme', value);
      document.querySelectorAll('[data-theme-option]').forEach(button => {
        button.setAttribute('aria-pressed', String(button.getAttribute('data-theme-option') === value));
        button.disabled = pendingNavigationTheme !== null;
      });
    }

    function initializeNavigationTheme() {
      savedNavigationTheme = normalizeNavigationTheme(INITIAL.nativeBrowser
        ? INITIAL.navigationBarTheme : settings.navigationBarTheme);
      applyNavigationTheme(savedNavigationTheme);
    }

    function renderNavigationThemes() {
      const container = document.getElementById('navigationThemeOptions');
      if (!container.children.length) {
        const icons = ['navHome', 'navTickets', 'navHistory', 'navRedemptions'];
        container.innerHTML = NAVIGATION_THEMES.map(theme => `
          <button class="navigation-theme-option" data-theme-option="${theme.id}" aria-pressed="false">
            <span class="bottom-nav navigation-theme-preview" data-navigation-theme="${theme.id}" aria-hidden="true">
              ${icons.map((icon, index) => `<span class="bottom-nav-item${index === 0 ? ' active' : ''}"><span class="nav-icon-wrap">${ICONS[icon] || ''}</span><span class="active-dot"></span></span>`).join('')}
            </span>
            <span class="navigation-theme-label">${theme.label}<span class="navigation-theme-check" aria-hidden="true">✓</span></span>
            <span class="navigation-theme-description">${theme.description}</span>
          </button>`).join('');
        container.querySelectorAll?.('[data-theme-option]')?.forEach(button => {
          button.addEventListener?.('click', () => chooseNavigationTheme(button.getAttribute('data-theme-option')));
        });
      }
      applyNavigationTheme(settings.navigationBarTheme);
    }

    function chooseNavigationTheme(theme) {
      if (pendingNavigationTheme !== null || !NAVIGATION_THEMES.some(item => item.id === theme)) return;
      if (INITIAL.nativeBrowser) pendingNavigationTheme = theme;
      applyNavigationTheme(theme);
      const status = document.getElementById('navigationThemeStatus');
      if (INITIAL.nativeBrowser) {
        status.textContent = 'Salvando neste iPhone…';
        window.location.href = 'hunter-theme://set?value=' + theme;
      } else {
        try {
          if (saveSettingsToStorage() === false) throw new Error('storage unavailable');
          savedNavigationTheme = theme;
          status.textContent = 'Salvo neste navegador.';
        } catch (_) {
          applyNavigationTheme(savedNavigationTheme);
          status.textContent = 'Não foi possível salvar. A escolha anterior foi mantida.';
        }
      }
    }

    function handleNavigationThemeSaveResult(theme, saved) {
      if (pendingNavigationTheme !== theme) return;
      pendingNavigationTheme = null;
      if (saved) savedNavigationTheme = theme;
      applyNavigationTheme(savedNavigationTheme);
      if (saved) saveSettingsToStorage();
      document.getElementById('navigationThemeStatus').textContent = saved
        ? 'Salvo neste iPhone.' : 'Não foi possível salvar. A escolha anterior foi mantida.';
    }

    function selectSettingsTab(tab) {
      const personalization = tab === 'personalization';
      document.getElementById('settingsGeneralPanel').hidden = personalization;
      document.getElementById('settingsPersonalizationPanel').hidden = !personalization;
      document.getElementById('settingsGeneralTab').setAttribute('aria-selected', String(!personalization));
      document.getElementById('settingsPersonalizationTab').setAttribute('aria-selected', String(personalization));
      if (personalization) renderNavigationThemes();
    }
    // NAVIGATION_PERSONALIZATION_END

    function openSettingsModal() {
      document.getElementById('inputPriorizar').value = (settings.priorizarWords || []).join(', ');
      document.getElementById('inputDespriorizar').value = (settings.despriorizarWords || []).join(', ');

      selectUser(settings.currentUser || 'leosaquetto', false);
      updateCredsStatusUI();
      renderHunterAccessUI();
      selectSettingsTab('general');
      renderNavigationThemes();

      const verLabel = document.getElementById('currentVersionLabel');
      if (verLabel) verLabel.textContent = 'v' + SCRIPT_VERSION;

      const modal = document.getElementById('settingsModal');
      modal.style.display = 'flex';
      enterOwnedModal(modal, closeSettingsModal);
    }

    function closeSettingsModal(e) {
      if (e && e.stopPropagation) e.stopPropagation();
      const modal = document.getElementById('settingsModal');
      modal.style.display = 'none';
      leaveOwnedModal(modal);
    }

    let pendingAccountPreference = null;
    function persistNativeAccountChoice(user, previous) {
      if (!INITIAL.nativeAccounts || user === previous) return;
      pendingAccountPreference = { user, previous };
      window.location.href = 'hunter-preference://account?value=' + user;
    }

    function handleNativePreferenceResult(value, saved) {
      const pending = pendingAccountPreference;
      if (!pending || pending.user !== value) return;
      pendingAccountPreference = null;
      if (saved) return;
      selectUser(pending.previous, false);
      saveSettingsToStorage();
      updateCredsStatusUI();
      showToast('Não foi possível salvar a conta ativa. A seleção anterior foi restaurada.');
    }

    function selectUser(user, autoSave = true) {
      if (!['leosaquetto', 'gaoriel'].includes(user)) return;
      const previous = settings.currentUser || 'leosaquetto';
      if (settings.currentUser !== user) {
        if (document.getElementById('voucherModal')?.style.display === 'flex') closeVoucherModal();
        if (document.getElementById('detailsModal')?.style.display === 'flex') closeDetailsModal();
      }
      settings.currentUser = user;
      const pLeo = document.getElementById('pillUserLeo');
      if (pLeo) pLeo.classList.toggle('active', user === 'leosaquetto');
      const pGao = document.getElementById('pillUserGao');
      if (pGao) pGao.classList.toggle('active', user === 'gaoriel');
      updateUserAvatar();
      if (autoSave) {
        saveSettingsToStorage();
        persistNativeAccountChoice(user, previous);
      }
      if (currentTab === 'redemptions') {
        renderRedemptionsScreen();
      }
    }

    // ===================================================================
    // ALTERNÂNCIA DE CONTAS & CONFIRMAÇÃO DE SESSÃO HEADLESS
    // ===================================================================
    let pendingSwitchUser = null;

    function requestUserSwitch(targetUser) {
      const curr = settings.currentUser || 'leosaquetto';
      if (targetUser === curr) {
        const pLeo = document.getElementById('pillUserLeo');
        if (pLeo) pLeo.classList.toggle('active', curr === 'leosaquetto');
        const pGao = document.getElementById('pillUserGao');
        if (pGao) pGao.classList.toggle('active', curr === 'gaoriel');
        return;
      }
      pendingSwitchUser = targetUser;
      openConfirmSwitchModal(targetUser);
    }

    function openConfirmSwitchModal(targetUser) {
      const isTargetLeo = (targetUser === 'leosaquetto');
      const targetDisplayName = isTargetLeo ? 'Leonardo Saquetto' : 'Gabriel';
      const targetHandle = isTargetLeo ? 'leosaquetto' : 'gaoriel';
      const currUser = settings.currentUser || 'leosaquetto';
      const isCurrLeo = (currUser === 'leosaquetto');
      const currDisplayName = isCurrLeo ? 'Leonardo Saquetto' : 'Gabriel';
      const currHandle = isCurrLeo ? 'leosaquetto' : 'gaoriel';

      const targetNameEl = document.getElementById('switchTargetDisplayName');
      if (targetNameEl) targetNameEl.textContent = targetDisplayName;

      const preview = document.getElementById('switchAccountsPreview');
      if (preview) {
        preview.innerHTML = `
          <div class="switch-acc-side">
            <img class="switch-acc-avatar" src="${getAvatarUrl(currHandle)}" />
            <div class="switch-acc-name">${currDisplayName.split(' ')[0]}</div>
            <span class="switch-acc-badge badge-curr">Atual</span>
          </div>
          <div class="switch-acc-arrow">
            <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
          </div>
          <div class="switch-acc-side">
            <img class="switch-acc-avatar target" src="${getAvatarUrl(targetHandle)}" />
            <div class="switch-acc-name">${targetDisplayName.split(' ')[0]}</div>
            <span class="switch-acc-badge badge-next">Próxima</span>
          </div>
        `;
        preview.querySelectorAll?.('img').forEach(image => image.addEventListener?.('error', () => {
          image.src = 'https://i.imgur.com/UdIgTfI.png';
        }, { once: true }));
      }

      const credsItem = document.getElementById('switchCredsStatusText');
      const accounts = credentials && credentials.accounts ? credentials.accounts : {};
      const targetCreds = accounts[targetUser] || {};
      const hasCreds = Boolean(targetCreds.hasLogin && targetCreds.hasPassword);

      if (credsItem) {
        if (hasCreds) {
          const maskedLogin = targetCreds.maskedLogin || '••••••';
          credsItem.innerHTML = `<strong>Credenciais prontas (${escapeReceiptHtml(maskedLogin)}):</strong> Salvas em <code>clubeuolscrip.txt</code>. A sessão será conferida antes de qualquer resgate.`;
        } else {
          credsItem.innerHTML = `<strong>Aviso de credenciais:</strong> Nenhuma credencial específica de ${targetDisplayName} em <code>clubeuolscrip.txt</code>. A sessão dependerá dos cookies locais.`;
        }
      }

      const modal = document.getElementById('confirmSwitchUserModal');
      if (modal) { modal.style.display = 'flex'; enterOwnedModal(modal, closeConfirmSwitchModal); }
    }

    function closeConfirmSwitchModal() {
      pendingSwitchUser = null;
      const modal = document.getElementById('confirmSwitchUserModal');
      if (modal) { modal.style.display = 'none'; leaveOwnedModal(modal); }
    }

    async function executeUserSwitch() {
      if (!pendingSwitchUser) return;
      const targetUser = pendingSwitchUser;
      const prevUser = settings.currentUser || 'leosaquetto';

      if (nativeRedemptionPending) { showToast('Aguarde a confirmação da tentativa atual.'); return; }
      if (document.getElementById('voucherModal')?.style.display === 'flex') closeVoucherModal();
      if (document.getElementById('detailsModal')?.style.display === 'flex') closeDetailsModal();
      // Selection changes presentation only. Native requests keep separate verified cookie jars.
      settings.currentUser = targetUser;
      currentSessionStatus = 'unknown'; currentSessionReason = '';
      updateSessionStatusUI();

      // 5. Salva configurações e atualiza interface
      saveSettingsToStorage();
      persistNativeAccountChoice(targetUser, prevUser);
      updateUserAvatar();
      updateCredsStatusUI();

      const pLeo = document.getElementById('pillUserLeo');
      if (pLeo) pLeo.classList.toggle('active', targetUser === 'leosaquetto');
      const pGao = document.getElementById('pillUserGao');
      if (pGao) pGao.classList.toggle('active', targetUser === 'gaoriel');

      closeConfirmSwitchModal();
      closeSettingsModal();

      if (currentTab === 'redemptions') {
        renderRedemptionsScreen();
      }

      const isTargetLeo = (targetUser === 'leosaquetto');
      const newName = isTargetLeo ? 'Leonardo Saquetto' : 'Gabriel';
      showToast(`Sessão alternada para ${newName} (@${targetUser})`);

      // Sincroniza resgates reais para o novo usuário
      fetchRealRedemptions();
    }

    function toggleUserProfile() {
      const next = (settings.currentUser === 'leosaquetto' ? 'gaoriel' : 'leosaquetto');
      requestUserSwitch(next);
    }

    // ===================================================================
    // ASSISTENTE DE LOGIN & CONEXÃO UOL
    // ===================================================================
    function openAuthAssistantModal() {
      const user = settings.currentUser || 'leosaquetto';
      const isLeo = (user !== 'gaoriel');
      const name = isLeo ? 'Leonardo Saquetto' : 'Gabriel';
      const handle = isLeo ? 'leosaquetto' : 'gaoriel';
      const accounts = credentials && credentials.accounts ? credentials.accounts : {};
      const userCreds = accounts[user] || {};

      const avatar = document.getElementById('authModalAvatar');
      if (avatar) avatar.src = getAvatarUrl(handle);
      const nameEl = document.getElementById('authModalName');
      if (nameEl) nameEl.textContent = name;
      const handleEl = document.getElementById('authModalHandle');
      if (handleEl) handleEl.textContent = '@' + handle + ' • Perfil Ativo';

      updateSessionStatusUI();

      const loginVal = document.getElementById('authModalLoginVal');
      if (loginVal) {
        loginVal.textContent = userCreds.hasLogin ? userCreds.maskedLogin || '••••••' : 'Não configurado';
        loginVal.title = userCreds.hasLogin ? 'Mostrar login no Scriptable' : '';
        loginVal.tabIndex = userCreds.hasLogin ? 0 : -1;
        loginVal.setAttribute?.('role', userCreds.hasLogin ? 'button' : 'text');
        loginVal.onclick = userCreds.hasLogin ? showAuthLogin : null;
        loginVal.onkeydown = userCreds.hasLogin ? event => {
          if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); showAuthLogin(); }
        } : null;
      }

      const passVal = document.getElementById('authModalPassVal');
      if (passVal) passVal.textContent = userCreds.hasPassword ? '••••••••' : 'Não configurado';

      const modal = document.getElementById('authAssistantModal');
      if (modal) { modal.style.display = 'flex'; enterOwnedModal(modal, closeAuthAssistantModal); }
    }

    function closeAuthAssistantModal() {
      const modal = document.getElementById('authAssistantModal');
      if (modal) { modal.style.display = 'none'; leaveOwnedModal(modal); }
    }

    let pendingNativeSecret = null;
    function requestNativeSecret(action, redirect = '') {
      const user = settings.currentUser || 'leosaquetto';
      if (!INITIAL.nativeAccounts || !['leosaquetto', 'gaoriel'].includes(user)) {
        showToast('Abra no Scriptable para consultar os dados da conta.');
        return;
      }
      if (pendingNativeSecret) { showToast('Aguarde a ação anterior terminar.'); return; }
      pendingNativeSecret = { action, account: user, redirect };
      window.location.href = 'hunter-secret://' + action + '?account=' + user;
    }

    function showAuthLogin() {
      requestNativeSecret('show-login');
    }

    function toggleAuthPassVisibility(btn) {
      requestNativeSecret('show');
    }

    function copyAuthLogin(btn) {
      requestNativeSecret('copy-login');
    }

    function copyAuthPassword(btn) {
      requestNativeSecret('copy');
    }

    function handleNativeSecretResult(action, account, result) {
      const pending = pendingNativeSecret;
      if (!pending || pending.action !== action || pending.account !== account) return;
      pendingNativeSecret = null;
      if (result?.status === 'copied') {
        showToast(action === 'copy-login' ? 'Login copiado pelo Scriptable.' : 'Senha copiada pelo Scriptable.');
        if (action === 'copy-login' && pending.redirect && (settings.currentUser || 'leosaquetto') === account) {
          setTimeout(() => {
            if ((settings.currentUser || 'leosaquetto') !== account) return;
            if (pending.redirect === 'logout') {
              const dest = encodeURIComponent('https://conta.uol.com.br/login?dest=https%3A%2F%2Fclube.uol.com.br%2Fperfil%2Fbeneficios&t=clubeuol');
              window.location.href = 'https://conta.uol.com.br/logout?dest=' + dest;
            } else if (pending.redirect === 'direct') {
              window.location.href = 'https://conta.uol.com.br/login?dest=' + encodeURIComponent('https://clube.uol.com.br/perfil/beneficios') + '&t=clubeuol';
            }
          }, 700);
        }
      } else if (result?.status === 'shown') {
        showToast(action === 'show-login' ? 'Login exibido pelo Scriptable.' : 'Senha exibida pelo Scriptable.');
      } else if (result?.status === 'missing') {
        showToast(action.endsWith('-login') ? 'Login não configurado para este perfil.' : 'Senha não configurada para este perfil.');
      } else if (result?.status !== 'cancelled') {
        showToast('Não foi possível concluir a ação no Scriptable.');
      }
    }

    function openUolLogoutAndLogin() {
      requestNativeSecret('copy-login', 'logout');
    }

    function openUolDirectLogin() {
      requestNativeSecret('copy-login', 'direct');
    }

    function reconnectUolSession() {
      const account = settings.currentUser || 'leosaquetto';
      if (!INITIAL.nativeAccounts || nativeHistoryPending.has(account)) return;
      nativeHistoryPending.add(account);
      currentSessionStatus = 'checking'; currentSessionReason = ''; updateSessionStatusUI();
      window.location.href = 'hunter-account://reconnect?account=' + account;
    }

    function showToast(message) {
      let toast = document.getElementById('hubToast');
      if (!toast) {
        toast = document.createElement('div');
        toast.id = 'hubToast';
        toast.className = 'hub-toast';
        document.body.appendChild(toast);
      }
      toast.innerHTML = '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="#ffb300" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg> <span></span>';
      toast.querySelector('span').textContent = String(message || '');
      toast.classList.add('show');
      clearTimeout(window._toastTimeout);
      window._toastTimeout = setTimeout(() => {
        toast.classList.remove('show');
      }, 2600);
    }

    function showAvatarFallback(img) {
      img.style.display = 'none';
      const fb = document.getElementById('headerUserFallback');
      if (fb) {
        fb.style.display = 'flex';
        fb.textContent = (settings.currentUser === 'gaoriel' ? 'GR' : 'LS');
      }
    }

    function updateUserAvatar() {
      const avatarEl = document.getElementById('headerUserAvatar');
      const fb = document.getElementById('headerUserFallback');
      const user = settings.currentUser || 'leosaquetto';
      if (avatarEl) {
        avatarEl.style.display = 'block';
        avatarEl.src = getAvatarUrl(user);
      }
      if (fb) {
        fb.style.display = 'none';
        fb.textContent = (user === 'gaoriel' ? 'GR' : 'LS');
      }
    }

    function updateCredsStatusUI() {
      const box = document.getElementById('credsStatusBox');
      if (!box) return;
      const user = settings.currentUser || 'leosaquetto';
      const accounts = credentials && credentials.accounts ? credentials.accounts : {};
      const userCreds = accounts[user] || {};
      const hasCreds = Boolean(userCreds.hasLogin && userCreds.hasPassword);
      const name = (user === 'gaoriel' ? 'Gabriel' : 'Leonardo');

      const state = uolSessionPresentation();
      const statusHtml = '<span style="color: ' + (currentSessionStatus === 'valid' ? '#10b981' : '#94a3b8') + '; font-weight: 600;">' + state.label + '</span>';

      if (hasCreds) {
        const masked = userCreds.maskedLogin || '••••••';
        box.innerHTML = `
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
            <div><strong>Perfil Ativo:</strong> ${name} (@${user})</div>
            ${statusHtml}
          </div>
          <div style="font-size: 11px; color: #94a3b8; margin-bottom: 8px;">
            <strong>Credenciais:</strong> ${escapeReceiptHtml(masked)} (clubeuolscrip.txt)
          </div>
          <button data-open-auth style="width: 100%; background: rgba(255, 179, 0, 0.15); border: 1px solid rgba(255, 179, 0, 0.35); color: #ffb300; font-size: 11.5px; font-weight: 700; padding: 7px 12px; border-radius: 8px; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 6px;">
            <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
            Reconectar / Assistente de Login UOL
          </button>
        `;
        box.querySelector?.('[data-open-auth]')?.addEventListener?.('click', openAuthAssistantModal);
      } else if (credentials && credentials.source) {
        box.innerHTML = '🟢 <strong>Credenciais configuradas:</strong> Fonte: ' + escapeReceiptHtml(credentials.source);
      } else {
        box.innerHTML = '⚪ Nenhuma credencial automática encontrada em <code>clubeuolscrip.txt</code>.';
      }
    }

    function saveSettingsFromModal() {
      const prioStr = document.getElementById('inputPriorizar').value;
      const desprioStr = document.getElementById('inputDespriorizar').value;

      settings.priorizarWords = prioStr.split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
      settings.despriorizarWords = desprioStr.split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
      saveSettingsToStorage();

      closeSettingsModal();
      renderOffers(historyData.active || []);
    }

    // ===================================================================
    // ATUALIZAÇÃO AUTOMÁTICA & MANUAL VIA GITHUB RAW (OTA)
    // ===================================================================
    const SCRIPT_VERSION = INITIAL.scriptVersion || "5.5.0";

    async function checkForScriptUpdateManual() {
      const btn = document.getElementById('btnCheckUpdate');
      const msg = document.getElementById('updateStatusMsg');
      if (btn && btn.disabled) return;
      if (!INITIAL.nativeBrowser) {
        if (msg) { msg.style.display = 'block'; msg.textContent = 'Abra o Clube UOL Premium no Scriptable para atualizar.'; }
        return;
      }
      if (btn) btn.disabled = true;
      if (msg) {
        msg.style.display = 'block';
        msg.style.color = '#94a3b8';
        msg.textContent = 'Verificando atualizações no GitHub...';
      }
      // One native request validates and installs; the WebView never downloads
      // another copy of the script or opens a competing Scriptable execution.
      window.location.href = 'hunter-update://check';
    }

    function handleNativeUpdateResult(updated) {
      const btn = document.getElementById('btnCheckUpdate');
      const msg = document.getElementById('updateStatusMsg');
      if (btn) btn.disabled = false;
      if (msg) {
        msg.style.display = 'block'; msg.style.color = updated ? '#00e676' : '#94a3b8';
        msg.textContent = updated ? 'Atualização instalada. A nova versão está abrindo.'
          : 'Verificação concluída. O script instalado foi preservado.';
      }
    }

    function triggerScriptUpdate() { return checkForScriptUpdateManual(); }

    // ===================================================================
    // LINHA DO TEMPO DE ATIVIDADES / NOTIFICAÇÕES (MODAL HEADER)
    // ===================================================================
    let currentTimelineFilter = 'all';

    function getTimelineEvents() {
      const events = [];
      const now = Date.now();
      const allOffers = [...(historyData.active || []), ...(historyData.history || [])];
      const imagesByGroup = new Map();
      allOffers.forEach(offer => {
        const key = getGroupKey(offer);
        const records = imagesByGroup.get(key) || [];
        records.push(offer);
        imagesByGroup.set(key, records);
      });
      const timelineOfferImage = offer => offerImage([offer, ...(imagesByGroup.get(getGroupKey(offer)) || [])]);

      // 1. Ofertas Ativas (Novas)
      (historyData.active || []).forEach(o => {
        const isStoriesOnly = isStoriesOnlyOffer(o);
        const isStory = isStoriesOnly || Boolean(o.hasStory || o.source === 'Stories' || o.badge === 'Stories');
        const isPortal = isStoriesOnly ? false : (o.isPortal !== false);
        let srcLabel = 'Portal';
        if (isPortal && isStory) srcLabel = 'Portal e Stories';
        else if (isStory) srcLabel = 'Stories';

        const catDef = CATEGORY_MAP[o.category] || CATEGORY_MAP['OUTROS'];
        events.push({
          id: 'nova-' + o.id,
          offerId: o.id,
          type: 'nova',
          title: o.rawTitle || o.title,
          badgeText: isStory ? 'Nova Stories' : 'Nova Portal',
          desc: `Descoberta via ${srcLabel} • ${catDef.label}`,
          timestamp: captureTime(o) || null,
          imageUrl: timelineOfferImage(o),
          source: srcLabel
        });
      });

      // 2. Ofertas do Histórico (Esgotadas e Descobertas)
      (historyData.history || []).forEach(o => {
        const expTime = confirmedSoldOutTime(o);
        const capturedAt = captureTime(o);
        if (expTime) {
          let durationText = '';
          if (capturedAt && expTime > capturedAt) {
            durationText = ` • Tempo no ar: ${formatStopwatchDuration(capturedAt, expTime)}`;
          }
          events.push({
            id: 'esgotada-' + o.id,
            offerId: o.id,
            type: 'esgotada',
            title: o.rawTitle || o.title,
            badgeText: 'Esgotada',
            desc: `Ingressos esgotados no Clube UOL${durationText}`,
            timestamp: expTime,
            imageUrl: timelineOfferImage(o)
          });
        }

        if (capturedAt) {
          const isStoriesOnly = isStoriesOnlyOffer(o);
          const isStory = isStoriesOnly || Boolean(o.hasStory || o.source === 'Stories' || o.badge === 'Stories');
          const isPortal = isStoriesOnly ? false : (o.isPortal !== false);
          let srcLabel = 'Portal';
          if (isPortal && isStory) srcLabel = 'Portal e Stories';
          else if (isStory) srcLabel = 'Stories';

          const catDef = CATEGORY_MAP[o.category] || CATEGORY_MAP['OUTROS'];
          events.push({
            id: 'hist-nova-' + o.id,
            offerId: o.id,
            type: 'nova',
            title: o.rawTitle || o.title,
            badgeText: isStory ? 'Nova Stories' : 'Nova Portal',
            desc: `Descoberta via ${srcLabel} • ${catDef.label}`,
            timestamp: capturedAt,
            imageUrl: timelineOfferImage(o),
            source: srcLabel
          });
        }
      });

      // Future availability transitions are append-only; older missing transitions
      // cannot be reconstructed from the current offer state.
      const availabilityEvents = typeof window !== 'undefined' && Array.isArray(window.hunterAvailabilityEvents)
        ? window.hunterAvailabilityEvents : [];
      const offerById = new Map(allOffers.map(offer => [String(offer.id), offer]));
      const seenSoldOut = new Set(events.filter(event => event.type === 'esgotada' && event.timestamp)
        .map(event => String(event.offerId) + '|' + event.timestamp));
      availabilityEvents.forEach(entry => {
        if (!entry || !['sold_out', 'restocked'].includes(entry.eventType)) return;
        const timestamp = catalogTime(entry.occurredAt);
        if (!timestamp || timestamp > now + 300000) return;
        const offerId = String(entry.offerId || '');
        if (!offerId || entry.eventType === 'sold_out' && seenSoldOut.has(offerId + '|' + timestamp)) return;
        const offer = offerById.get(offerId);
        events.push({
          id: 'transition-' + entry.eventType + '-' + offerId + '-' + timestamp,
          offerId: offer ? offer.id : null,
          type: entry.eventType === 'sold_out' ? 'esgotada' : 'reaberta',
          title: offer?.rawTitle || offer?.title || entry.title || 'Oferta do Clube UOL',
          badgeText: entry.eventType === 'sold_out' ? 'Esgotada' : 'Reaberta',
          desc: entry.eventType === 'sold_out' ? 'Sessão esgotada no Clube UOL' : 'Sessão disponível novamente no Clube UOL',
          timestamp,
          imageUrl: offer ? timelineOfferImage(offer) : entry.imageUrl || ''
        });
      });

      // 3. Resgates Realizados (Filtrados para Campanhas de Ingressos)
      const rawRedemptions = getAllRedemptionsList();
      const redList = rawRedemptions.filter(r => (r.partner || '').trim().toLowerCase() === 'campanhas de ingressos');
      redList.forEach((r, index) => {
        const userHandle = String(r.user || r.accountId || '').trim().toLowerCase();
        const userName = userHandle === 'leosaquetto' ? 'Leonardo' : userHandle === 'gaoriel' ? 'Gabriel' : 'Conta não informada';
        const moment = parseRedemptionMoment(r.redeemedAt || r.date);
        const campaign = catalogCampaign(r.campaignUrl || r.link || r.url);
        const associated = campaign ? allOffers.filter(o => {
          const candidate = catalogCampaign(o.link);
          return candidate && candidate.key === campaign.key;
        }) : [];
        events.push({
          id: 'resgate-' + (redemptionIdentity(r) || 'unidentified-' + index),
          type: 'resgate',
          title: formatRedemptionTitle(r.title),
          badgeText: 'Resgate',
          desc: `${userName} • ${r.partner || 'Campanhas de Ingressos'}`,
          avatarUrl: ['leosaquetto', 'gaoriel'].includes(userHandle) ? getAvatarUrl(userHandle) : '',
          timestamp: moment ? moment.timestamp : null,
          datePrecision: moment ? moment.precision : null,
          dateLabel: moment ? moment.label : '',
          imageUrl: validOfferImage(r.thumb) || offerImage(associated),
          redemption: r
        });
      });

      // Datas desconhecidas permanecem visíveis ao final, sem recência inventada.
      const dated = events.filter(event => event.timestamp).sort((a, b) => b.timestamp - a.timestamp);
      const undated = events.filter(event => !event.timestamp);
      return [...dated.slice(0, 40), ...undated];
    }

    function updateTimelineBadgeDot() {
      const dot = document.getElementById('timelineBadgeDot');
      if (!dot) return;
      const events = getTimelineEvents();
      if (events.length === 0) {
        dot.style.display = 'none';
        return;
      }
      let lastSeenTs = 0;
      try {
        lastSeenTs = parseInt(localStorage.getItem('uol_last_seen_timeline_ts') || '0', 10);
      } catch(e) {}

      const hasUnseen = events.some(ev => ev.timestamp > lastSeenTs);
      dot.style.display = hasUnseen ? 'block' : 'none';
    }

    function openActivityTimelineModal() {
      const modal = document.getElementById('activityTimelineModal');
      if (!modal) return;
      currentTimelineFilter = 'all';
      renderTimelineList();
      modal.style.display = 'flex';
      enterOwnedModal(modal, closeActivityTimelineModal);

      try {
        localStorage.setItem('uol_last_seen_timeline_ts', String(Date.now()));
      } catch(e) {}
      const dot = document.getElementById('timelineBadgeDot');
      if (dot) dot.style.display = 'none';
    }

    function closeActivityTimelineModal(e) {
      if (e && e.stopPropagation) e.stopPropagation();
      const modal = document.getElementById('activityTimelineModal');
      if (modal) { modal.style.display = 'none'; leaveOwnedModal(modal); }
    }

    function setTimelineFilter(filter) {
      currentTimelineFilter = filter;
      renderTimelineList();
    }

    function renderTimelineList() {
      const listEl = document.getElementById('timelineList');
      if (!listEl) return;

      const allEvents = getTimelineEvents();
      const countAll = allEvents.length;
      const countNew = allEvents.filter(e => e.type === 'nova' || e.type === 'reaberta').length;
      const countExp = allEvents.filter(e => e.type === 'esgotada').length;
      const countRed = allEvents.filter(e => e.type === 'resgate').length;

      const elAll = document.getElementById('tlCountAll');
      const elNew = document.getElementById('tlCountNew');
      const elExp = document.getElementById('tlCountExpired');
      const elRed = document.getElementById('tlCountRedemption');
      if (elAll) elAll.textContent = countAll;
      if (elNew) elNew.textContent = countNew;
      if (elExp) elExp.textContent = countExp;
      if (elRed) elRed.textContent = countRed;

      const btnAll = document.getElementById('tlFilterAll');
      const btnNew = document.getElementById('tlFilterNew');
      const btnExp = document.getElementById('tlFilterExpired');
      const btnRed = document.getElementById('tlFilterRedemption');
      if (btnAll) btnAll.classList.toggle('active', currentTimelineFilter === 'all');
      if (btnNew) btnNew.classList.toggle('active', currentTimelineFilter === 'nova');
      if (btnExp) btnExp.classList.toggle('active', currentTimelineFilter === 'esgotada');
      if (btnRed) btnRed.classList.toggle('active', currentTimelineFilter === 'resgate');
      [[btnAll, 'all'], [btnNew, 'nova'], [btnExp, 'esgotada'], [btnRed, 'resgate']].forEach(([button, filter]) => {
        if (button) button.setAttribute('aria-pressed', String(currentTimelineFilter === filter));
      });

      let filtered = allEvents;
      if (currentTimelineFilter !== 'all') {
        filtered = allEvents.filter(e => e.type === currentTimelineFilter ||
          currentTimelineFilter === 'nova' && e.type === 'reaberta');
      }

      if (filtered.length === 0) {
        listEl.innerHTML = `
          <div style="text-align: center; padding: 32px 16px; color: #64748b; font-size: 13px;">
            Nenhum evento registrado nesta categoria.
          </div>
        `;
        return;
      }

      const now = Date.now();
      let html = '';
      filtered.forEach((ev, index) => {
        let iconHtml = '';
        let badgeClass = '';
        if (ev.type === 'nova' || ev.type === 'reaberta') {
          iconHtml = `<div class="timeline-icon-box type-nova"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="16"/><line x1="8" y1="12" x2="16" y2="12"/></svg></div>`;
          badgeClass = 'badge-nova';
        } else if (ev.type === 'esgotada') {
          iconHtml = `<div class="timeline-icon-box type-esgotada"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg></div>`;
          badgeClass = 'badge-esgotada';
        } else if (ev.type === 'resgate') {
          iconHtml = `<div class="timeline-icon-box type-resgate">${ICONS.tickets}</div>`;
          badgeClass = 'badge-resgate';
        }

        if (validOfferImage(ev.imageUrl)) iconHtml = iconHtml.replace('</div>', `<img class="timeline-thumbnail" src="${htmlAttribute(ev.imageUrl)}" alt="" loading="lazy" /></div>`);
        const timeStr = timelineTimeLabel(ev, now);
        const title = htmlAttribute(ev.title).replace(/\s+(?:([-–—])\s*)?(SP|RJ|BA|MG|PR|RS|SC|DF|PE|CE|GO|ES|AM|PA|RN|PB|AL|SE|MT|MS|TO|RO|RR|AP|AC|PI|MA)\s*$/, (_, separator, state) => '\u00a0' + (separator ? separator + '\u00a0' : '') + state);
        const actionable = Boolean(ev.offerId || ev.redemption);
        const avatar = ev.avatarUrl ? `<img class="timeline-user-avatar" src="${escapeReceiptHtml(ev.avatarUrl)}" alt="" /> ` : '';

        html += `
          <div class="timeline-item" ${actionable ? `data-timeline-index="${index}" role="button" tabindex="0"` : ''}>
            ${iconHtml}
            <div class="timeline-content">
              <div class="timeline-top-row">
                <span class="timeline-badge ${badgeClass}">${escapeReceiptHtml(ev.badgeText)}</span>
                <span class="timeline-time">${escapeReceiptHtml(timeStr)}</span>
              </div>
              <div class="timeline-item-title">${title}</div>
              <div class="timeline-item-desc">${avatar}${escapeReceiptHtml(ev.desc)}</div>
            </div>
          </div>
        `;
      });

      listEl.innerHTML = html;
      if (typeof listEl.querySelectorAll === 'function') {
        listEl.querySelectorAll('[data-timeline-index]').forEach(node => {
          const event = filtered[Number(node.getAttribute('data-timeline-index'))];
          const activate = () => {
            closeActivityTimelineModal();
            if (event.offerId) openDetails(event.offerId);
            else if (event.redemption) switchTab('redemptions');
          };
          node.addEventListener('click', activate);
          node.addEventListener('keydown', keyEvent => {
            if (keyEvent.key === 'Enter' || keyEvent.key === ' ') { keyEvent.preventDefault(); activate(); }
          });
        });
        listEl.querySelectorAll('.timeline-thumbnail').forEach(image => {
          image.addEventListener('error', () => image.remove(), { once: true });
        });
      }
    }

    window.addEventListener('hunter-availability-events', () => {
      updateTimelineBadgeDot();
      if (document.getElementById('activityTimelineModal')?.style.display === 'flex') renderTimelineList();
    });

    function handleBackdropClick(e) {
      if (!e) return;
      if (e.target && e.target.classList && e.target.classList.contains('modal-backdrop')) {
        closeDetailsModal();
        closeConfirmModal();
        closeConfirmSwitchModal();
        closeAuthAssistantModal();
        closeSettingsModal();
        closeMyRedemptionsModal();
        closeGroupSessionsModal();
        closeVoucherModal();
        closeRedemptionDetailsModal();
        closeActivityTimelineModal();
      }
    }

    window.addEventListener('click', (e) => {
      handleBackdropClick(e);
    });

