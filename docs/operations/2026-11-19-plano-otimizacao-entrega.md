# Plano de otimização da entrega (19/11/2026)

Origem: duas ofertas comuns presas 16 h em entrega incerta (timeout de 8 s do `sendPhoto`), `/readyz` em 503 e um aviso por hora. Corrigido no #164 (oferta comum incerta fecha sozinha; ingresso continua incerto). Prioridade do dono: **ingresso primeiro**; oferta comum também, desde que economize.

Tudo abaixo vem de dados de produção de 19/11 (`/dashboard.json`, `/decisions`). O que é inferência está marcado.

## Parte 1 — `remote_url` fora da frente (feito neste PR)

- Evidência: `remote_url` com 22 falhas seguidas ("failed to get HTTP URL content"), última vitória em 18/11 03:13 UTC. O Telegram não consegue baixar a imagem do CDN do Clube. Ingressos usam `discord_proxy` (`telegramImageRemoteStrategy`), então não são afetados; só oferta comum.
- Efeito: cada oferta comum nova gastava uma chamada ao Telegram na `remote_url` antes de cair no `upload`, e cada sonda do disjuntor (a cada 10 min) repetia isso.
- Mudança: a sonda da `remote_url` passa a esperar 6 h (`IMAGE_REMOTE_URL_COOLDOWN_MINUTES`, padrão 360). Continua se curando sozinha se o CDN voltar. As outras estratégias mantêm 10 min.
- Como medir depois do release: `remote_url.last_failure_at` em `imageDelivery.strategies` deve andar no máximo a cada 6 h.

## Parte 2 — medir a latência do ingresso (próxima, só leitura)

- Hoje: oferta comum leva ~90 s da detecção ao Telegram (mediana 60 s, p95 120 s em 38 ofertas/24 h). Parte vem de `MAIN_IMAGE_WAIT_SECONDS=60` e do ciclo de 30 s.
- Não temos a latência só de ingresso, que é a que importa. Medir primeiro (detecção → Discord → Telegram, por oferta de `campanhasdeingresso`) antes de mexer em qualquer espera.
- Só depois: decidir se ingresso pode pular a espera pela foto ou se ela já é rápida pelo proxy. Nada de mudança sem número.

## Parte 3 — origem das escritas da conta (investigação)

- Evidência: o Worker UOL escreveu 29,8 mil linhas em 19/11 até 16:44 BRT, mas a conta já estava em 62,8 mil de 100 mil. Inferência: ~33 mil vêm de outro Worker/DO da mesma conta. Não confirmado.
- Por que importa: a previsão da conta (`optionalWorkReason: quota_forecast`) é o que liga `maintenance_deferred` e fazia o aviso ir e voltar.
- Passos: (a) listar Workers/DOs da conta e as escritas de cada um (analytics do Cloudflare); (b) se for do UOL, cortar `sourceObservations` e `offerTouches` (~20 mil/dia previstos) sem tocar ingresso; (c) reduzir a leitura de `maintenanceLedger` (38% das leituras).
- Regra: nada que aumente escrita por scan (ver `AGENTS.md`).
