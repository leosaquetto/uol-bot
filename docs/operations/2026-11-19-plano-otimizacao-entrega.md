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

### Resultado da Parte 2 (medido em 19/11, 300 ofertas do `/inventory`)

Tempo entre a oferta aparecer e a mensagem sair no canal principal do Telegram:

| Grupo | Amostras | Mediana | p90 | Saíram em até 20 s |
|---|---|---|---|---|
| Comum | 159 | 90 s | 267 s | 45 |
| Ingresso de teatro | 89 | 62 s | 120 s | 8 |
| Ingresso (outros) | 49 | 60 s | 91 s | 22 |

- Teatro: a espera de 90 s é proposital (agrupa as datas do mesmo show numa mensagem). Não mexer.
- Ingresso que não é teatro: 22 de 49 saem em até 20 s (estratégias `upload` e `file_id`: 1 a 6 s). Os outros 27 saem entre 60 e 92 s e ficam na estratégia `discord_proxy_edit` (texto no prazo de 60 s e foto editada depois). **Mais da metade dos ingressos simples espera o prazo inteiro de `MAIN_IMAGE_WAIT_SECONDS`.**
- Os tempos 60/90/120/150 s são múltiplos do ciclo de 30 s do Worker: a tentativa só repete a cada ciclo.
- Causa provável (hipótese, não confirmada): nessas ofertas a primeira tentativa de foto não acontece ou não tem URL utilizável (o Telegram não baixa o CDN do Clube; a imagem só chega pelo proxy do Discord), e o Worker espera até o prazo antes de mandar texto. A saúde das estratégias não mostra falhas recentes de `upload`/`discord_proxy`, o que reforça que a espera vem de não haver tentativa, e não de tentativa falhando.
- Próximo passo (Parte 2b): descobrir **por que** a primeira tentativa foi adiada, sem gravar nada extra (log do Worker por oferta: `imageUrl` vazio? proxy pronto? estratégia desligada?). Só então encurtar a espera para ingresso, por exemplo tentar o `upload` na hora quando o proxy ainda não existe, ou reduzir o prazo de 60 s só para ingresso. Qualquer mudança precisa de teste que prove que não aumenta escrita nem duplica mensagem.

### Resultado da Parte 2b — por que a foto é adiada (causa confirmada no código e nos dados)

- Os "ingressos outros" lentos são ofertas comuns de "Promoções do Mês" com "ingresso" no link (Beto Carrero, SeaWorld), não campanhas de ingresso. O mecanismo é o mesmo para qualquer oferta.
- A mensagem principal só tenta a foto se `agora ≤ visto + 60 s − 38 s − 8 s = visto + 14 s` (`photoMutationBudgetOpen` com `imageMutationDeadlineAt`; os 38 s são `MAIN_AMBIGUOUS_RETRY_SECONDS` (30) + o timeout de 8 s do Telegram, definidos no commit `491126da2`, para uma foto ambígua se resolver antes do texto de fallback e não duplicar).
- O Worker roda a cada 30 s. A primeira tentativa cai entre +0 e +30 s depois da oferta ser vista. Se cair depois de +14 s (cerca de 53% das vezes), a foto é pulada: aparece `main image deferred` na linha do tempo, o Worker espera o prazo de 60 s, manda texto e depois edita a foto (`discord_proxy_edit`). Isso dá as latências de 60/90/120 s.
- Confere com os números: 22 de 49 (45%) saíram em até 20 s, perto dos 14/30 = 47% esperados.
- Evidência da linha do tempo de `pyu-black-ate-50-off-no-beto-carrero-world`: visto 02:06:10, `main image deferred` 02:06:40 (+30 s), texto às 02:07:41 (+90 s).
- Não entendi ainda por que parte das ofertas não é entregue no mesmo ciclo em que é vista (pode ser o lote de `DELIVERY_BATCH_SIZE=4` em rajadas). Fica como pergunta aberta.

Proposta (Parte 2c, precisa de aprovação por mexer na proteção contra duplicata):
1. Separar a janela da foto do prazo do alerta: nova variável `MAIN_PHOTO_WINDOW_SECONDS` em vez de reaproveitar `MAIN_AMBIGUOUS_RETRY_SECONDS` (mínimo hoje 15 s, o que ainda deixa a janela em +29 s, menor que um ciclo de 30 s).
2. Com janela de +45 s, toda primeira tentativa de foto cabe no primeiro ciclo. O pior caso (foto ambígua perto do limite) continua virando "incerta", sem texto duplicado.
3. Teste de integração: oferta vista em t0, primeira tentativa em t0+30 s com `sendPhoto` ok deve sair como foto, sem `deferred`; e com `sendPhoto` em timeout, não pode sair texto depois.

## Parte 3 — origem das escritas da conta (investigação)

- Evidência: o Worker UOL escreveu 29,8 mil linhas em 19/11 até 16:44 BRT, mas a conta já estava em 62,8 mil de 100 mil. Inferência: ~33 mil vêm de outro Worker/DO da mesma conta. Não confirmado.
- Por que importa: a previsão da conta (`optionalWorkReason: quota_forecast`) é o que liga `maintenance_deferred` e fazia o aviso ir e voltar.
- Passos: (a) listar Workers/DOs da conta e as escritas de cada um (analytics do Cloudflare); (b) se for do UOL, cortar `sourceObservations` e `offerTouches` (~20 mil/dia previstos) sem tocar ingresso; (c) reduzir a leitura de `maintenanceLedger` (38% das leituras).
- Regra: nada que aumente escrita por scan (ver `AGENTS.md`).
