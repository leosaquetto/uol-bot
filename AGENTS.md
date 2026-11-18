# Regras para agentes no uol-bot

Vale para Claude, Codex, Antigravity e qualquer outro agente. Escrito em 18/11/2026 a partir dos incidentes de 17 e 18/11 (PRs #152–#160).

## Antes de começar

- Leia o quadro `~/Developer/GitHub/_coordination/saquettoOS.md`. Outras sessões costumam mexer no Worker ao mesmo tempo.
- Trabalhe num worktree próprio: `git -C ~/Developer/GitHub/uol-bot worktree add ../uol-bot-wt/<agente>-<tarefa> -b <agente>/<tarefa> origin/main`. Não edite o checkout `uol-bot` (main) diretamente.
- Para testar no worktree, ligue o `node_modules` do checkout principal: `ln -s ~/Developer/GitHub/uol-bot/cloudflare-workers/uol-telegram-shadow-worker/node_modules node_modules`. Antes de remover o worktree, apague o link.
- `npm run check:ci` regrava `worker-startup.cpuprofile`. Não comite esse arquivo: `git checkout -- worker-startup.cpuprofile`.

## Merge e release

- Merge só com `safe-merge leosaquetto/uol-bot <pr> --squash`.
- **Merge não implanta.** O deploy é `npm run release` no checkout principal, depois de `git checkout main && git pull --ff-only` e com o CI do `main` verde.
- **O release precisa de aprovação explícita do dono**, a cada vez.
- O release publica o HEAD do `main` inteiro. Se outra sessão mesclou depois de você, um único release leva os dois PRs. Antes de rodar, confira `npx wrangler deployments list` e o quadro, para não haver dois releases ao mesmo tempo.
- A verificação pós-deploy espera até 150 s (`UOL_VERIFY_ATTEMPTS`, padrão 30). Se ainda assim falhar com `version_metadata_mismatch`, rode de novo só a verificação: `EXPECTED_VERSION_ID=<versão> EXPECTED_DELIVERY_MODE=live npm run postdeploy:check`.
- Mudanças só em `scripts/`, testes ou docs não precisam de release, mesmo que o `safe-merge` lembre do release.

## Custo zero: cota do Durable Object

- O gargalo do plano grátis são as **escritas** (cerca de 100 mil por dia; a previsão costuma ficar entre 60 e 65 mil). Nada de gravação extra por scan.
- Resolva em memória o que não precisa persistir, como a arte alternativa de uma oferta.
- Quando mexer em caminho quente, prove no teste de integração: compare `instance.storageUsage.rowsWritten` com e sem a mudança (veja `test-worker/offer-page-image.integration.test.js`).

## Pedidos para fora do Worker

- **Todo `fetch` para o Clube UOL ou o CloudFront dele precisa mandar `User-Agent: UOLTelegramCloudflare/1.0`.** O `fetch` do Worker não manda nenhum por padrão. Sem ele, `clube.uol.com.br` responde **405** e o CloudFront (`*.cloudfront.net/beneficios/…`) responde **403**. Isso derrubou a alternativa de imagem duas vezes (#157, #158) e deixou a sonda de esgotado cega (#159).
- O stub de `fetch` nos testes deve se comportar como o servidor real e recusar pedidos sem UA. Antes de confiar no stub, faça um pedido real com curl, com e sem UA.

## Imagem das ofertas no Telegram

- Quando a `imageUrl` da API dá 403, o upload usa a arte da página da oferta: a única URL em `/beneficios/`; `/beneficios_categorias/` é ignorada (`src/offer-page-image.js`).
- Para reabrir a foto de uma oferta que ficou só em texto (`text_timeout`, tentativas esgotadas): `POST /requeue-delivery {"id":"<id>","target":"main_image"}`. Isso edita a mesma mensagem. **Nunca use `target: "main"` para isso: reenvia a oferta como mensagem nova, duplicada.** Custa 2 gravações por oferta e precisa de aprovação do dono.

## Onde ver o estado (admin, token em `~/.config/uol-ticket-research/shadow-admin-token`)

| Rota | Serve para |
|---|---|
| `/readyz` | saúde e versão em produção |
| `/dashboard.json` | resumo; `recent` mostra só 8 ofertas |
| `/decisions?limit=100` | até 100 ofertas, **com** os campos de imagem (`mainMessageKind`, `telegramImageStrategy`, `mainImageUpgradeError`) |
| `/inventory?limit=300` | até 300 ofertas, com a sonda de esgotado (`ticketProbeLastResult`), **sem** os campos de imagem |

## Testes de integração (vitest)

- Ofertas com "teatro" no link ficam retidas 90 s para agrupamento. No teste, use `THEATER_GROUP_HOLD_SECONDS = "0"`.
- Ingresso só vai ao canal principal depois do Discord: o teste precisa de `discord_message_id`, `discord_sent_at` e `DISCORD_WEBHOOK_URL`.

## Ao terminar

- Registre toda falha (build, check, release, produção) no quadro: o que, causa, correção, prevenção.
- Avise o dono: `printf '%s' '<resumo>' | claude-notify --id <id-estável>`.
- O Main guard pode avisar "entrou no main sem PR" se a API do GitHub atrasar. Desde o #160 ele tenta de novo por ~60 s; se ainda assim acusar, confira `gh api repos/leosaquetto/uol-bot/commits/<sha>/pulls` antes de tratar como incidente.
