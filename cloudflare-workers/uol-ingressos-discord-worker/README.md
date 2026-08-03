# UOL Ingressos Discord Worker

Worker isolado de rollback para monitorar exclusivamente links de
`/campanhasdeingresso/` no Clube UOL e publicar novas ofertas em um webhook do
Discord.

O histórico técnico, as decisões e as validações estão registrados em
[`IMPLEMENTATION.md`](./IMPLEMENTATION.md).

## Segurança do piloto

- Quando explicitamente reativado, um alarme de Durable Object roda a cada 1
  minuto sem consumir Cron Triggers.
- `DELIVERY_MODE` está em `dry-run`: o fan-out ativo foi consolidado no Worker
  do Telegram e este Worker não publica mais mensagens.
- `COLLECTOR_ENABLED=false` remove o alarme: não há polling duplicado do UOL.
- A primeira execução cria um baseline e não envia ofertas já existentes.
- A identidade persistente usa `parceiro + código interno`; correções no restante
  do slug não geram uma segunda mensagem. Estados antigos equivalentes são
  reconciliados automaticamente antes de cada decisão.
- O webhook e o token administrativo são secrets do Worker; nunca entram no
  repositório.
- O KV só é escrito ao criar o baseline ou quando o estado de uma oferta muda.
- O alarme usa cerca de 1.440 execuções e 2.880 escritas de linha por dia:
  aproximadamente 1,44% das requisições e 2,88% das escritas de linha
  gratuitas compartilhadas correspondentes.

## Comandos

```bash
npm install
npm run check
npx wrangler secret put ADMIN_TOKEN
npx wrangler secret put DISCORD_WEBHOOK_URL
npm run deploy
```

O namespace `UOL_TICKETS_STATE` já está criado e associado no
`wrangler.jsonc`.

## Rotas

- `GET /health`: estado sanitizado; não rearma alarme nem expõe secrets/ofertas.
- `POST /run` e `POST /test`: retornam HTTP 410 enquanto
  `COLLECTOR_ENABLED=false`. Só voltam a executar após reativação deliberada;
  então exigem `Authorization: Bearer <ADMIN_TOKEN>`.

## Validação realizada

O baseline, a coleta automática, o consumo de CPU e o webhook foram validados
historicamente antes da promoção para `live`. O Worker está aposentado como
consumidor e serve apenas como código de rollback frio.
