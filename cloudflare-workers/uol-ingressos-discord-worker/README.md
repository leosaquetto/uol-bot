# UOL Ingressos Discord Worker

Worker isolado para monitorar exclusivamente links de
`/campanhasdeingresso/` no Clube UOL e publicar novas ofertas em um webhook do
Discord.

O histórico técnico, as decisões e as validações estão registrados em
[`IMPLEMENTATION.md`](./IMPLEMENTATION.md).

## Segurança do piloto

- Um alarme de Durable Object roda a cada 1 minuto sem consumir um dos cinco
  Cron Triggers já usados pelo Backstage.
- `DELIVERY_MODE` está em `live` após a validação do baseline e do webhook.
- A primeira execução cria um baseline e não envia ofertas já existentes.
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

- `GET /health`: estado sanitizado, agenda o primeiro alarme e não expõe
  secrets ou ofertas privadas.
- `POST /run`: execução manual autenticada com `Authorization: Bearer
  <ADMIN_TOKEN>`.
- `POST /run?send=1`: execução manual com envio, destinada apenas ao teste
  controlado após configurar o webhook.
- `POST /test`: envia uma mensagem de teste ao webhook, sem alterar o estado
  das ofertas.

## Validação realizada

O baseline, a coleta automática, o consumo de CPU e o webhook foram validados
antes da promoção para `live`. A rota `/test` permanece disponível para
diagnóstico autenticado.
