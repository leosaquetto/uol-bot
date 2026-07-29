# UOL Ingressos Discord Worker

Piloto isolado para monitorar exclusivamente links de
`/campanhasdeingresso/` no Clube UOL e, quando promovido para `live`, publicar
as ofertas em um webhook do Discord.

## Segurança do piloto

- Um alarme de Durable Object roda a cada 3 minutos sem consumir um dos cinco
  Cron Triggers já usados pelo Backstage.
- `DELIVERY_MODE` começa como `dry-run`.
- A primeira execução cria um baseline e não envia ofertas já existentes.
- O webhook e o token administrativo são secrets do Worker; nunca entram no
  repositório.
- O KV só é escrito ao criar o baseline ou quando o estado de uma oferta muda.
- O alarme usa cerca de 480 execuções e 960 escritas de linha por dia, abaixo
  de 1% das cotas gratuitas compartilhadas correspondentes.

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

## Promoção para live

Após validar coleta, CPU e baseline no Cloudflare:

1. criar o canal do Discord e copiar o webhook;
2. cadastrar `DISCORD_WEBHOOK_URL`;
3. executar um teste manual controlado;
4. alterar `DELIVERY_MODE` para `live`;
5. redeployar e acompanhar os logs das primeiras execuções.
