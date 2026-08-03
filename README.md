# UOL Bot

O sistema ativo de coleta e envio de ofertas é o Cloudflare Worker
`uol-telegram-shadow-pilot`, implementado em
[`cloudflare-workers/uol-telegram-shadow-worker`](cloudflare-workers/uol-telegram-shadow-worker/).
Ele consulta as fontes do Clube UOL, mantém o estado em um Durable Object com
SQLite e entrega para Telegram e Discord sem depender deste Mac.

As melhorias API-first presentes no checkout são locais até existir uma
publicação deliberada com Version ID e verificação pós-deploy. O endpoint de
produção não prova automaticamente que estas alterações locais já estão ativas.

## Operação ativa

- Código e runbook: [`cloudflare-workers/uol-telegram-shadow-worker/README.md`](cloudflare-workers/uol-telegram-shadow-worker/README.md)
- Health sanitizado: <https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/health>
- Teste rápido no Mac:

  ```bash
  cd cloudflare-workers/uol-telegram-shadow-worker
  npm test
  ```

- Validação completa: usar CI Ubuntu. `workerd` atual exige macOS 13.5+ e não
  inicia neste Mac com macOS 12.6:

  ```bash
  cd cloudflare-workers/uol-telegram-shadow-worker
  npm run check
  ```

Publicação, mudança de modo e alteração de secrets são ações operacionais
separadas. Um teste local bem-sucedido não prova que uma nova versão foi
publicada.

## Legado: recuperação fria, não operação paralela

Os programas Python, Mac/Playwright e Scriptable na raiz são preservados apenas
para diagnóstico histórico ou recuperação manual. Os workflows correspondentes
estão desativados em `.github/workflows-archive/`; portanto, fazer upload de um
snapshot não significa que uma oferta foi entregue.

Antes de executar qualquer recuperação legada:

1. colocar o Worker ativo em `shadow` e confirmar o modo;
2. remover o webhook do Telegram antes de usar um consumidor com `getUpdates`;
3. restaurar e validar conscientemente exatamente um consumidor legado;
4. confirmar credenciais, dependências e destino sem imprimir secrets;
5. executar uma oferta de teste e verificar a mensagem no destino.

O [`mac_uol_scraper.js`](mac_uol_scraper.js) só devolve `MAC_OK` depois de um
`workflow_dispatch` confirmado pelo GitHub com HTTP 204. Com o workflow
arquivado ou o dispatch desligado, ele devolve `MAC_FAIL` e código de saída 3.
Isso é intencional: snapshot salvo não é recibo de entrega.

Detalhes do caminho frio estão em
[`SHORTCUT_MAC_FALLBACK.md`](SHORTCUT_MAC_FALLBACK.md). O diagnóstico do guard
está em [`OPERACAO_MAC_INCIDENTE_RAPIDO.md`](OPERACAO_MAC_INCIDENTE_RAPIDO.md).

## Segurança

- Nunca colocar tokens em comandos compartilhados, Git, relatórios ou arquivos
  de auditoria.
- Carregar credenciais somente do ambiente local ou do Keychain.
- Falha de certificado TLS encerra aquela coleta; o legado não aceita
  `verify=False`.
- Arquivos produzidos em `run/` são estado local e ficam fora do Git.

## Testes rápidos do legado

Os testes não abrem navegador, não acessam a rede e não enviam ofertas:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
node --test tests/legacy_safety.test.js
```
