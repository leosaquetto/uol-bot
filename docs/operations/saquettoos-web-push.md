# saquettoOS Web Push — 2026-11-04

Adaptadores implementados, ainda **não implantados/ativados**. ntfy permanece intacto. O servidor de destino está no Site `appgprj_6aeaa768703c8191876e7312c03db8c0`; código e publicação em `saquettoOS/web`.

Integrações deste delta:
- Sentinela UOL: cópia das notificações finais já enfileiradas, mesmas chaves do ledger; abre Clube UOL. A fila Web Push é separada da confirmação ntfy. O alarme existente contempla a fila por até 24 horas, sem permitir novo resgate.
- Drops: primeira confirmação de envio pessoal, ID do job já existente; aviso genérico, sem copiar mensagem ou JID. Não altera captura, destinatários ou confirmação do WhatsApp.
- Shadow worker: avisos operacionais existentes, texto sanitizado, mesmo ID do episódio; abre Home.

Módulo compartilhado em `services/saquetto-drops/src/saquettoos-push.mjs`, importado pelos Workers no bundle. Reutiliza settings/metadata/ledger existentes, sem migração de banco. Cada runtime exige `SAQUETTOOS_PUSH_ENABLED=1`, `SAQUETTOOS_PUSH_ORIGIN` e `SAQUETTOOS_PUSH_TOKEN` específico do canal. Ausência mantém transporte desativado. Nunca usar token de outro produtor.

Fila local limitada a 256 itens, retenção 7 dias e até 5 tentativas de ingresso idempotente. Os ticks existentes chamam também `/api/push/drain` para backoff do provedor. Aceitação HTTP não prova banner ou leitura. Não copiar `.env`/estado privado do host para o Git. Instalação Oracle deve levar apenas fontes alteradas; preservar SQLite, autenticação e o `macsend-relay.py` alterado por outra tarefa.

Validação desta rodada: 3 testes do novo adaptador; 24 testes de regressão notify/personal-ntfy/operations-notify; 9 testes Worker de operações. Sintaxe dos três entrypoints passou. Sentinela Worker: 12/13; `three transient offer failures block without reserving or redeeming` falha também em uma cópia isolada do HEAD anterior, com a mesma diferença `active` versus `blocked`. Não classificado como infraestrutura e não corrigido nesta frente.

Gates antes de ativar: comprovar franquia R$0, seguir release oficial de cada serviço e preservar alterações alheias; resolver/aceitar separadamente a falha preexistente da sentinela. A API Cloudflare disponível respondeu 403 para subscriptions/billing (OAuth sem Billing Read); não houve mudança de plano nem deploy de Worker nesta rodada. Nenhum produtor é considerado live só porque o Site aceitou testes sintéticos autenticados.
