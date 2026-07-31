# Registro da migração do Telegram para Cloudflare

## Objetivo

Eliminar a dependência do Mac ligado e reduzir a latência entre a publicação de
uma oferta no Clube UOL e a notificação no Telegram.

## Arquitetura implantada

O Worker `uol-telegram-shadow-pilot` preserva o nome do piloto para reutilizar o
mesmo Durable Object e o baseline já validado. A execução é serializada por uma
única instância porque a listagem global do Clube UOL é uma única unidade de
coordenação.

O estado SQLite contém:

- ofertas conhecidas e seus detalhes;
- chaves de deduplicação;
- decisão do canal principal e do canal 2;
- IDs, horários, tentativas e erros de cada entrega;
- sincronização de esgotamento por canal;
- últimas 240 execuções (aproximadamente uma hora na cadência atual);
- baseline e início do modo live.

Antes de inserir um card, o Worker compara as identidades canônicas do endereço.
Além do slug completo, é usada a combinação estável
`parceiro + código interno da oferta`. Essa segunda chave protege contra
alterações de codificação como `tênis/tnis`, `cardápio/cardpio` e
`grátis/grtis` sem confundir ofertas de parceiros diferentes.

A canonicalização foi alinhada ao consumidor legado: percent-encoding,
mojibake, remoção de diacríticos, variantes `de`/`João` e correções conhecidas
como `seleo/selecao`, `ps/pos`, `at/ate` e `grtis/gratis`. A deduplicação
também conserva as chaves de conteúdo estrita/solta e o bloqueio recente por
título + validade durante sete dias.

O alarme é reprogramado ao final de toda execução. Uma listagem vazia ou
suspeitamente pequena falha de forma segura e não produz esgotamentos.

O modo operacional também é persistido no Durable Object e pode ser alterado
imediatamente pela rota autenticada `POST /mode`. Isso evita depender da
reciclagem de uma instância aquecida após um deploy e permite contenção
`live -> shadow` sem aguardar uma nova publicação.

## Entrega ao Telegram

O Worker utiliza a Bot API diretamente:

1. tenta `sendPhoto` usando a imagem pública da oferta;
2. usa `sendMessage` imediatamente caso o Telegram rejeite a imagem;
3. persiste a confirmação do canal principal;
4. para ingressos elegíveis, usa `forwardMessage` para o canal 2;
5. uma falha no canal 2 não apaga o sucesso do canal principal;
6. novas tentativas processam somente o destino ainda pendente.

As legendas utilizam HTML escapado, link canônico, validade, localização quando
disponível e as hashtags relevantes. Campanhas de ingresso não são silenciosas.

## Esgotamento

Uma oferta entregue passa a ser candidata a esgotamento somente quando:

- foi decidida nos últimos três dias;
- desapareceu de uma listagem saudável;
- acumulou pelo menos duas ausências;
- permaneceu ausente por no mínimo 15 minutos.

Depois da confirmação, o Worker usa `editMessageCaption` para fotos ou
`editMessageText` para mensagens sem imagem. Principal e canal 2 possuem
confirmações e retries independentes.

## Baseline e corte

O baseline inicial de 30/07/2026 registrou 48 ofertas e não enviou nenhuma
mensagem. As 12 ofertas recentes observadas pelo Mac estavam presentes no
inventário remoto.

Na promoção:

- as ofertas do baseline permaneceram bloqueadas;
- candidatos registrados durante o modo sombra não são reenviados
  retroativamente;
- somente decisões criadas depois do início do modo `live` entram na fila;
- o workflow legado foi convertido em fallback manual;
- o LaunchAgent do Mac foi descarregado;
- o Worker do Discord permaneceu inalterado.

## Segurança

- tokens e IDs de chat são secrets do Worker;
- valores não são expostos em `/health`, logs ou repositório;
- a cópia inicial dos secrets foi feita por um workflow temporário;
- o workflow e a credencial temporária de Cloudflare foram removidos após o
  sucesso;
- rotas mutáveis exigem `ADMIN_TOKEN`;
- a cópia local do `ADMIN_TOKEN` fica no Chaves do macOS, no serviço
  `uol-telegram-cloudflare-worker-admin-token`;
- logs contêm somente eventos, contagens e erros sanitizados.

## Validação

- testes unitários de coleta, deduplicação, validade e filtros;
- testes de legenda HTML e marcação de esgotamento;
- testes do fallback `sendPhoto` para `sendMessage`;
- testes do encaminhamento ao canal 2;
- teste real do transporte no canal principal e no canal 2;
- geração de tipos pelo Wrangler;
- dry-run do pacote;
- migração SQLite remota;
- saúde e cadência do alarme verificadas em produção.

## Ativação confirmada

- Data: 30/07/2026
- Início do modo live: `2026-07-30T06:38:57.696Z`
- Version ID: `447a4e10-bf21-4462-9969-6a5056f0aaa0`
- Startup remoto: 6 ms
- Inventário no corte: 48 ofertas de baseline
- Pendências no corte: zero
- Erros de entrega no corte: zero
- Teste real: principal e canal 2 confirmados
- Workflow automático legado: removido
- LaunchAgent `com.leosaquetto.uolmonitor`: descarregado

## Incidente de canonicalização de 30/07/2026

Às `07:05Z`, a listagem alternou endereços com letras acentuadas perdidas para
endereços corrigidos. Como o baseline inicial armazenava apenas o slug visto
naquele momento, 20 aliases foram inicialmente classificados como novos.

- oito mensagens chegaram ao canal principal antes da contenção;
- nenhuma foi encaminhada ao canal 2;
- o modo foi alterado imediatamente para `shadow`;
- as 12 decisões ainda não enviadas foram bloqueadas;
- os 20 pares foram reconciliados por `parceiro + código interno`;
- os oito registros enviados foram preservados para permitir edição futura de
  esgotamento;
- após o reparo: zero pendências, zero aliases ativos e zero erros de entrega.

O caso real foi convertido em testes de regressão para `grtis/gratis`,
`cardpio/cardapio`, `tnis/tenis`, mojibake e variantes do consumidor legado.

### Estado após a correção

- Version ID: `c6b4de31-2955-4333-99fd-92c80fc09d6b`
- modo persistido: `live`
- listagem saudável: 48 ofertas
- identidades ativas: 48
- aliases ativos: zero
- pendências de entrega: zero
- erros de entrega: zero
- primeiro alarme após a retomada: `48 vistas / 0 novas / 0 envios`

## Rollback

1. executar `POST /mode` com `{"mode":"shadow"}`;
2. publicar a versão protegida;
3. executar `bot leouol scraper - fallback manual`;
4. se necessário, recarregar
   `~/Library/LaunchAgents/com.leosaquetto.uolmonitor.plist`.

O Discord não participa desse rollback.

## Consolidação de baixa latência de 31/07/2026

O coletor passou a ser a fonte única para Telegram e Discord:

- alarme reduzido de 30 para 15 segundos;
- uma única leitura e uma única decisão durável por oferta;
- Telegram e Discord disparados em paralelo antes do enriquecimento para
  ingressos novos; a legenda é completada por edição logo depois;
- Discord preserva o embed aprovado com thumbnail;
- o coletor Discord anterior ficou com `COLLECTOR_ENABLED=false`, sem alarme;
- ofertas históricas foram marcadas como já entregues no Discord durante a
  migração, impedindo replay.

O histórico completo do Telegram foi recuperado por webhook:

1. o canal recebe thumbnail e legenda compacta;
2. o Telegram encaminha automaticamente a publicação ao `LeoUOL Chat`;
3. o webhook valida `X-Telegram-Bot-Api-Secret-Token`;
4. `forward_origin.message_id` é associado ao `main_message_id` durável;
5. a descrição completa é formatada e enviada como resposta nessa discussão;
6. cada chunk confirmado é persistido para que retries não o repitam.

O registro do webhook usa `drop_pending_updates=true`, portanto mensagens
anteriores à ativação não geram comentários retroativos. Para um rollback ao
consumer legado baseado em `getUpdates`, o webhook deve ser removido primeiro.

### Estado da ativação

- Worker central: `uol-telegram-shadow-pilot`
- Version ID final de baixa latência: `df550012-fe56-4c07-aab4-ea8f81b733af`
- intervalo: 15 segundos
- grupo vinculado: `-1003802235343`
- Discord consolidado: configurado
- webhook Telegram: registrado
- ofertas novas durante a migração: zero
- reenvios durante a migração: zero
- erros de entrega durante a migração: zero

## Encerramento dos consumidores legados e otimização de escrita

Em 31/07/2026, os dois workflows UOL foram desativados na API do GitHub e
movidos para `.github/workflows-archive/`. O LaunchAgent foi descarregado e o
plist passou para `~/Library/LaunchAgents.disabled/`.

O update periódico de `last_seen_at` também foi removido. Cards conhecidos só
geram `UPDATE` quando URL, título, categoria, imagens ou estado de ausência
mudam. Isso eliminou cerca de 276 mil escritas redundantes por dia na cadência
de 15 segundos.

- versão otimizada: `bcfe236f-89ea-4e94-9c23-56b3e605bd81`
- invocações projetadas do alarme: 5.760/dia
- escritas SQLite estáveis projetadas: aproximadamente 17.280/dia
- aliases ativos após a mudança: zero
- erros de entrega após a mudança: zero

### Retenção automática

A manutenção diária remove ofertas terminais que não estão mais na listagem e
tenham mais de 30 dias. O teto adicional preserva no máximo 300 registros
terminais. Ofertas ainda visíveis e estados com entrega pendente são protegidos,
portanto a limpeza não pode apagar um alerta antes do envio nem transformar um
card antigo ainda publicado em uma oferta nova.
