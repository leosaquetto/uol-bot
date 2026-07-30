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
- últimas 240 execuções;
- baseline e início do modo live.

O alarme é reprogramado ao final de toda execução. Uma listagem vazia ou
suspeitamente pequena falha de forma segura e não produz esgotamentos.

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

## Rollback

1. retornar `DELIVERY_MODE` para `shadow`;
2. publicar a versão protegida;
3. executar `bot leouol scraper - fallback manual`;
4. se necessário, recarregar
   `~/Library/LaunchAgents/com.leosaquetto.uolmonitor.plist`.

O Discord não participa desse rollback.
