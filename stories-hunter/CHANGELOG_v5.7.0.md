# Stories Hunter 5.7.0 — descobertas complementares e histórico fiel

O iPhone observa Portal e Stories de `@clubeuol`; o Worker valida, mantém as filas
e entrega pelos transportes existentes. O telefone não envia mensagens aos destinos.

- Home filtra antes de agrupar: ofertas ativas e esgotamento confirmado há até
  24 horas. O histórico permanece completo; ausência no catálogo não esgota oferta.
- Pôsteres aceitam o CDN exato `d310rdc9v8uuq.cloudfront.net`, buscam imagem entre
  sessões da campanha e usam Story como complemento. Ajuste pela altura, recorte
  lateral centralizado e fundo discreto para imagens estreitas.
- Datas em faixa rolável independente do título, ordenadas pelo calendário de
  São Paulo, inclusive virada de ano. Ordem de categorias e cards preservada.
- Timeline inclui thumbnails. Datas brasileiras e ISO mantêm sua precisão;
  registros sem horário exibem só a data e registros inválidos ficam no final como
  “Data não informada”. Resgates exigem conta e identidade comprovadas, sem inferência
  por título nem horário artificial de uma hora atrás.

## Coletor nativo

`collector.js` é incorporado pelo build ao host Scriptable, fora da WebView.
Abre junto da interface ou pelo Atalho com `action=collect`/`action=notification`.
A primeira abertura de uma versão nova preserva a interface e não inicia coleta,
pois a automação OTA existente abre o script sem distinguir atualização de abertura.
A execução explícita de coleta continua disponível; não há timer contínuo.

Portal: intervalo mínimo de 120 segundos. Instagram: 5 minutos; identidade em cache
por 24 horas. Cada fonte admite até 30 requisições por aparelho/dia. Recuos persistem;
429 do Instagram espera no mínimo seis horas e respeita `Retry-After`; sessão inválida
suspende a fonte. IDs numéricos longos são preservados textualmente.

Descobertas são salvas localmente antes de transmitir, com UUID e conteúdo imutáveis.
Resposta perdida repete o mesmo evento. Recebimento e entrega são estados distintos.
O `Request` nativo usa timeout e verifica tamanho após download; a API Scriptable
não oferece leitura incremental equivalente ao limite em streaming do Worker.

## Entrada e retomada no Worker

- `POST /hunter-intake`: credencial `h1` individual, revogável por secret hash;
  8 observações/64 KiB, sem cookies, credenciais, HTML ou destinos livres.
- `GET /hunter-intake-status?eventId=...`: recibo acessível somente ao aparelho dono.
- Metadata existente: 32 pendências/256 KiB agregados. Admissão reserva espaço de
  progresso; falta de capacidade/orçamento conserva a pendência no aparelho.
- Mesma identidade com conteúdo diferente retorna 409. Campanha nova é conferida
  na página pública pelo contrato de identidade e botão de disponibilidade existente;
  HTTP 200 isolado não autoriza publicação. Campanhas conhecidas não são republicadas.
  A campanha entra como `pending_enrichment`; o próximo ciclo primário normal
  assume enriquecimento/entrega, evitando concorrência extra de publicação.
- Stories convergem para a outbox existente com identidade comum ao Oracle.
  A manutenção retoma até dois itens, no máximo um Story, sem novo Cron ou
  antecipação do ciclo primário. Entregas incertas continuam em reconciliação.
- Catálogo acrescenta evidência opcional de disponibilidade, sem mudar o contrato
  dos clientes antigos. Cache/lastSeen não criam horário de esgotamento.

## Instalação e evidências

Preparar: `node stories-hunter/build.js --prepare --code-only`.
Após publicar e verificar o backend: `node stories-hunter/build.js --code-only`.
O fluxo existente MacSend → ntfy → Atalho instala o código. Sessões e vouchers
existentes têm prioridade e não são regravados em massa.

O arquivo privado `uol_hunter_private_bootstrap.json`, nos Documentos do Scriptable,
provisiona `deviceId`, `token` e `instagramSession` para as chaves
`uol.hunter.device.v1`, `uol.hunter.token.v1`, `uol.hunter.instagram.v1` do Keychain.
Somente chaves ausentes são importadas. Nunca incluir esse arquivo no Git/MacSend.
Outro aparelho precisa de credencial própria; revogação remove/desabilita seu registro
em `HUNTER_INTAKE_CLIENTS` sem alterar a sessão do servidor.

Publicação, recebimento OTA, execução real no iPhone e descoberta natural com recibos
dos destinos são evidências separadas. Consultar o documento operacional para o
estado verificado; testes offline não comprovam execução no aparelho.
