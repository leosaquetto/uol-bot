# Instagram como complemento do UOL bot

Plano autorizado em 09/10/2026: instalar o coletor no Oracle existente, integrar ao Worker atual e enviar o Story de `pQg` aos quatro destinos. O ensaio isolado de 72 horas não é requisito para ativar; estabilidade será observada depois. Esta integração não depende do Mac ou do Codex aberto.

## Caminho de execução

1. `uol-instagram-monitor.service` lê `@clubeuol` por HTTP no Oracle a cada 120–130 segundos. A sessão dedicada e cookies atualizados ficam privados no servidor. Não há navegador por ciclo nem login UOL.
2. Extrai o sticker e a imagem original do mesmo objeto do Story. Vídeos usam sua imagem de capa. Apenas links canônicos em `https://clube.uol.com.br/campanhasdeingresso/<codigo>-<slug>` entram na fila; outros benefícios são ignorados. Não usa OCR para decidir elegibilidade.
3. Envia à entrada autenticada do Worker existente. A fila de Stories usa `(Story ID, link)` e recibo independente para Discord, Telegram principal, Canal 2 e WhatsApp. Uma campanha já anunciada pelo bot pode receber este complemento de Story, sem alterar o registro de estoque da oferta.

## Resultado em cada destino

| Destino | Mensagem |
| --- | --- |
| Discord de ingressos | Imagem do Story e link público; recibo da mensagem e confirmação de mídia pelo proxy |
| Telegram principal | Foto do Story com legenda e link |
| Telegram Canal 2 | Cópia da foto confirmada no principal |
| WhatsApp já configurado | Foto inteira e legenda com link, com confirmação pelo transporte Baileys |

Não há novo destino, token de canais no coletor ou conta WhatsApp adicional. O Worker usa seus segredos e destinos existentes. A imagem do Discord fornece o proxy permitido pelo gateway WhatsApp; os bytes da foto são preservados. As ofertas normais continuam usando o formato anterior de cartão.

O Discord recebe a foto por upload de bytes, com tamanho limitado a 5 MiB, em vez de depender de buscar a URL assinada do Instagram. Uma mensagem já publicada sem mídia pode ter sua foto reparada por PATCH no mesmo ID, até duas tentativas, sem criar outra mensagem.

Forwards automáticos de Stories no grupo de comentários do Telegram são separados das ofertas normais. Isso impede que o Story confirme uma oferta ambígua ou receba edição de esgotamento/comentários destinados à oferta original.

## Limites, sessão e recuperação

- Uma coleta de cada vez; até dois GETs Instagram por ciclo e 1.440 reservados em uma janela móvel de 24 horas. A reserva é persistida antes da rede e sobrevive a crash/reinício. Até vinte novos avisos de Story por dia no Worker, com cinquenta registros recentes.
- HTTP 200 sem payload reconhecido é inconclusivo. Falhas transitórias usam espera crescente. Autenticação pendente entra em espera de seis horas; 429 respeita `Retry-After`, com pelo menos uma hora. Não troca contas/IPs ou contorna desafios.
- Atualização de `Set-Cookie` é automática e privada; aceitação da sessão por tempo ilimitado não é garantida. Um desafio real pode exigir renovação. O monitor continua vivo, registra o motivo e publica seu estado operacional.
- O coletor publica estado a cada mudança e pelo menos a cada quinze minutos. O Worker expõe saúde separada da coleta UOL e usa o destino operacional já configurado para falhas de autenticação/limite e recuperação, sem cair nos canais públicos.
- Retentativas da entrada do Worker são idempotentes. Destinos confirmados não são reenviados. Telegram/Discord ambíguos não são repetidos automaticamente; WhatsApp reconcilia a mesma chave e corpo com seu ledger de recibos. Fila e entrega continuam independentes da coleta Instagram.
- O modo `shadow` do bot suspende também os envios de Stories; retorno a `live` retoma só pendências. A sentinela de resgate permanece pausada.

Os intervalos limitam o tráfego, mas não estabelecem uma quota oficialmente segura do Instagram. Publicação de Story não comprova estoque, elegibilidade ou início de cadastro da campanha.

## Arquivos e operação

Código no Oracle: `/opt/uol-instagram-monitor/{instagram.py,monitor.py,trial.py}`. `trial.py` fornece utilitários privados ao monitor; o ensaio de 72 horas não é executado por esta unidade.

Estado em `/var/lib/uol-instagram-monitor` (0700, usuário próprio): `session.json`, `config.json`, `monitor.sqlite`, `status.json` e `monitor.lock`. Arquivos de sessão e configuração são 0600. O token de ingest só permite as rotas Instagram, não administração ou resgate. URLs assinadas da mídia ficam apenas no armazenamento privado; relatórios e recibos não as retornam.

Rotas no Worker, com `INSTAGRAM_STORY_INGEST_TOKEN`:

- `POST /ingest-instagram-story`: imagem, Story ID, datas e link da campanha; retorna recibo por destino.
- `GET /instagram-story-status`: recibos e saúde sanitizada do coletor.
- `POST /instagram-monitor-heartbeat`: estado sanitizado da coleta, sem cookies ou corpo HTML.

Checagem sem nova consulta ao Instagram:

```sh
sudo systemctl status uol-instagram-monitor --no-pager
sudo cat /var/lib/uol-instagram-monitor/status.json
```

Interromper apenas este complemento: `sudo systemctl stop uol-instagram-monitor`. Preservar bancos e sessão. O status do processo deve ser combinado com `sourceStatus`, última leitura válida e recibos; processo ativo sozinho não comprova coleta saudável.

## Validação

Testes locais do parser/mídia, orçamento, fila/reinício, modo do bot, isolamento de forwards, recibos por destino e foto WhatsApp. O Worker passa pela CI/release do projeto; a ativação verifica leitura autenticada no Oracle e envio real do Story autorizado, sem mensagens sintéticas para grupos.

Estado de implantação e recibos reais serão registrados após a publicação. Estabilidade prolongada e renovação real da sessão continuam sendo acompanhamento de operação; não são declaradas comprovadas por fixtures.
