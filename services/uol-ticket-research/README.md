# Histórico público de ingressos UOL

Coletor local de páginas e catálogos públicos do Clube UOL. Usa Python 3.9+ e biblioteca padrão. Não faz login, resgate, OCR ou envio de notificações. A sentinel de resgate permanece pausada; este script não altera sua configuração.

O [estudo de Stories do Instagram](INSTAGRAM-STORIES.md) preserva a extração comprovada e os limites da prova inicial. O [monitor operacional](INSTAGRAM-MONITOR.md) está ativo no Oracle, sem depender do Mac, e enviou imagem e link da campanha `pQg` aos quatro destinos existentes.

O [ensaio de estabilidade Instagram](INSTAGRAM-TRIAL.md) tem transporte e controle de 72 horas implementados, testados sem rede e ainda inativos. Ele não altera a coleta diária descrita abaixo.

O usuário autorizou o [complemento Instagram no servidor](INSTAGRAM-MONITOR.md), com imagem do Story e link de campanhas nos mesmos quatro destinos do UOL bot. Esse monitor contínuo é independente da coleta diária e do ensaio isolado.

## Janela atual de pesquisa

O acompanhamento diário foi mantido até **30/10/2026**, inclusive, no horário de
São Paulo, priorizando The Pussycat Dolls. `config.research_until` em `state.json`
define a data de revisão. Depois dela, `--run` retorna `review_required` com zero
GETs; não altera snapshots, relatórios ou serviços. A data é opcional: ausente ou
`null` preserva o comportamento anterior para outros acompanhamentos.

Em 31/10, a automação deste chat deve apresentar a avaliação e pausar somente a
pesquisa diária. Um novo alvo ou nova data de término precisa ser definido antes
de retomar. O UOL bot, seus quatro destinos e o monitor operacional Instagram
continuam independentes dessa janela; o fim da pesquisa não os desativa.

Em 13/10, o coletor e o estado local receberam essa trava. A atualização das
instruções da automação não respondeu durante a primeira verificação, mas foi
concluída depois pelo app. Em 14/10, a configuração persistida confirmou a janela,
o cruzamento diário Oracle e a avaliação/pausa em 31/10, mantendo a rotina ativa
às 18h de São Paulo. A trava do coletor também impede novos GETs após 30/10.

O receptor push permanece em piloto até uma prova real no Oracle. Em 13/10, o
Story `pQ5` foi entregue com imagem e link aos quatro destinos por consulta
periódica. Em 14/10, `pQH` também foi confirmado nos quatro destinos por `poll`,
sem sinal push correspondente. Essas entregas não autorizam promover o intervalo
para 30 minutos.

Na raiz do repositório:

```sh
python3 services/uol-ticket-research/observe.py --plan
python3 services/uol-ticket-research/observe.py --run
```

`--plan` mostra o escopo e os limites, sem rede ou gravação. `--run` coleta por GET e grava uma única rodada por dia em `America/Sao_Paulo`. Repetir no mesmo dia retorna `already_recorded` sem novos GETs, inclusive após uma rodada parcial. Um lock impede rodadas simultâneas.

Limites por rodada: **560 códigos, 650 GETs, 2 workers e 480 segundos**. O escopo inicial contém os prefixos `pM` a `pT` e o código conhecido `pJq`. Códigos diferenciam maiúsculas e minúsculas. A ampliação de prefixos depende de dois códigos distintos no catálogo de novidades e precisa caber no limite; excedentes ficam para revisão.

HTTP **403 ou 429 pausa novas requisições da rodada**; requisições já em andamento podem terminar. O resultado fica parcial, sem contorno do bloqueio nem nova tentativa automática no mesmo dia.

## Dados locais

Por padrão, tudo fica em `~/.local/share/uol-ticket-research`. Use `--data-dir /caminho` para escolher outro diretório.

| Arquivo | Conteúdo |
| --- | --- |
| `state.json` | Configuração, observações acumuladas, transições e dias executados. |
| `snapshots/*.json` | Registro imutável de cada rodada, com estado recuperável. |
| `summary.json` | Resumo da última rodada e pendências. |
| `observations.csv` | Histórico tabular, incluindo códigos inconclusivos e referências históricas. |
| `report.md` | Relatório legível de páginas observadas, referências e mudanças. |
| `patterns.json` / `patterns.md` | Cruzamento offline de páginas, listas, validade e shows identificados. |
| `instagram-history.json` | Histórico sanitizado de Stories já registrados no Oracle, quando disponível. |

Os relatórios usam `config.metadata[codigo]` de `state.json` para nome/data do evento e fontes históricas:

```json
{
  "eventName": "Nome do evento",
  "eventDate": "30/10/2026",
  "sortDate": "2026-10-30",
  "referenceSource": "Descrição ou URL da referência histórica"
}
```

`sortDate` deve ser uma data ISO válida (`YYYY-MM-DD`). CSV e seções do relatório ordenam do futuro para o passado; datas ausentes ou inválidas ficam no final. `eventName` e `eventDate` são exibidos quando disponíveis. Os campos legados `event` e `date` continuam aceitos.

`referenceSource` permite mostrar uma referência histórica mesmo sem `first_seen` ou registro em `codes`. O relatório separa essas referências das páginas observadas publicamente; no CSV, `tipo_registro` e `fonte_referencia` explicitam a distinção. Referências não criam observações, datas de publicação ou provas de disponibilidade.

## Cruzamento offline com Stories

`pattern_report.py` lê o histórico público e, quando presente, o arquivo local
`instagram-history.json`. `export_reports` também regenera `patterns.json` e
`patterns.md`, sem repetir a coleta. Os shows ficam do futuro para o passado;
os Stories de outras campanhas permanecem no cruzamento geral.

```sh
python3 services/uol-ticket-research/pattern_report.py --format markdown
```

No Oracle, o utilitário instalado lê somente metadados existentes, sob o usuário
proprietário do banco:

```sh
sudo -n -u uol-instagram-monitor python3 -B \
  /opt/uol-instagram-monitor/story_history.py \
  --database /var/lib/uol-instagram-monitor/monitor.sqlite
```

A automação usa o SSH de `~/.config/uol-ticket-research/server.json`, captura essa
saída JSON em memória e chama `merge_story_history(histórico_anterior, exportação)`.
Grava o resultado com `observe.atomic_write` em `instagram-history.json`, sob o
mesmo lock local `.lock`, e chama `export_reports` com o estado e resumo atuais.
Não precisa abrir Instagram, consultar Analytics ou copiar o banco privado.
Falha de SSH conserva o histórico anterior; não o substitui por uma lista vazia.
Se a captura estiver até cinco segundos adiante do relógio local, aguarde esse
intervalo antes da importação; uma divergência maior interrompe a importação.
Não ajuste os horários das fontes. O merge recusa capturas futuras.

A exportação inclui apenas URL pública canônica, código, horários públicos/de
observação, gatilho, baseline, correlação e confirmação de imagem por destino.
Não exporta IDs de Story/mensagem, imagens, cookies, sessão, credenciais ou corpos
de notificações. O SQLite abre em modo somente leitura. Banco ausente ou sem
permissão segura é `unavailable`; cobertura legada ausente é `partial`.
Registros com horários diferentes permanecem distintos; a contagem é de registros
históricos, sem inferir quantidade de Stories únicos.

Só intervalos delimitados por duas observações conclusivas permitem comparar a
ordem entre página/catálogo e publicação do Story. Baseline, lacuna, URL divergente
ou sobreposição permanece `order_uncertain`; os relógios das fontes não foram
validados independentemente. Confirmação de entrega por `poll` não comprova push.
O alfabeto candidato dos códigos permanece hipótese, não algoritmo confirmado.

## Limites da evidência

- `first_seen` significa **primeira observação pelo coletor**, não criação, cadastro ou publicação da campanha. A validade publicada também não prova essas datas.
- Catálogos incompletos, bloqueados ou com cobertura diferente **não provam deslistagem**. Estado inconclusivo preserva a última evidência conclusiva.
- Página ausente não prova estoque, esgotamento ou impossibilidade de resgate. Estar fora das listas monitoradas não prova ausência em todo o site.
- Observações diárias podem perder mudanças transitórias entre rodadas. Uma transição informa o intervalo entre duas observações, não um horário exato de mudança.

Testes locais, sem rede:

```sh
cd services/uol-ticket-research
python3 -m unittest test_observe test_probe test_pattern_report test_story_history
```
