# Histórico público de ingressos UOL

Coletor local de páginas e catálogos públicos do Clube UOL. Usa Python 3.9+ e biblioteca padrão. Não faz login, resgate, OCR ou envio de notificações. A sentinel de resgate permanece pausada; este script não altera sua configuração.

O [estudo de Stories do Instagram](INSTAGRAM-STORIES.md) preserva a extração comprovada e os limites da prova inicial. O [monitor operacional](INSTAGRAM-MONITOR.md) está ativo no Oracle, sem depender do Mac, e enviou imagem e link da campanha `pQg` aos quatro destinos existentes.

O [ensaio de estabilidade Instagram](INSTAGRAM-TRIAL.md) tem transporte e controle de 72 horas implementados, testados sem rede e ainda inativos. Ele não altera a coleta diária descrita abaixo.

O usuário autorizou o [complemento Instagram no servidor](INSTAGRAM-MONITOR.md), com imagem do Story e link de campanhas nos mesmos quatro destinos do UOL bot. Esse monitor contínuo é independente da coleta diária e do ensaio isolado.

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

## Limites da evidência

- `first_seen` significa **primeira observação pelo coletor**, não criação, cadastro ou publicação da campanha. A validade publicada também não prova essas datas.
- Catálogos incompletos, bloqueados ou com cobertura diferente **não provam deslistagem**. Estado inconclusivo preserva a última evidência conclusiva.
- Página ausente não prova estoque, esgotamento ou impossibilidade de resgate. Estar fora das listas monitoradas não prova ausência em todo o site.
- Observações diárias podem perder mudanças transitórias entre rodadas. Uma transição informa o intervalo entre duas observações, não um horário exato de mudança.

Testes locais, sem rede:

```sh
cd services/uol-ticket-research
python3 -m unittest test_observe test_probe
```
