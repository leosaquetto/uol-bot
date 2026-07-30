# Registro do piloto Cloudflare para o Telegram

## Objetivo

Provar, sem risco de mensagens duplicadas, que a Cloudflare pode substituir a
coleta executada no Mac e posteriormente o processamento realizado pelo
GitHub Actions.

O piloto cobre coleta, novidade, enriquecimento, deduplicação, validade,
roteamento dos dois canais e detecção de esgotamento. A entrega ao Telegram
permanece desativada.

## Fonte das regras

As regras foram conferidas na versão atual de `origin/main`:

- `mac_uol_scraper.js`: listagem, enriquecimento e confirmação de ausência;
- `github_scraper.py`: normalização, deduplicação, validade e pending;
- `bot_leouol.py`: entrega principal, filtro do canal 2 e edição de esgotadas;
- `.github/workflows/bot_leouol.yml`: janelas de validade e lote do consumer.

## Arquitetura

O Worker utiliza um único Durable Object porque a coleta global do Clube UOL é
uma única unidade de coordenação. Isso impede duas rodadas simultâneas de
decidirem sobre a mesma oferta.

O estado fica em SQLite dentro do Durable Object:

- `offers`: cards conhecidos, detalhes somente dos novos e decisões sombra;
- `runs`: últimas 240 execuções;
- `metadata`: baseline e último horário de coleta.

O alarme é reprogramado ao final de cada execução, inclusive após falhas. Uma
listagem vazia ou suspeitamente pequena não é usada para inferir esgotamento.

## Garantias do modo sombra

1. não há binding, variável ou função de Telegram;
2. `SHADOW_MODE` precisa ser exatamente `1`;
3. o baseline nunca gera decisão de envio;
4. apenas ofertas posteriores ao baseline recebem enriquecimento;
5. falhas de detalhe são tentadas novamente e não apagam a descoberta;
6. nenhuma ausência é considerada quando a própria listagem falha;
7. o estado do Discord e o estado do fluxo atual não são lidos ou alterados.

## Critério para promoção futura

Antes de incluir o Telegram, observar o piloto por pelo menos 24 horas e
comparar:

- horário da descoberta Cloudflare versus snapshot do Mac;
- conjunto de ofertas novas;
- completude do enriquecimento;
- decisões do canal principal;
- decisões do canal 2;
- marcações de esgotamento;
- erros, duração e consumo no plano gratuito.

Somente depois dessa comparação o envio real deve ser ativado. A promoção deve
adicionar timestamps independentes de entrega e manter Mac/GitHub como fallback
até uma janela adicional de estabilidade.

## Ativação inicial

Em 30/07/2026, o piloto foi publicado em modo sombra. A primeira rodada remota:

- obteve 48 ofertas por HTTP;
- criou 48 registros de baseline;
- não enriqueceu ofertas preexistentes;
- não simulou nenhum envio;
- confirmou que nenhuma configuração de Telegram existe no Worker.

As duas rodadas automáticas seguintes ocorreram no intervalo nominal de um
minuto, encontraram as mesmas 48 ofertas e registraram `no_change`, sem
duplicatas.

Versão remota validada:

- Worker: `uol-telegram-shadow-pilot`
- URL: `https://uol-telegram-shadow-pilot.leosaquetto.workers.dev`
- Version ID: `ddbf98d5-4f5c-428f-8ef1-12d9c9cc7fdb`
- startup remoto: 5 ms

Na comparação feita após a ativação:

- inventário HTTP da Cloudflare: 48 ofertas visíveis;
- snapshot recente do Mac em `origin/main`: 12 ofertas;
- ofertas recentes do Mac presentes na Cloudflare: 12 de 12;
- ofertas recentes ausentes na Cloudflare: zero.
