# Design: confirmação rápida de esgotamento para ingressos

Status: proposta para revisão do usuário

## Objetivo

Reduzir o tempo entre uma oferta de ingresso sair da página pública e a
edição de “esgotado” no Telegram/Discord, sem alterar o comportamento das
ofertas comuns nem consumir a reserva que mantém o polling rápido da API.

O escopo fica limitado a ofertas cuja identidade é uma campanha de ingresso
(`/campanhasdeingresso/`). Ofertas de outras categorias continuam usando a
confirmação de ausência da manutenção.

Quando o esgotamento for confirmado, Telegram e Discord devem mostrar o
horário no fuso de São Paulo e, logo abaixo, a duração entre a primeira
observação/publicação da oferta e o horário de esgotamento. Se algum timestamp
for inválido, a duração é omitida sem bloquear a edição.

## Contexto atual

- O alarme primário consulta a API a cada aproximadamente 15 segundos e envia
  ofertas novas.
- A confirmação atual de esgotamento depende da reconciliação HTML, com duas
  ausências e pelo menos 15 minutos de ausência.
- A manutenção também processa comentários, restocks, Discord, cache de
  imagens e reconciliações. Ela é interrompida quando a reserva diária de
  leituras do Durable Object precisa ser preservada.
- A edição da mensagem é barata depois que o esgotamento está comprovado, mas
  hoje fica atrás dessa manutenção pesada.

## Fluxo proposto

### 1. Fila de probes somente para ingressos

Quando uma oferta de ingresso for entregue, o worker agenda uma verificação
direta da URL pública para aproximadamente um minuto depois. O estado fica na
própria oferta, com contador de tentativas consecutivas, próximo horário e
último resultado. Nenhuma oferta comum entra nessa fila.

O processamento ocorre no alarme primário, em lote estritamente limitado. A
consulta deve usar a identidade/índice da oferta e nunca varrer todo o
inventário para descobrir candidatos.

### 2. Classificação da URL

Cada probe usa GET com prazo curto, sem cache e seguindo redirecionamentos.

- `gone`: 404/410 ou redirecionamento confirmado para a home, sem evidência de
  que a página da oferta continua disponível;
- `available`: a resposta mantém a identidade da oferta ou contém a página
  pública correspondente;
- `indeterminate`: timeout, erro de rede, 5xx, bloqueio/anti-bot, resposta
  incompleta ou qualquer situação que não prove ausência.

Uma única resposta só poderia confirmar esgotamento se for uma ausência
determinística. Como o site pode ter redirecionamento transitório, o padrão
será exigir duas respostas `gone` consecutivas, separadas por alguns segundos.
Qualquer `available` ou `indeterminate` zera a sequência e mantém a oferta
protegida pelo fallback de 15 minutos.

Para evitar um incidente global do site virar centenas de esgotados, o worker
deve falhar fechado quando várias URLs de ingresso apresentarem o mesmo
redirecionamento/erro no mesmo intervalo.

### 3. Edição crítica imediata

Após duas confirmações `gone`, o worker marca a oferta como `sold_out` de forma
idempotente e executa a sincronização das mensagens existentes:

- edição da mensagem principal no Telegram;
- edição do Canal 2 quando houver mensagem;
- edição do card correspondente no Discord quando houver mensagem.

O status visível nos dois canais terá o mesmo conteúdo lógico:

```text
❌ Oferta esgotada às 15:33.
⏱️ Ficou no ar por 6 min.
```

O horário usa `America/Sao_Paulo`; a duração usa `first_seen_at` como início e
`sold_out_at` como fim, com formato legível em minutos/horas. O Discord também
mantém seu timestamp nativo para ordenação/visualização.

O estado é gravado antes/depois de cada destino para que uma repetição não
duplique envio. Falhas ambíguas continuam em estado desconhecido e não geram
uma nova mensagem automaticamente.

### 4. Orçamento e fallback

A fila crítica terá limite próprio de probes por ciclo e por dia, contabilizado
na mesma leitura de orçamento do Durable Object. A reserva da API permanece
intocada. Se o limite crítico acabar ou a resposta não for conclusiva, o
worker não força a manutenção pesada: a oferta segue para o processo atual de
duas ausências/15 minutos.

Assim, o sistema ganha rapidez para casos claros sem transformar a proteção do
free tier em uma falha aberta.

## Alternativas consideradas

1. **Somente reduzir 15 minutos para 1 minuto.** Menor mudança, mas ainda
   depende da manutenção e pode atrasar quando a cota bloquear; não resolve o
   caso observado.
2. **Consultar a URL uma vez para todas as ofertas.** Mais simples, mas uma
   resposta transitória poderia marcar falsos esgotamentos; não atende ao
   requisito de segurança de aproximadamente 99%.
3. **Probe limitado + duas confirmações + lane crítica.** Recomendado: separa
   descoberta da edição, mantém fallback e restringe o custo a ingressos.

## Validação

Serão adicionados testes para:

- classificação de 404/410, home, página disponível, timeout e 5xx;
- exigência de duas confirmações e reset após resposta inconclusiva;
- exclusão de ofertas comuns da fila;
- idempotência da transição para `sold_out` e das edições dos três destinos;
- horário e duração iguais no Telegram e no Discord, incluindo timestamps
  inválidos sem falha;
- limite diário da lane crítica e fallback quando a reserva estiver ativa;
- migração/índice e bundle/types do Worker.

Nenhuma alteração será feita no código de produção antes da revisão desta
especificação.
