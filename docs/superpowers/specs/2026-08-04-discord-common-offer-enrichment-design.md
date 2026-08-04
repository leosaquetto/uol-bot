# Enriquecimento seguro dos cards comuns no Discord

## Objetivo

Aplicar o mesmo padrão de informação usado nos ingressos aos cards de ofertas comuns no Discord, sem mudar o texto enviado ao Telegram, sem atrasar a captura/API e sem criar leituras ou colunas adicionais no Durable Object.

## Design aprovado

### Limites do embed

- Centralizar limites conservadores do card: título com até 240 caracteres, resumo com até 1.200 e campos com limites abaixo dos máximos do Discord.
- Quando qualquer texto for cortado, terminar com `...` ASCII.
- Manter o total do texto do embed confortavelmente abaixo de 6.000 caracteres; o teste calcula esse total.
- Não deixar truncamento ou falha de conteúdo bloquear o webhook: o payload básico continua válido.

### Enriquecimento comum

- O caminho rápido permanece API/listagem → decisão → Telegram/Discord.
- Durante a manutenção, apenas a criação do card de cache do Discord pode fazer uma requisição HTTP limitada à página pública da oferta comum quando a linha ainda não possui descrição/validade.
- O fetch é serializado em lote pequeno (máximo padrão de 2 cards por ciclo), sem navegador, sem persistência adicional e sem alterar `offers`.
- O parser captura somente título, descrição e validade suficientes para o embed. A imagem já usada pelo card continua sendo a thumbnail/listagem existente.
- Se a página, conteúdo ou parser falhar, o card básico atual é enviado; a falha é best-effort e não vira erro da captura, Telegram ou Canal 2.

### Esgotamento e restock

- Antes de editar um card comum de esgotado/restock, reutilizar o detalhe transitório quando disponível; se a linha não tiver descrição/validade, refazer uma única busca limitada no mesmo caminho best-effort.
- O payload editado preserva a marcação de status (`[ESGOTADO]` ou disponibilidade) e os campos atuais.
- Ingressos continuam usando o detalhe/API já persistido e não fazem esse fetch extra.

## Não objetivos

- Não enriquecer legenda, comentário ou mensagem do Telegram.
- Não alterar polling, deduplicação, disponibilidade, imagens do Telegram ou o limite de captura.
- Não adicionar tabela/coluna, dependência ou worker paralelo.

## Validação

- Testes unitários do payload: truncamento ASCII, título/resumo dentro dos limites e soma do embed abaixo do teto.
- Teste do parser HTTP com HTML público simulado: extrai detalhe comum, rejeita resposta não HTML/sem conteúdo útil e mantém fallback.
- Teste de integração do ciclo: enriquecimento só ocorre no cache/edição do Discord e não é chamado no caminho rápido.
- Rodar a suíte rápida, verificação de tipos e dry-run do bundle antes do deploy.
