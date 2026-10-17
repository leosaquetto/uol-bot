# Stories Hunter 5.6.0 — catálogo do Worker

Primeira etapa da integração: o Worker continua coletando e entregando; o Hunter
reutiliza as observações existentes ao abrir. Esta versão não modifica resgates,
contas, cookies, vouchers ou o mecanismo de captura do Instagram.

- `GET /catalog` publica somente metadados de campanhas e Stories, sem dados de
  contas, erros internos, recibos ou vouchers. Limites fixos: 300 ofertas recentes
  examinadas e 30 Stories ativos; `coverage` informa o escopo e o truncamento.
- Cache de 120 segundos no Worker e no cliente, ETag/304 e CORS. Parâmetros de
  consulta não multiplicam as chaves do cache. Não há novo timer ou polling.
- Uma leitura fria exige margem de 700 linhas no orçamento opcional existente;
  quando bloqueada, responde 503 com `Retry-After: 120`. Não grava checkpoints nem
  interfere na estimativa do ciclo primário. A estimativa máxima de 512 permanece.
- O Hunter mantém IDs, visualizações, histórico e descrições locais mais completas.
  Ausência em catálogo parcial não significa esgotamento; um Story expirado não
  significa ingresso esgotado. Reposição precisa de evidência mais recente.
- Catálogo válido evita consulta ao portal e prefetch automático de descrições.
  Em falha, cache válido é preservado. Somente a abertura sem cache permite o
  fallback anterior ao portal; detalhes solicitados pelo usuário continuam.
- `node stories-hunter/build.js --prepare --code-only` gera o pacote para revisão
  sem instalar ou notificar. `--code-only` na instalação preserva o banco de
  resgates e os PDFs no iCloud. O comando sem flags conserva o comportamento antigo.
- Testes offline do Hunter passam a fazer parte da CI junto com os testes de
  projeção, orçamento e consultas SQLite reais do Worker.
- O artefato do repositório público e a entrega de código do MacSend não incluem
  cookies nem resgates. O pacote privado instalado no iCloud conserva o bootstrap
  compartilhado autorizado. Arquivos privados existentes têm prioridade; o pacote
  privado cria os ausentes uma vez para sobreviver a futuras atualizações de código.
  Esta separação não revoga credenciais nem remove versões antigas do Git.

Publicação, custo medido e confirmação no aparelho são registrados separadamente
em `docs/operations/2026-10-15-uol-reliability.md`. Aceite HTTP de OTA não comprova
que o iPhone instalou ou executou o script.
