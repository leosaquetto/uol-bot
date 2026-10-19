# Clube UOL 5.9.2 — captura pela página oficial

## Causa e correção

Na 5.9.1, a WebView ainda executava GETs manuais para `web_profile_info` e `reels_media`. O iPhone confirmou HTTP 429 na primeira consulta. Login visível no feed não provava sucesso desses endpoints.

A 5.9.2 carrega `https://www.instagram.com/stories/clubeuol/` e lê, dentro da WebView, somente a projeção necessária dos blocos JSON inertes da página: `xdt_api__v1__feed__reels_media`. Não faz consultas diretas à API nem consulta sessão Instagram do servidor. A identidade vem do mesmo reel de `clubeuol`; IDs extensos são preservados. HTML, viewer, cookies e credenciais não saem da página.

O teste real mostrou um segundo defeito: `item.user` contém `id` e `pk`, mas não `username`. O parser agora aceita username ausente somente no autor do item quando todos os IDs coincidem com o reel validado. Username divergente, IDs conflitantes e perfil sem identidade continuam rejeitados.

Caso os dados ainda não existam e haja um único botão visível `Ver story`, `View story` ou `See story`, o fallback clica uma vez e aguarda hidratação limitada. Não clica em login, verificação, consentimento ou controles ambíguos. Dados disponíveis dispensam clique.

Conectar permite login mesmo durante espera. Fechar o login libera somente suspensão de autenticação; uma limitação HTTP 429 já registrada e o orçamento diário são preservados. Mensagem distingue login concluído de captura confirmada. Uma navegação/captura conta como uma consulta; recursos normais da página não entram no orçamento. Ausência de payload nunca significa zero Stories.

## Evidências

- Execução do extrator e parser finais na página real em navegador integrado, sessão já autenticada: uma oferta, Story `4011415238472326441`, campanha `pRp-2-ingressos-31-10-nubank-arte-lab-sp`, imagem 828×1472; publicação `2026-10-19T13:53:01.000Z`, expiração `2026-10-20T13:53:01.000Z`. Projeção não contém campos de sessão/viewer. Nenhuma consulta extra de perfil/reels foi feita pelo extrator.
- Teste inicialmente reproduziu `owner_mismatch` com o payload real; passou depois da correção estrita de IDs.
- 297 testes locais Stories Hunter aprovados, incluindo captura, limites, autenticação, ponte, concorrência, estado anterior 429, fila, atualização e interface. Build idêntico às fontes; `git diff --check` aprovado.
- Fallback do botão validado com fixtures: botão exato e visível, clique único, hidratação tardia, ausência de clique com dados presentes/429/login, rejeição de botões ocultos/ambíguos.
- A documentação [WebView do Scriptable](https://docs.scriptable.app/webview/) confirma carregamento da página e callback de avaliação. A evidência no navegador integrado não substitui Scriptable/WebKit físico: captura e persistência entre execuções ainda dependem do iPhone.

Publicação stable e atualização automática autorizadas pelo usuário. Não limpar a espera anterior até 20:50 no aparelho. Primeira abertura após atualização mantém pausa existente; reabrir retoma a coleta elegível.
