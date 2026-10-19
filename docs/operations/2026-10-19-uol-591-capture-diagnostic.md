# Clube UOL 5.9.1 — retorno da WebView e diagnóstico

O usuário confirmou instalação automática da 5.9.0. No iPhone, o formulário oficial abriu o feed autenticado; fechar o login e coletar manualmente produziram imediatamente “consulta não confirmada”. Isso não comprova captura nem persistência da sessão.

## Correções

- A expressão enviada a `evaluateJavaScript(..., true)` terminava em uma chamada `async`, cujo resultado imediato é uma Promise. Agora termina em string; o resultado da captura chega somente por `completion`. Rejeições assíncronas também devolvem um código seguro.
- Erros de carregamento, avaliação JavaScript, recursos ausentes, resposta inválida, identidade e HTTP ficam distintos. Resultado contém somente motivos permitidos, etapa fixa e HTTP numérico; não contém erro bruto, corpo privado, sessão ou cookies.
- Ajustes mantém o último diagnóstico visível. “Coletar Stories agora” usa a sessão existente; quando a autenticação está pendente, orienta conectar. Falha usa ícone de aviso no toast.
- Preservados sessão local, orçamento, HTTP 429, fila, deduplicação e atualizador.

## Evidência e limites

O teste de regressão da ponte rejeita Promise como retorno imediato. Falha no código 5.9.0 com `browser_load_failed` e passa com a alteração. É simulação do contrato nativo, não execução no iPhone. Testes também exercitam diagnóstico seguro, HTTP/etapa, callback assíncrono, captura sem sessão e preservação da interface fora da mudança.

Uma consulta de diagnóstico em outra sessão, no navegador integrado do Mac, retornou HTTP 429 e não foi repetida. Esse resultado não identifica o erro do iPhone nem autoriza atribuir sua falha a limitação.

Referências primárias: [callback do Scriptable](https://docs.scriptable.app/webview/#evaluatejavascript) e [tipo de retorno não suportado no WebKit](https://developer.apple.com/documentation/webkit/wkerror/code/javascriptresulttypeisunsupported). A compatibilidade do retorno é uma causa plausível da falha imediata; a confirmação depende de nova captura no aparelho.

A publicação stable segue a autorização do usuário para receber pelo atualizador normal e testar no iPhone depois. Pendente: abrir 5.9.1, coletar, conferir o diagnóstico e fechar/reabrir para comprovar persistência. Não declarar a via operacional antes disso.

## Publicação

- Artefato `712149e936d4b25fa575c6e7f8101f21b9093b1e`; publicação beta/stable `c2f45b60348b7e5c3f054f50cd6db7404c2b325e`.
- CI aprovado: https://github.com/leosaquetto/uol-bot/actions/runs/39297329321 . Todos os 295 testes Stories Hunter passaram.
- Manifesto stable no endereço usado pelo app, artefato remoto e espelho legado verificados: 712264 bytes, SHA-256 `09f6a7ea847682005c8c6e9d0061b5cc15e36995d7bf29f0a139efe7d5ada36c`.
- ntfy aceitou ID `gCQ0K1FxzQCP`, timestamp `1792432222`, título **Scriptable - Clube UOL Premium**, versão 5.9.1 no corpo. Sem substituição externa do arquivo no iCloud.
- Solicitado novo teste no aparelho; captura/persistência permanecem pendentes.
