# Stories Hunter 5.7.3

- Inclui as melhorias de abertura, Home, datas, histórico, timeline e atualização
  do catálogo da 5.7.2, ainda sem distribuição OTA quando este ajuste foi solicitado.
- Abrir uma oferta apresenta o navegador interno por cima do Hunter. Concluir/Fechar
  revela o painel que permaneceu carregado, com a aba e posição preservadas.
- A rota nativa aceita somente HTTPS dos dois hostnames do Clube UOL e bloqueia
  aberturas simultâneas. Requisições normais de imagens, fontes e catálogo continuam
  permitidas. Cookies e credenciais não são colocados na rota.
- A interface explica como retornar. Resgate por fetch e navegação de autenticação
  mantêm os contratos existentes. Não se presume compartilhamento de cookies entre
  o navegador e a WebView.

API utilizada: [Safari.openInApp](https://docs.scriptable.app/safari/#openinapp),
com [shouldAllowRequest](https://docs.scriptable.app/webview/#shouldallowrequest)
cancelando apenas a rota interna explícita. A apresentação e o retorno reais devem
ser confirmados no iPhone; os testes locais não substituem essa confirmação.
