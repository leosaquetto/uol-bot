# Stories Hunter 5.7.7

- Perfil sem verificação mostra “Não verificada”; somente uma resposta de
  autenticação do UOL pode indicar sessão expirada.
- O modal aberto acompanha a resposta nativa e oferece “Verificar sessão salva”,
  sem resgate, inclusive na primeira abertura após a atualização.
- Falhas de consulta, permissão e interpretação têm diagnóstico próprio e não
  se confundem com falha de autenticação.
- Identidade continua conferida por SHA-256, agora no JavaScript nativo, sem
  depender de WebCrypto ou contexto seguro do WebView.
- “Conectar conta ao Hunter” abre o login oficial e recebe o retorno OAuth com
  estado vinculado. A nova sessão permanece isolada até o histórico confirmar
  a conta selecionada; cancelamento, erro ou outra conta preservam a sessão anterior.
- Sessões, contas, vouchers e reservas de resgate são preservados.
