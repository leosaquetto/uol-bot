# Clube UOL Premium 5.9.0 — captura Instagram local

## Contrato

- O servidor mantém sua captura atual. O iPhone consulta perfil e Stories na página oficial do Instagram, em uma WebView com login escolhido no aparelho.
- O adaptador não lê nem exporta cookies/senhas. A página recebe somente o caminho permitido da consulta; o token de dispositivo permanece no host. A resposta é projetada para campos públicos antes de atravessar a ponte, e validada novamente pelo coletor.
- Aprovação do aparelho continua obrigatória. Não há consulta a `/hunter-access/config` nem importação de `instagramSession`. O endpoint antigo, a chave antiga e o coletor do servidor permanecem compatíveis com 5.8.6.
- A fila, os recibos e `source: "instagram"` permanecem iguais. A deduplicação no Worker reúne observações do servidor e dos aparelhos; Portal + Stories continuam na mesma oferta.
- Estado novo: `state-instagramWebView-*` e `request-instagramWebView-*`. Os prefixos antigos `state-instagram-*` e `request-instagram-*` não os selecionam.
- Uma captura automática por abertura, sem apresentar login. A primeira abertura de uma versão nova conserva a pausa do atualizador. As ações `collect` e `notification` permanecem sem interação.
- Ajustes oferece **Conectar Instagram neste iPhone** e **Coletar Stories agora**. Fechar o login não confirma sucesso: é necessária uma consulta válida ao perfil/Stories.
- Sem intervalo fixo de cinco minutos na WebView. Mantidos o bloqueio de execução, 30 tentativas de consulta por dia UTC e espera mínima de seis horas após HTTP 429, respeitando `Retry-After`. Recursos carregados pela página não entram no orçamento. Renovar login não apaga limites.

## Evidência automatizada

Testes cobrem aprovação sem sessão compartilhada, origem/rotas da WebView, login cancelado ou expirado, projeção sem campos privados, IDs longos, dados inválidos, limite diário, HTTP 429, concorrência e isolamento dos arquivos antigos. Testes existentes preservam integridade/recuperação do atualizador, pausa pós-atualização e a composição Portal + Stories.

O teste do Worker `two phones and Oracle converge on one durable Story and four destination messages` verifica uma única Story durável e uma mensagem por destino. São testes locais, não evidência de captura real no iPhone.

## Beta e aceite no aparelho

Publicar o artefato imutável via `release.cjs prepare --channel beta --commit <sha>`. Verificar CI e o hash dos bytes remotos antes de avisar por ntfy. Título: **Scriptable - Clube UOL Premium**. Aviso deve identificar 5.9.0 beta; stable permanece 5.8.6 até o aceite abaixo.

Para optar pela beta no aparelho já instalado, executar:

`scriptable:///run?scriptName=Clube%20UOL%20Premium&action=update&channel=beta`

1. Confirmar versão 5.9.0. A primeira abertura após a atualização pode mostrar coleta pausada.
2. Em Ajustes → Coleta neste iPhone, escolher **Conectar Instagram neste iPhone**. Entrar com a conta desejada no formulário oficial e fechar a janela.
3. Confirmar consulta concluída em **Verificar coleta**. Usar **Coletar Stories agora** e conferir o resultado novamente.
4. Fechar o script e reabrir normalmente. Confirmar captura automática sem novo login. Não basta a janela do login fechar; verificar o diagnóstico da consulta.

**Pendente:** login escolhido, captura manual e persistência da sessão entre execuções no iPhone. A API documenta `loadURL`, `present` e `evaluateJavaScript`, mas não garante essa persistência: https://docs.scriptable.app/webview/.

Somente depois desse teste promover com `release.cjs promote`, sem reconstruir o artefato. Se a sessão não persistir, manter beta e registrar o impedimento, sem retornar silenciosamente à sessão compartilhada.

O atualizador do próprio script e a central do saquettoOS continuam disponíveis. A substituição externa do arquivo do Clube UOL é dispensável; nenhum ramo das automações de outros scripts foi alterado.

## Publicação verificada em 19/10/2026

- Artefato: commit `a0a44da20b22c3b6a616d58085c4626c536f1c50`; manifesto beta: `b1be8fbd5a581b420cef7afb2101a2b4f65ce3f9`.
- Bytes remotos conferidos contra o artefato local: 706137 bytes, SHA-256 `bbc86386f574c9c85b1c0ce755ec099b0679076b1c0224ea08ef6afec5e76f0e`.
- CI completo aprovado: https://github.com/leosaquetto/uol-bot/actions/runs/39294146086 . Os 46 testes finais direcionados e os 32 testes de Stories/deduplicação do Worker passaram localmente.
- Manifesto stable e espelho legado continuam na 5.8.6, com integridade remota verificada.
- ntfy aceitou o aviso de beta: ID `zCNYUK7AkNHu`, timestamp `1792430872`, título `Scriptable - Clube UOL Premium`. Link abre o atualizador interno com `channel=beta`. Aceite do serviço não comprova entrega ou instalação no aparelho.
- Teste real solicitado ao usuário; promoção a stable permanece pendente.
