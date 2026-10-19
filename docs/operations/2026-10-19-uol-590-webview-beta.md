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

Fluxo usado para preparar a beta: publicar o artefato imutável via `release.cjs prepare --channel beta --commit <sha>`. Verificar CI e o hash dos bytes remotos antes de avisar por ntfy. Título: **Scriptable - Clube UOL Premium**. O primeiro aviso identificou 5.9.0 beta. O plano original mantinha stable em 5.8.6 até o aceite abaixo; o usuário autorizou posteriormente antecipar a promoção, conforme registro no fim deste documento.

Para optar pela beta no aparelho já instalado, executar:

`scriptable:///run?scriptName=Clube%20UOL%20Premium&action=update&channel=beta`

1. Confirmar versão 5.9.0. A primeira abertura após a atualização pode mostrar coleta pausada.
2. Em Ajustes → Coleta neste iPhone, escolher **Conectar Instagram neste iPhone**. Entrar com a conta desejada no formulário oficial e fechar a janela.
3. Confirmar consulta concluída em **Verificar coleta**. Usar **Coletar Stories agora** e conferir o resultado novamente.
4. Fechar o script e reabrir normalmente. Confirmar captura automática sem novo login. Não basta a janela do login fechar; verificar o diagnóstico da consulta.

**Pendente:** login escolhido, captura manual e persistência da sessão entre execuções no iPhone. A API documenta `loadURL`, `present` e `evaluateJavaScript`, mas não garante essa persistência: https://docs.scriptable.app/webview/.

A promoção usa `release.cjs promote`, sem reconstruir o artefato. A antecipação autorizada não comprova persistência. Se a sessão não persistir, registrar o impedimento e corrigir a via local sem retornar silenciosamente à sessão compartilhada.

O atualizador do próprio script e a central do saquettoOS continuam disponíveis. A substituição externa do arquivo do Clube UOL é dispensável; nenhum ramo das automações de outros scripts foi alterado.

## Publicação verificada em 19/10/2026

- Artefato: commit `a0a44da20b22c3b6a616d58085c4626c536f1c50`; manifesto beta: `b1be8fbd5a581b420cef7afb2101a2b4f65ce3f9`.
- Bytes remotos conferidos contra o artefato local: 706137 bytes, SHA-256 `bbc86386f574c9c85b1c0ce755ec099b0679076b1c0224ea08ef6afec5e76f0e`.
- CI completo aprovado: https://github.com/leosaquetto/uol-bot/actions/runs/39294146086 . Os 46 testes finais direcionados e os 32 testes de Stories/deduplicação do Worker passaram localmente.
- Manifesto stable e espelho legado continuam na 5.8.6, com integridade remota verificada.
- ntfy aceitou o aviso de beta: ID `zCNYUK7AkNHu`, timestamp `1792430872`, título `Scriptable - Clube UOL Premium`. Link abre o atualizador interno com `channel=beta`. Aceite do serviço não comprova entrega ou instalação no aparelho.
- Teste real solicitado ao usuário. A promoção estava pendente antes da autorização abaixo.

## Promoção antecipada autorizada

O usuário pediu recebimento pela atualização automática ao abrir. Após explicar que o cliente consulta stable e que a mudança para beta exige ação no próprio iPhone, o usuário autorizou explicitamente: “Publicar stable agora; testar no iPhone depois”. Essa instrução substitui a ordem de aceite do plano inicial.

Promovido o mesmo artefato `a0a44da20b22c3b6a616d58085c4626c536f1c50`, sem reconstrução. A versão 5.9.0 chega pelo atualizador normal ao abrir. Integridade, recuperação e pausa da primeira abertura permanecem preservadas.

**Ainda não validado no aparelho:** login escolhido na WebView, captura manual, persistência após fechar/reabrir e captura automática com essa sessão. Não declarar a independência operacional comprovada enquanto esse teste não for concluído.

### Verificação da publicação stable

- Promoção publicada no commit `56d8a548bcd838346d7fadd2f18da00c8b201a71`.
- CI completo aprovado: https://github.com/leosaquetto/uol-bot/actions/runs/39294715751 . A suíte Stories Hunter contém 289 testes; a beta já havia passado os mesmos 289 testes.
- Após expirar o cache, o endereço `main/stories-hunter/releases/stable.json` consultado pelo app retornou 5.9.0. O artefato remoto, a beta e o espelho legado têm os mesmos 706137 bytes e SHA-256 registrado acima.
- Aviso stable enviado por `build.js --code-only`, sem instalação/substituição externa no iCloud: ntfy aceitou ID `MIM00qEcKSHx`, timestamp `1792431220`. Título **Scriptable - Clube UOL Premium**; texto informa versão 5.9.0 e orienta abrir o app ou usar o saquettoOS.
- Não há confirmação de instalação nem dos testes físicos do iPhone neste registro.
