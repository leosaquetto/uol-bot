# saquettoOS 1.0.3 — 2026-10-18

## Interfaces originais e retorno à central

- Os seis apps passam de iframe/adapters para execução local da fonte verificada, usando as APIs nativas originais. Nenhuma fonte privada é incluída no artefato; nenhum arquivo de app instalado é alterado por esta mudança.
- `Script.complete()` do filho é isolado. Nome, arquivo e URLs do script apontam ao app correto. A fonte do Calendário aguarda `t.present()`; a do media-downloader aguarda sua IIFE principal. Transformações limitadas a fontes já verificadas por SHA-256 e aplicadas somente em memória.
- A central pausa a leitura da fila enquanto a sessão nativa está ativa, aguarda o encerramento e então retoma. A UI não trata uma sessão maior que 90 segundos como erro. Atualizações e uma segunda abertura ficam bloqueadas enquanto o app está ativo.
- 48 testes direcionados aprovados: 9 host, 12 UI, 8 ciclo de vida, 12 runtime e 7 sessões nativas. Cobrem fechamento, bloqueio de abertura concorrente, pausa da ponte, espera sem timeout, isolamento de identidade e recusa de fonte desconhecida. Build e diff-check aprovados.
- Os seis arquivos instalados passaram na verificação de fingerprint e compilação do corpo assíncrono sem execução. Nenhum dado de calendário, medicamentos ou benefícios foi acessado pelos testes.
- Documentação primária consultada: [URLScheme](https://docs.scriptable.app/urlscheme/), [WebView.present](https://docs.scriptable.app/webview/#-present), [UITable.present](https://docs.scriptable.app/uitable/#-present) e [Script.complete](https://docs.scriptable.app/script/#complete). A URL executa scripts, sem contrato documentado de retorno. O retorno implementado usa as promessas das apresentações nativas dentro da sessão aguardada.
- Pendente no aparelho: abrir UOL a partir da central, confirmar layout/armazenamento original e fechar para voltar; repetir com uma tabela nativa. Testes sintéticos não demonstram a pilha de apresentação do Scriptable. Ações internas que abrem uma nova execução por URL conservam seu comportamento original.

# saquettoOS 1.0.2 — 2026-10-18

## Nova sequência de apresentação

- O usuário confirmou no editor do iPhone a versão 1.0.1 e informou que a tela preta persistia. Portanto, a hipótese do filtro inicial não explica sozinha a falha observada; a 1.0.1 não foi aceita como correção visual.
- A 1.0.1 ainda avaliava JavaScript antes e imediatamente depois de `present()`, com chamadas concorrentes para enviar o estado e drenar a fila. O histórico do UOL (`CHANGELOG_v5.7.1.md`) registra o mesmo risco e sua correção por sequência nativa. O launcher agora carrega uma tela leve, apresenta a janela, aguarda 800 ms nativos e carrega a interface completa com o estado embutido. Avaliações começam somente depois dessa carga, são serializadas e recusam trabalho novo após fechar.
- O marcador de renderização só é definido depois do primeiro `render()`. O host verifica esse marcador e as dimensões reais do shell. Falha recupera a tela leve e apresenta diagnóstico ao fechar.
- `saquettoos.startup.v1.json`, na pasta Scriptable, registra versão, etapas, horários e dimensões. Não contém identidade, tokens, nomes/perfis de usuários, URLs de requisições ou dados dos apps.
- 27 testes direcionados aprovados: host, UI e sete cenários do ciclo de vida. Verificam apresentação antes das avaliações, estado no documento, serialização, fechamento, trabalho nativo pendente e recuperação de dashboard vazio. Build e diff-check aprovados.
- A prévia completa foi carregada por um `WKWebView` nativo do macOS, após a tela leve: marcador de renderização verdadeiro, seis apps, três imagens decodificadas, shell `display:flex`, dimensões 375 × 855 e título presente. Isso verifica o documento no WebKit, não a apresentação pelo bridge do Scriptable no iPhone.
- Abertura da 1.0.2 confirmada pelo usuário no Scriptable do iPhone. A mesma confirmação revelou falha nos apps adaptados: UOL com layout alterado e aviso de armazenamento; motivação da 1.0.3.

## Base 1.0.1

## Correção de abertura e encerramento

- Relato real no iPhone: WebView preta e execução ainda ativa após Close na 1.0.0.
- O HTML gerado contém a estrutura estática e JavaScript válido. O filtro de requisições era instalado antes de `loadHTML` e podia recusar a URL interna de carregamento; a URL exata emitida pelo iPhone não foi capturada. Agora o documento confiável carrega antes do filtro, e a presença da interface/ponte é verificada antes de apresentar a WebView. Falha de inicialização exibe alerta nativo.
- A ponte usava `evaluateJavaScript(..., true)` sem chamar `completion` enquanto a fila estava vazia. A documentação do Scriptable exige essa chamada para encerrar a avaliação; `Promise.race` não a cancelava. Agora cada leitura da fila retorna imediatamente, a espera usa Timer nativo cancelado ao fechar, e novas avaliações da tela fechada são recusadas.
- 6 testes novos do ciclo de vida aprovados, executando `saquettoMain` com mocks. O mesmo harness sobre a 1.0.0 produziu 4 falhas e 2 passes. Mais 32 testes existentes de host/runtime/UI aprovados. Build verifica sintaxe do artefato e da interface.
- Não houve teste físico da 1.0.1 no iPhone. Abertura, encerramento e sincronização iCloud ainda exigem confirmação no aparelho.

## Base 1.0.0

## Confirmado

- Build autocontido: fontes públicas e logos fornecidos, sem fonte privada dos apps nem credenciais do administrador.
- 105 testes isolados aprovados: host, updater, runtime, ponte HTML, três adapters e serviço de acesso.
- Transformação e sintaxe dos seis scripts instalados verificadas sem executá-los. UOL público/instalado 5.8.5 reconhecido pelo fingerprint registrado.
- Revisão corrigiu exposição de senhas/cookies UOL no HTML, autoridade de iframe após navegação, render duplicado e fechamento durante abertura/operação pendente.
- Prévia no navegador integrado em 390 × 844: temas claro e escuro, material vidro, seis apps e dock. Capturas locais em `preview/`, fora do Git.
- Serviço de aprovação publicado: `https://saquettoos-access.leosaquetto.workers.dev/health` retorna `ready`. Identidade de Leo verificada como administrador aprovado; credenciais fora do repositório.

## Limites

- Testes usam fixtures e mocks. Não comprovam execução real no iPhone, entrega de notificação, permissões iOS, sincronização iCloud ou latência entre apps.
- Login, confirmações, Photos e ShareSheet mantêm folhas nativas quando necessárias. O fechamento aguarda operações nativas já iniciadas.
- Atualizações oficiais configuradas para saquettoOS e Clube UOL Premium. Outros cinco apps aguardam suas fontes de distribuição; fonte desconhecida nunca é instalada.
- Versões locais desconhecidas são bloqueadas pelo adapter e oferecem abertura separada explícita. PillTracker exige dados existentes válidos.
- Nenhum benefício UOL, dose/histórico médico ou calendário real foi alterado nos testes.

## Reprodução

```sh
node saquettoos/build.cjs --preview
node --test saquettoos/test-*.cjs saquettoos/server/test-access.mjs
git diff --check -- saquettoos
```

O manifesto stable registra os bytes publicados; `install.cjs` confere o mesmo hash após instalar somente `saquettoOS.js`.
