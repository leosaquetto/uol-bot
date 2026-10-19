# saquettoOS 1.0.10 — 2026-10-19

- Last.fm Small recebeu margem interna no `viewBox` do SVG e perdeu apenas a camada de relevo deslocada que acentuava a base da marca; o vetor do símbolo foi preservado. A cápsula do PillTracker passou de 62% para 56% do recipiente, sem alterar o media-downloader.
- Build/sintaxe aprovado: 265.849 bytes, SHA-256 `2015299b0a81b0be64f926f485352f433aaf6f60b8a9940e3e80af6ad70b5341`. 17 testes UI aprovados e `git diff --check` limpo. Prévia clara e escura conferida no navegador integrado; capturas `/tmp/saquettoos-1.0.10-light.png` e `/tmp/saquettoos-1.0.10-dark-fixed-settled.png`, fora do Git.
- Limite: a prévia é sintética. O resultado visual no iPhone e a antiga tela preta após reaberturas ainda dependem de confirmação no Scriptable do aparelho.

# saquettoOS 1.0.9 — 2026-10-19

- Os seis apps usam símbolos SVG fornecidos, normalizados para máscara clara. O Super Calendário usa o último `IMG_5428_clean.svg` (15 caminhos preservados); PillTracker usa `capsule_icon_black.svg`. O UOL exibe somente seu símbolo, com traço ligeiramente engrossado. A marca saquettoOS usa o novo vetor Hedgy em claro/escuro.
- Grade e dock mantêm recipientes iguais. Os símbolos de PillTracker e media-downloader passaram de 68% para 62% do recipiente; Bilhete Único e Last.fm Small, para 64,6% (redução de 5%). Reflexo dos símbolos tem transição gradual. No claro, dois orbs ciano/violeta animados atravessam o vidro; material sólido e Reduzir Transparência ocultam os orbs, e Reduzir Movimento congela a animação.
- Build/sintaxe aprovado: 265.746 bytes, SHA-256 `0050e973e7b805d59fbbe9b07f29035911396704c0950660a69b9d990ef3f7b7`. 17 testes UI aprovados e `git diff --check` limpo. Prévia no navegador integrado: dez recipientes de 74 px, grade/dock idênticos, símbolos de 45,9 px (Pill/media), 47,8 px (Bilhete/Last.fm) e 50,3 px (demais), sem overflow ou erros/avisos no console. Claro e escuro inspecionados; capturas `/tmp/saquettoos-1.0.9-light.png` e `/tmp/saquettoos-1.0.9-dark.png`, fora do Git.
- Limite: a prévia é sintética e não comprova a apresentação no iPhone. A causa exata da antiga tela preta após reaberturas no Scriptable permanece sem confirmação no aparelho; os testes sintéticos de encerramento da 1.0.7 são a evidência disponível.

# saquettoOS 1.0.8 — 2026-10-19

- Corrigida a aplicação do tingimento: degradê Hedgy pertence ao recipiente de vidro no modo claro; o símbolo permanece branco/claro. Escuro conserva vidro neutro transparente e símbolo claro. Removidos os aros explícitos dos ícones nos dois modos e dos mini-ícones; profundidade usa somente reflexo e sombras suaves.
- Tamanhos de grade/dock e proporção dos símbolos preservados. Navegador integrado confirmou os dez ícones com aproximadamente 74 px, borda de 0 px e ausência do pseudo-elemento de aro. Claro e escuro conferidos; console sem erros/avisos capturados. Capturas `/tmp/saquettoos-1.0.8-light.png` e `/tmp/saquettoos-1.0.8-dark.png`, fora do Git.
- Build/sintaxe e diff-check aprovados. Alteração visual apenas: os 37 testes direcionados executados na 1.0.7 continuam sendo a evidência para o encerramento, sem repetição desnecessária. Correção de espera pendente preservada; confirmação da tela preta no iPhone continua pendente.

# saquettoOS 1.0.7 — 2026-10-19

- Ícones padronizados: vidro neutro transparente no escuro, vidro tingido e símbolos no degradê Hedgy no claro. SVG UOL fornecido preservado byte a byte e usado como máscara, com a mesma caixa de 68% dos demais símbolos. Customizações com imagem continuam suportadas. Sem blur por ícone; fundos opacos em material sólido/redução de transparência.
- Bundle passou de 4.698.514 para 2.910.011 bytes (redução de 38,1%) ao substituir o PNG UOL embutido pelo SVG. Isso reduz a carga; não comprova que memória era a causa da tela preta.
- Reproduzido no host anterior: fechar enquanto `evaluateJavaScript` nunca resolve impedia a conclusão do script; avaliações pendentes e enfileiradas não rejeitavam ao fechar. A ponte agora libera sua própria espera na dispensa da tela, rejeita a fila e ignora resultados tardios. Script.complete fica em finally. Nenhuma API de cancelamento da avaliação nativa é presumida. Operações nativas já iniciadas continuam aguardadas.
- 37 testes direcionados aprovados: 10 host, 17 UI e 10 ciclo de vida. Novo cenário completa seis sessões consecutivas com avaliação eternamente pendente, sem alertas falsos, timers ou novas avaliações depois de fechar. Ambas as regressões falharam contra HEAD anterior em fixture isolada e passaram com a correção. Build/sintaxe, diff-check e revisão final do encerramento aprovados.
- Navegador integrado: claro/escuro conferidos com dimensões CSS 390 × 844 e insets sintéticos 91/34 px. Os dez ícones de grade/dock medem aproximadamente 74 × 74 px, máscaras presentes e símbolos 50,3 px; sem overflow do documento e sem erros/avisos capturados. Capturas `/tmp/saquettoos-1.0.7-dark.png` e `/tmp/saquettoos-1.0.7-light.png`, fora do Git.
- Limite: registro sincronizado da última execução 1.0.6 consultada mostrava dashboard_ready, dismissed e complete; não capturou a falha relatada. Causa exata da tela preta após reaberturas no iPhone permanece não confirmada. Testes de encerramento são sintéticos; validação no Scriptable real continua necessária. Nenhum app ou dado pessoal foi executado pelos testes.

# saquettoOS 1.0.6 — 2026-10-19

- Foto do iPhone confirmou abertura direta da 1.0.5 e regressão de layout: cabeçalho sob a barra nativa. A regra de altura fixa havia sobrescrito o padding com `safe-area-inset-top`. Restaurado em todos os breakpoints, incluindo modo compacto; painéis também respeitam esse limite superior. Sem altura de toolbar fixada no código.
- A [API WebView do Scriptable](https://docs.scriptable.app/webview/#-present) documenta apresentação fullscreen, sem controle para ocultar Close/Share. Insets seguem o [contrato de área segura do WebKit](https://webkit.org/blog/7929/designing-websites-for-iphone-x/).
- Detecção de modo compacto desconta insets superior/inferior. Quando não há altura para os seis apps e nomes, somente a grade encolhe/rola; busca e dock permanecem fixos, com dimensões de ícone iguais.
- 17 testes UI aprovados, build/sintaxe e diff-check. Navegador integrado com insets sintéticos de 91 px acima/34 px abaixo: em 390 × 844, padding superior 111 px e painel começa em y=111; dock termina em y=788. Em 390 × 380 com nomes ligados, cabeçalho começa em y=98 e dock termina em y=346; rolagem da grade permite alcançar o último nome sem mover o dock. Console sem erros/avisos capturados. Captura `/tmp/saquettoos-1.0.6-safe-area.png` fora do Git.
- Os insets foram simulados no Chromium: isso verifica a geometria CSS, não o valor informado pelo WKWebView do Scriptable. Confirmação da área segura real permanece no iPhone.

# saquettoOS 1.0.5 — 2026-10-19

- Ajuste visual adicional: “lugar” e os avatares com inicial, na home e nos Ajustes, usam o mesmo degradê Hedgy do “OS” claro. Letras brancas e indicador de presença preservados.
- Build/sintaxe e diff-check aprovados. No navegador integrado, as três superfícies da home clara retornam o mesmo gradiente computado; avatar dos Ajustes também conferido. Captura local `/tmp/saquettoos-1.0.5-light.png`. Sem mudanças de comportamento que justifiquem repetir os testes da 1.0.4.

# saquettoOS 1.0.4 — 2026-10-19

## Tela inicial e identidade visual

- Primeiro documento já contém a tela inicial; entrada suave em camadas substitui o texto de preparação. Avaliações da ponte continuam posteriores à apresentação nativa e à espera de 800 ms. Verificação de acesso atualiza apenas o estado, sem reenviar os assets ou recarregar o documento.
- Home ocupa a área visível sem rolagem; dock permanece na base. Campo de busca usa fonte de 16 px e a interface acompanha a altura do VisualViewport. Altura zero ou inválida antes da apresentação é ignorada. Pinch zoom permanece disponível.
- Preferência persistente `showAppNames` começa desativada e controla grade e dock. Falha ao salvar restaura o estado confirmado. Nomes acessíveis permanecem nos botões.
- Logo claro substituído pelo PNG transparente fornecido; recorte CSS remove a margem branca do logo escuro. “OS” claro usa o degradê ciano/azul/violeta/rosa. Gradientes dos apps foram suavizados e símbolos internos usam 68% do bloco. Grade e dock compartilham a mesma dimensão de ícone.
- 35 testes direcionados aprovados nesta mudança: 10 host, 9 ciclo de vida e 16 UI. Build valida sintaxe do artefato e da interface; diff-check aprovado.
- Navegador integrado: em 390 × 700 CSS, os dez ícones medem aproximadamente 66 × 66 px, documento sem overflow e dock termina em y=671. Em 390 × 380 CSS, com seis apps e nomes ligados, todos medem 44 × 44 px e dock termina em y=373. Busca por UOL retorna um app, escala permanece 1 e fonte mede 16 px. Console sem erros/avisos capturados. Captura local: `/tmp/saquettoos-1.0.4-light.png` (fora do Git).
- Limite: a altura reduzida simula o espaço ocupado pelo teclado; não prova comportamento do teclado ou zoom no iOS. A nova abertura direta e entrada animada precisam de confirmação no Scriptable do iPhone. Nenhum app real ou dado pessoal foi executado pelos testes.

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
