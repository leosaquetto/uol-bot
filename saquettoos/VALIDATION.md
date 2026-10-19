# saquettoOS 1.0.1 — 2026-10-18

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
