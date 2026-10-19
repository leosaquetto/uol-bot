# saquettoOS 1.0.0 — 2026-10-18

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
