# Documentação & Changelog: Versão 5.5.0 (Clube UOL Stories Hunter)

> **Data da Atualização**: 17 de Outubro de 2026  
> **Versão**: `5.5.0`  
> **Commit Canônico**: `7fabefbe6`  
> **Tamanho do Script Compilado**: 666.513 bytes  
> **Localização de Origem**: `/Users/leosaquetto/Developer/GitHub/uol-bot/stories-hunter/`  
> **Destino iCloud iOS**: `/Users/leosaquetto/Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents/Clube UOL - Stories Hunter.js`  
> **Destino Repositório**: `/Users/leosaquetto/Developer/GitHub/uol-bot/scriptable_uol_stories_hunter.js`

---

## 1. Visão Geral da Atualização

A versão **5.5.0** traz uma grande evolução funcional e visual para o **Clube UOL - Stories Hunter**, implementando um novo centro de atividades no cabeçalho (*Activity Timeline*), agrupamento de sessões com múltiplas datas no catálogo histórico, suporte nativo a ofertas recém-esgotadas nas últimas 24h na aba Ingressos, unificação de fotos de perfil em alta definição via CDN Catbox e refinamentos estéticos de tipografia, tags dinâmicas e categorias.

---

## 2. Decisões de Design Alinhadas via `/grill-me`

Antes da codificação, o fluxo interativo do `/grill-me` estabeleceu três diretrizes essenciais de arquitetura e experiência:

1. **Hospedagem das Fotos de Perfil (Leo e Gab)**:
   - **Opção Escolhida**: Hospedagem com links diretos permanentes no **Catbox CDN**.
   - **Motivo**: Mantém o arquivo do Scriptable leve (sem inchar com strings Base64 de 400 KB) e permite cache nativo de imagem de alta velocidade no WebKit do iOS.
   - **URLs Geradas**:
     - Leonardo Saquetto: `https://files.catbox.moe/a44poo.jpg`
     - Gabriel: `https://files.catbox.moe/0hj4g2.jpg`

2. **Posicionamento de Esgotadas de 24h na Aba Ingressos**:
   - **Opção Escolhida**: Agrupadas ao final da lista com estilo translúcido / monocromático suave (`.card-expired`), mantendo sempre os ingressos disponíveis no topo.
   - **Motivo**: Evita que ingressos esgotados empurrem ofertas ativas para baixo ou confundam o usuário em busca de resgate imediato.

3. **Ícone do Header para a Linha do Tempo**:
   - **Opção Escolhida**: Sino de notificações com *dot* de novidades animado (`#btnTimelineModal`).
   - **Motivo**: Padrão universal para histórico de alertas, com indicação visual sutil quando houver novidades não lidas.

---

## 3. Detalhamento dos 8 Ajustes Implementados

### 3.1. Tela de Resgates: Filtro Estrito & Thumbnails Ampliadas
* **Filtro de Campanhas de Ingressos**:
  * Apenas benefícios onde `r.partner` seja correspondente a `"Campanhas de Ingressos"` são exibidos na lista e nas métricas de resgates. Vouchers de parceiros de desconto comercial (como *Netshoes*, *Original Colchões*, *Banquete do Chef*, *Pecorino*, *Spoleto*) foram filtrados da tela principal de ingressos.
  * Total consolidado: **21 resgates de ingressos autênticos** (12 do Leonardo e 9 do Gabriel).
* **Thumbnails Ampliadas**:
  * Dimensões aumentadas de `56px` para `74px × 74px`.
  * Bordas suaves com `border-radius: 14px`, `box-shadow` discreto e `object-fit: cover`.
* **Correção do C6 Fest do Gabriel**:
  * O destaque da curadoria do Gabriel foi corrigido de 2024 para **C6 Fest 2026** (Maio de 2026), com o ID `curated-c6fest`, local *Parque Ibirapuera SP* e thumbnail oficial CloudFront.

### 3.2. Histórico da Coleção: Agrupamento de Eventos em Múltiplos Dias
* Na aba **Histórico (Coleção)**, as ofertas de um mesmo evento que aconteceram em dias distintos (como as sessões de sexta, sábado e domingo do *Theatro Municipal*, *Teatro Claro* ou *Vibra SP*) agora são consolidadas em um **único card de evento**, exatamente como no menu Início.
* Cada dia de sessão aparece como uma pílula micro inline (`micro-date-pill`).
* Ao tocar no card, abre-se o modal nativo de sessões (`openGroupSessionsModal(group.key)`), permitindo inspecionar todos os horários e links individuais.

### 3.3. Tagline de Tempo Relativo
* **Substituição de Unidade**:
  * Todos os textos que usavam `"min"` foram encurtados para `"m"`:
    * Exemplo: `2h 15min atrás` ➔ `2h 15m atrás`.
    * Exemplo: `Há 3h 10m`, `45m atrás`, `1 dia atrás`.
* **Tipografia**:
  * Redução suave no tamanho da fonte (`font-size: 8px` a `9px` dependendo do container).
  * Aumento no espaçamento entre caracteres (`letter-spacing: 0.35px`) para leitura cristalina sem quebra de linha.

### 3.4. Tag "HOJE" Verde com Pulsação Suave
* Todas as pílulas de datas onde a sessão coincide com o dia atual (`dateStr === todayStr`) deixam de usar o tom azul ciano e adotam o **verde neon oficial do Clube UOL**:
  * `background: rgba(0, 230, 118, 0.16)`
  * `border-color: rgba(0, 230, 118, 0.45)`
  * `color: #00e676`
* Animação contínua e suave via `@keyframes pulseGreen` (oscilação de opacidade entre 0.6 e 1.0), herdando o dinamismo visual da antiga tag "NOVA".

### 3.5. Categoria Teatro UOL Corrigida
* Na função `classifyCategory(title, url, description)`:
  * O padrão de busca para `'teatro uol'` e `'teatro-uol'` foi movido para o topo das regras prioritárias, **antes** da checagem de palavras como `"show"`, `"música"` ou `"musical"`.
  * Qualquer espetáculo sediado no Teatro UOL agora recebe infalivelmente a categoria **TEATRO** com o ícone de máscaras teatrais.

### 3.6. Aba Ingressos com Ofertas Recém-Esgotadas (24h)
* A aba **Ingressos** (visualização detalhada sem agrupamento) agora busca no histórico todas as ofertas cujo encerramento (`expiredAt` ou `lastSeen`) tenha ocorrido nas últimas 24 horas (`Date.now() - timestamp <= 86.400.000 ms`).
* Essas ofertas são inseridas ao final da lista, após um separador elegante:
  * Divisor: `.soldout-24h-divider` com a legenda *"Esgotadas nas últimas 24h"*.
  * Estilo dos cards: `.card-expired` com thumbnail dessaturada, opacidade reduzida e badge cinza de esgotado.
* Após 24 horas, as ofertas saem automaticamente da aba Ingressos e continuam acessíveis apenas na aba Histórico.

### 3.7. Novo Centro de Notificações / Linha do Tempo no Header
* **Ícone no Header**:
  * Adicionado um botão circular com ícone de sino vetorial (`#btnTimelineModal`) posicionado imediatamente à esquerda do avatar do usuário ativo.
  * Dot de notificação (`.timeline-badge-dot`) em verde esmeralda com pulso discreto, indicando atividades recentes não lidas.
* **Modal Bottom-Sheet de Atividades (`#activityTimelineModal`)**:
  * Ao tocar no sino, abre um modal deslizante de baixo para cima (padrão nativo do app).
  * **Filtros com Chips**:
    * `Todas`: exibe a linha do tempo consolidada.
    * `Nova`: apenas ofertas descobertas (com distinção se foi *Stories* ou *Portal*).
    * `Esgotada`: ofertas que saíram do ar, com tempo total de permanência.
    * `Resgate`: ingressos resgatados na conta do Leonardo ou do Gabriel.
  * **Capacidade**: Limitado aos **40 eventos mais recentes**, ordenados cronologicamente (do mais novo para o mais antigo).
  * **Interatividade**: Tocar em um evento de oferta abre diretamente o modal de detalhes do espetáculo; tocar em um resgate redireciona para a aba de Resgates.

### 3.8. Fotos de Perfil em Alta Resolução (Catbox CDN)
* Substituição de todas as ocorrências antigas do serviço `unavatar.io` por links diretos permanentes:
  * Leonardo: `https://files.catbox.moe/a44poo.jpg`
  * Gabriel: `https://files.catbox.moe/0hj4g2.jpg`
* Aplicado no avatar do header, no modal de ajustes, no modal de alternância de conta, nos cards de resgate e no centro de notificações.

---

## 4. Estrutura de Arquivos Alterados

| Arquivo | Modificações |
| :--- | :--- |
| [`stories-hunter/template.html`](file:///Users/leosaquetto/Developer/GitHub/uol-bot/stories-hunter/template.html) | Implementação de CSS/HTML/JS para timeline, agrupamento de história, filtro de resgates, tag HOJE verde, tags de tempo e avatares. |
| [`stories-hunter/build.js`](file:///Users/leosaquetto/Developer/GitHub/uol-bot/stories-hunter/build.js) | Bump de versão para `5.5.0`, injeção de ícone do sino e empacotamento da WebView. |
| [`stories-hunter/svg_icons.json`](file:///Users/leosaquetto/Developer/GitHub/uol-bot/stories-hunter/svg_icons.json) | Adição do ícone vetorial `bell` (Majesticons Line). |
| [`scriptable_uol_stories_hunter.js`](file:///Users/leosaquetto/Developer/GitHub/uol-bot/scriptable_uol_stories_hunter.js) | Artefato JavaScript final gerado pelo compilador (`node build.js`), sincronizado com iCloud Drive. |

---

## 5. Validação e Testes de Qualidade

1. **Compilação e Tamanho**:
   - `node build.js`: Executado com saída limpa (código 0).
   - Tamanho final: `666.513 bytes`.
   - Sincronizado automaticamente com o iCloud Drive em `/Users/leosaquetto/Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents/Clube UOL - Stories Hunter.js`.
2. **Checagem de Sintaxe JavaScript**:
   - `node -c scriptable_uol_stories_hunter.js`: Zero erros de sintaxe ou referências inválidas.
3. **Teste de Sandbox em Máquina Virtual Node**:
   - Executado ambiente mock de DOM simulando a WebView do Scriptable:
     - `classifyCategory` testado com "Teatro UOL" ➔ Retornou `TEATRO`.
     - `formatRelativeTime` testado com intervalos reais ➔ Retornou `2h 15m atrás` e `5m atrás`.
     - `getTimelineEvents()` gerou 21 eventos consolidados sem exceções.
     - Filtros de timeline (`nova`, `esgotada`, `resgate`, `all`) validados com sucesso.
4. **Deploy & OTA**:
   - Disparo de push para `origin/main` no GitHub (`commit 7fabefbe6`).
   - Disparo de sinal de atualização via webhook **ntfy** para atualização automática em segundo plano no iPhone do usuário.
