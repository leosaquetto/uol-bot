# Handoff: Clube UOL Premium (Scriptable)

> **Data de Atualização**: 18 de Outubro de 2026
> **Status**: Contrato atual em [CHANGELOG_v5.8.1.md](CHANGELOG_v5.8.1.md) e [ARCHITECTURE.md](ARCHITECTURE.md); publicação e execução real são registradas separadamente no documento operacional.
> **Localização do Desenvolvimento**: `/Users/leosaquetto/Developer/GitHub/uol-bot/stories-hunter/`  
> **Destino iCloud iOS**: `/Users/leosaquetto/Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents/Clube UOL Premium.js`
> **Destino Repositório**: `/Users/leosaquetto/Developer/GitHub/uol-bot/scriptable_uol_stories_hunter.js`

---

## Contrato atual 5.8.1

Consulte [CHANGELOG_v5.7.0.md](CHANGELOG_v5.7.0.md). Esta revisão substitui as regras
antigas de tags inline nos cards: agora ficam em faixa horizontal rolável, separada
do título e em ordem cronológica. Home mostra ativas e esgotadas confirmadas nas
últimas 24 horas; Coleção preserva o histórico completo.

`collector.js` roda no host Scriptable e encaminha descobertas públicas ao Worker.
`access.js` solicita aprovação individual e importa a configuração técnica para o
Keychain; bootstrap privado antigo continua compatível. Permissões pessoais são
concedidas separadamente pelo proprietário. `redemption.js` verifica a identidade
da conta e o recibo antes de afirmar sucesso. Sessões, credenciais e vouchers não
entram no artefato público. Atualização usa `--code-only`; a primeira abertura da
versão não coleta. A documentação histórica abaixo não autoriza sobrescrever
sessões, vouchers, datas de resgate ou o banco privado numa atualização de código.

Reconexão pessoal usa “Conectar conta ao Hunter”: login oficial no WebView,
retorno OAuth interceptado com o estado criado pelo transporte nativo e troca do
código uma única vez. Cookies candidatos ficam apenas em memória até o histórico
confirmar a identidade esperada e a permissão continuar válida. Só a sessão da
conta selecionada no Keychain é substituída; outro perfil, arquivos privados e
journal de resgate são preservados. Login comum no navegador não importa sessão
para o Hunter. Desafios do UOL continuam no formulário oficial, sem automação.

---

## Personalização da barra

Ajustes → Personalização oferece Padrão, Roxo, Laranja e Branco. O Padrão está
registrado com CSS e marcação originais em [NAVIGATION_BAR_BASELINE.md](NAVIGATION_BAR_BASELINE.md).
As variantes mudam somente cores do vidro, borda, sombra, ícones e ponto ativo.
A preferência é individual por aparelho, salva pelo host Scriptable em
`FileManager.local()/uol_navigation_theme.json`; não depende de iCloud. A escolha
nativa prevalece sobre cache antigo da WebView e sobrevive a atualização de código.
Falha ao salvar restaura o último tema confirmado.

## 1. Arquitetura e distribuição atuais

A fonte de verdade para build, updater, promoção e recuperação é
[ARCHITECTURE.md](ARCHITECTURE.md). Fontes modulares geram um único
`dist/Hunter.js` autocontido. HTML, CSS, JS e host continuam em poucos blocos grandes;
coletores, acesso e resgate mantêm seus contratos. Assets utilizados são versionados.

O build público `--prepare` não lê dados privados. A distribuição `--code-only`
exige os bytes promovidos a stable, preserva o bootstrap privado existente e não
reescreve histórico ou vouchers. GitHub/MacSend recebem o artefato sanitizado.
O espelho `../scriptable_uol_stories_hunter.js` mantém compatibilidade técnica.
Publicação normal usa ntfy dedicado → Atalho `update-auto`, sem depender do Mac.
Migração e instalação inicial: [PREMIUM_MIGRATION.md](PREMIUM_MIGRATION.md).

## 2. Referência de equivalência

[REFACTOR_CHECKPOINTS.md](REFACTOR_CHECKPOINTS.md) registra tamanhos, hashes e
validações por etapa. `baseline/` preserva a versão pública 5.7.9. A suíte compara
os outputs completos e usa somente fixtures sintéticas para fluxos sensíveis.

As seções históricas abaixo preservam decisões anteriores e exemplos com nomes
antigos como `template.html`; caminhos e publicação devem seguir a arquitetura
atual. A ordem e as sobrescritas de CSS existentes são a referência visual.

---

## 3. Regras de Design e Especificações de Layout

> [!IMPORTANT]
> As regras abaixo foram refinadas através de rigoroso feedback visual e não devem ser revertidas.

### 3.1. Anatomia e Proporções dos Cards

1. **Card de Destaque Hero (Topo)**:
   - Dimensões: `width: 100%; height: 350px; border-radius: 24px;`.
   - Borda luminosa âmbar com animação pulsante suave (`#ffb300`).
   - Linha inferior (`.destaque-bottom-row`):
     - **Título** à esquerda em até 2 linhas (`font-size: 21px; font-weight: 700; line-height: 1.2;`).
     - **Tag de data** (`micro-date-pill`): posicionada **obrigatoriamente inline logo após o título** (ex: `Miró: Mestre das Formas [📅 ATÉ 13 DEZ]`). **NUNCA** isolada em container separado acima do título.
     - **Botão Resgatar** (`🎟 Resgatar`) à direita com gradiente âmbar/laranja (`padding: 9px 20px; font-size: 13.5px; border-radius: 20px;`).

2. **Cards dos Carrosséis (Poster Cards)**:
   - Dimensões: `width: 225px; min-width: 225px; height: 335px; border-radius: 20px;`.
   - **SEM botões de ação**: Os poster cards dos carrosséis **NÃO** possuem botões "Resgatar" ou "Detalhes". O card inteiro é interativo e clicável diretamente para exibir detalhes ou abrir sessões.
   - **Título estritamente embaixo**: O conteúdo textual fica posicionado no rodapé (`.poster-bottom-content`) sobre um gradiente escuro profundo (*scrim*).
   - **Local/Venue**: Exibido logo acima do título com tipografia discreta (`font-size: 12.5px; font-weight: 500; color: rgba(255, 255, 255, 0.72);`).
   - **Título**: Até 3 linhas (`-webkit-line-clamp: 3; font-size: 16px; font-weight: 600; line-height: 1.25;`).
   - **Destaque em Carrosséis Inferiores**: Quando um evento em destaque é repetido nos carrosséis por categoria, utiliza a badge `.pill-destaque-star-only` (círculo compacto dourado com estrela) no canto superior direito, evitando empurrar a tag de tempo relativo (`9h38min atrás`) para duas linhas.

---

### 3.2. Regra Anti-Órfã para Tags de Data (`formatTitleWithInlineDates`)

Para impedir que a tag de data (`[📅 ATÉ 13 DEZ]` ou `[📅 HOJE]`) caia sozinha em uma linha vazia abaixo do título quando houver quebra de linha:

```javascript
// Função implementada em template.html:
function formatTitleWithInlineDates(rawTitle, datesHtml) {
  if (!datesHtml) return escapeHtml(rawTitle);
  const words = rawTitle.trim().split(/\s+/);
  if (words.length === 1) {
    return `<span class="nowrap-title-tag">${escapeHtml(words[0])} ${datesHtml}</span>`;
  }
  // Amarra a última palavra junto com a tag de data
  const lastWord = words.pop();
  const rest = words.join(" ");
  return `${escapeHtml(rest)} <span class="nowrap-title-tag">${escapeHtml(lastWord)} ${datesHtml}</span>`;
}
```

- **Comportamento Garantido**:
  - Títulos curtos (ex: `Dois Papas`): permanecem em 1 única linha com a tag ao lado direito.
  - Títulos médios: caso a tag não caiba na linha 1, o texto quebra uma palavra antes (ex: `Miró: Mestre das` / `Formas [📅 ATÉ 13 DEZ]`), fazendo a tag ficar sempre acompanhada da última palavra.
  - Títulos longos: podem quebrar em até 3 linhas sem que a tag fique isolada.

---

### 3.3. Normalização de Títulos e Locais (Tratamento de Inversões da API UOL)

A API do Clube UOL frequentemente inverte título e local (ex: enviando o teatro como título e o nome da peça como parceiro/descrição). O script possui normalizadores dedicados:

- **Percy Jackson**:
  - Local forçado: `Teatro Liberdade`
  - Título forçado: `Percy Jackson – O Ladrão de Raios: O Musical`
- **Uma Noite em Buenos Aires**:
  - Local forçado: `Theatro Municipal` ou `Teatro Bradesco`
  - Título forçado: `Uma Noite em Buenos Aires`

---

## 4. Tipografia e Cores

- **Família Tipográfica**: `Manrope`, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif.
- **Pesos Utilizados**:
  - `300` / `400`: Descrições secundárias e datas detalhadas.
  - `500`: Locais (`.poster-venue`), tempos decorridos e metadados.
  - `600`: Títulos de carrosséis (`.poster-title`) e títulos de seções.
  - `700`: Título do Hero (`.destaque-hero-title`), botões de resgate e tags de data.
  - `800`: Badges de status (`NOVA`, `DESTAQUE`, `ESGOTADO`).
- **Micro-Date Tag CSS**:
  ```css
  .micro-date-pill {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    background: rgba(0, 0, 0, 0.65);
    -webkit-backdrop-filter: blur(8px);
    backdrop-filter: blur(8px);
    border: 0.5px solid rgba(56, 189, 248, 0.45);
    color: #38bdf8;
    padding: 1px 3.5px;
    border-radius: 3.5px;
    font-family: 'Manrope', -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 7.2px;
    font-weight: 700;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    white-space: nowrap;
    vertical-align: 1.5px;
    line-height: 1.1;
  }
  ```

---

## 5. Como Compilar e Sincronizar

Para preparar a integração 5.6.0 sem instalar nem notificar:

```bash
node stories-hunter/build.js --prepare --code-only
node --test stories-hunter/test-*.cjs
```

Após validar/publicar o Worker, a instalação `node stories-hunter/build.js --code-only`
preserva o banco de resgates e os vouchers existentes no iCloud. O catálogo vem de
`GET /catalog`, com cache de 120 segundos e atualização somente ao abrir. Consulte
`CHANGELOG_v5.6.0.md` para limites, fallback e preservação do histórico.

O artefato público/MacSend contém somente código. O pacote privado do iCloud mantém
os cookies compartilhados; arquivos de sessão por usuário e banco privado são
reutilizados nas atualizações públicas. Uma primeira instalação para outra pessoa
precisa do pacote privado autorizado, não apenas do arquivo público do GitHub.

O comando legado abaixo também sincroniza o banco de resgates e vouchers:

```bash
cd /Users/leosaquetto/Developer/GitHub/uol-bot/stories-hunter
node build.js
```

### Validação de Sintaxe (0 Erros)
```bash
node -c /Users/leosaquetto/Developer/GitHub/uol-bot/scriptable_uol_stories_hunter.js
node -c "/Users/leosaquetto/Library/Mobile Documents/iCloud~dk~simonbs~Scriptable/Documents/Clube UOL Premium.js"
```

---

---

## 6. Publicação e Atalhos

Fluxo atual em [PREMIUM_MIGRATION.md](PREMIUM_MIGRATION.md). Não enviar mais título
`Scriptable` para uma publicação UOL: ele pertence à automação genérica dos outros
scripts. Premium usa título `Clube UOL Premium`, sem stage genérico ou sobrescrita
do iCloud na publicação normal. O arquivo de Atalho dedicado é assinado pela Apple.

## 7. Instalação e dados

`--code-only` publica o aviso para o updater. `--code-only --install` realiza
instalação/migração local explícita com backup. Não sincronizar dados privados como
parte de atualização de código. Arquivos são distribuídos diretamente, sem ZIP.

---

## 8. Menu Inferior Fixo e Arquitetura de 4 Abas (Glassmorphism)

### 8.1. Estrutura Visual da Bottom Navigation Bar
- **Posicionamento**: Fixo e flutuante em formato cápsula no rodapé com `position: fixed; bottom: calc(env(safe-area-inset-bottom, 0px) + 16px); left: 50%; transform: translateX(-50%); width: fit-content; max-width: 90%; z-index: 9000;`.
- **Estilo Glassmorphism**: Idêntico ao cabeçalho superior, com fundo `rgba(0, 0, 0, 0.72)`, `-webkit-backdrop-filter: blur(24px) saturate(180%)`, borda translúcida `0.5px solid rgba(255, 255, 255, 0.14)`, sombra profunda `0 12px 36px rgba(0, 0, 0, 0.6)` e reflexo interno `inset 0 1px 1px rgba(255, 255, 255, 0.15)`. Raio de curvatura: `border-radius: 36px; padding: 6px 14px; gap: 12px;`.
- **Safe Area Insets**: O corpo da página (`body`) possui `padding-bottom: calc(env(safe-area-inset-bottom, 0px) + 90px)` para evitar que o conteúdo final fique encoberto pelo menu flutuante.
- **Camadas e Modais**: Todos os modais (`.modal-backdrop`) operam em `z-index: 10000+`, cobrindo harmoniosamente tanto o cabeçalho superior quanto o menu flutuante.
- **Micro Indicador Ativo**: Ponto de luz âmbar centralizado (`.active-dot`, `4px × 4px`, `border-radius: 50%`, `background: #ffb300`, sombra `0 0 6px #ffb300`).
- **Toque e Navegação**: Efeito de micro touch scale `:active` (`transform: scale(0.92)`). Tocar na aba já ativa rola suavemente para o topo da tela (`scrollTo({ top: 0, behavior: 'smooth' })`).
- **Ícones SVGs (sem texto)**:
  1. **Início (`home`)**: `casa.svg` (`__ICON_navHome__`) — Carrosséis verticais por categoria estilo Vivo Play (`isGrouped = true`), banner Hero de destaque.
  2. **Ingressos (`tickets`)**: `icone-circular.svg` (`__ICON_navTickets__`) — Lista contínua de cards individuais (`isGrouped = false`) com chips de categorias para filtragem rápida.
  3. **Histórico (`history`)**: `cartoes-empilhados.svg` (`__ICON_navHistory__`) — Tela dedicada `#viewHistory` com catálogo cronológico e arquivo permanente.
  4. **Resgates (`redemptions`)**: `mao-com-bilhetes.svg` (`__ICON_navRedemptions__`) — Tela dedicada `#viewRedemptions` nativa de vouchers e perfis.

### 8.2. Catálogo Histórico e Seção Editor's Choice
- **Sem Estados Esgotados**: Os cards são exibidos como pôsteres limpos, sem faixas vermelhas de "esgotado", badges pejorativas ou botões de resgate direto. O clique abre os detalhes completos no modal existente.
- **Barra de Métricas e Insights**: Três pílulas de vidro fosco no topo com:
  - *Total Catalogado* (contagem consolidada de eventos únicos ativos e históricos);
  - *Categoria Top* (categoria predominante do acervo, ex: Teatro ou Museus);
  - *Mês Recorde* (mês com maior volume de campanhas lançadas, ex: `Out 2026`).
- **Editor’s Choice (★ Curadoria)**:
  - Carrossel exclusivo no topo do catálogo destacando eventos icônicos (Zayn, Maroon 5, Robbie Williams, Xuxa, C6 Fest).
  - Badge dourada âmbar `★ Curadoria` nos pôsteres e dentro do modal de detalhes.
  - Sementes pré-carregadas (`CURATED_EDITORS_CHOICE_SEEDS`) integradas ao catálogo completo, sistema de busca e detalhes.
- **Navegação Cronológica Progressiva (Ano → Mês → Ofertas)**:
  - Seletor horizontal de anos com chips clicáveis (`.year-chip`).
  - Seletor horizontal de meses daquele ano com chips dinâmicos (`.month-chip`), permitindo filtrar por mês específico ou exibir todos.
  - Agrupamento mensal automático ordenado do mais recente ao mais antigo, com carrosséis horizontais dedicados por mês.
  - Itens de curadoria presentes no mês exibem automaticamente o selo distintivo `★ Curadoria`.
- **Paridade de Normalização de Títulos e Locais**: O catálogo histórico utiliza a mesma função `normalizeEventInfo([o])` da Home, garantindo que inversões de teatro/peça (como *Percy Jackson* e *Uma Noite em Buenos Aires*), formatação de títulos inteligentes extraídos da descrição e tags de data anti-órfã sejam aplicadas de forma consistente em todos os cards do Histórico e no modal de detalhes.

### 8.3. Tela Nativa "Meus Resgates"
- **Perfil do Usuário Ativo**: Card translúcido com avatar dinâmico (Instagram via `unavatar.io`), nome completo (*Leonardo Saquetto* ou *Gabriel*), e botão *"Alternar"* com isolamento estrito de dados por perfil.
- **Barra de Status de Sincronização**: Indicador verde ao vivo (`Sincronizado via Clube UOL`) e pílula com a contagem de ingressos do usuário ativo.
- **Cards de Resgate com Ações Duplas**:
  - **Formatação de Título em Sentença Sem Inversão**: Utiliza `formatRedemptionTitle(r.title)` preservando preposições minúsculas, siglas em maiúsculas (`SP`, `RJ`, `UOL`, `OFF`, `C6`) e termos de campanhas sem aplicar inversões de nome de peça/teatro da home.
  - **Data e Horário de Execução do Resgate**: Exibe a data e hora exata em que o voucher foi gerado no Clube UOL (ex: `Resgatado em 08/10/2026 às 11:09`), extraída do cabeçalho oficial do benefício.
  - **Dois Botões de Ação por Card**:
    1. `[Ver Detalhes]`: Abre o modal nativo in-app `#redemptionDetailsModal` com a sinopse completa, horários de portão, endereço e regras de resgate.
    2. `[Ver Voucher]`: Abre o modal nativo in-app `#voucherModal` estilizado como bilhete digital com picote, token de acesso, botão de cópia com 1 toque e link para PDF oficial.

### 8.4. Top Header Minimalista e Centralização de Ajustes no Avatar
- **Lado Esquerdo**: Logotipo oficial Clube UOL com ponto pulsante de status de conexão ao vivo (`#liveDot`) e texto de estado.
- **Lado Direito**: **Avatar Único Centralizador**. O ícone da engrenagem foi removido do topo para manter a estética extremamente limpa e focada. Tocar no Avatar abre diretamente o modal de Ajustes & Configurações (`#settingsModal`), unificando o controle de palavras-chave, status de credenciais e alternância de perfil.

### 8.5. Modais Nativos In-App para Vouchers e Detalhes
1. **Modal Digital Ticket Voucher (`#voucherModal`) com Encaixe Milimétrico e Renderizador Nativo**:
   - **Extração de Alta Fidelidade (Aspect Ratio Perfeito)**: Os vouchers do Clube UOL são gerados em PDFs de página única com canvas de 2000 × 930 pontos (proporção horizontal ~2.15:1). A função `extractVoucherImage(arrayBuffer)` extrai o JPEG de alta resolução diretamente dos streams do PDF (tanto raw JPEG quanto zlib-deflate via `DecompressionStream`). O voucher é renderizado como `<img>` com `width: 100%; height: auto; object-fit: contain; border-radius: 11px;`, eliminando o corte de arestas e textos gigantescos que ocorriam no zoom 100% de iframes no WebKit.
   - **Prevenção de Overflow & Segurança de Toque no iOS**:
     - `.modal-backdrop` configurado com `align-items: flex-start; overflow-y: auto; -webkit-overflow-scrolling: touch; cursor: pointer;` e padding com `env(safe-area-inset-top)` / `env(safe-area-inset-bottom)`.
     - `.modal-box` utiliza `margin: auto 0; max-height: calc(100vh - 75px);`, garantindo centralização quando cabe na tela e rolagem fluida quando necessário, sem que o topo fique preso em coordenadas negativas.
     - Botão de fechar `(x)` expandido (`36px × 36px`, `z-index: 1000`, `touch-action: manipulation`) com `stopPropagation()`.
     - Tocar no fundo escuro fora do card fecha o modal imediatamente via `handleBackdropClick(event)`.
   - **Sistema de Cache Persistente em 2 Camadas (Offline-First / Zero Requisições Redundantes)**:
     - **Camada 1 (RAM / Sessão)**: `voucherSessionCache` (Map) armazena a imagem e o blob do PDF em memória. Abrir o mesmo voucher múltiplas vezes na mesma sessão custa 0 ms e zero tráfego de rede.
     - **Camada 2 (LocalStorage Persistente)**: Salva o Base64 da imagem e do PDF original sob a chave `uol_vc_${cupomCode}`. Uma vez baixado pela primeira vez, o voucher **nunca mais precisa ser baixado da internet**. Ao reabrir o script mais tarde, amanhã ou em modo offline/avião, o voucher carrega instantaneamente em 0 ms e o botão de compartilhamento reconstrói o arquivo nativo para o Share Sheet sem requisitar a UOL.
   - **Compartilhamento e Salvamento Nativo iOS**: O `currentVoucherBlob` continua preservando o arquivo original (PDF/PNG), acionando o menu nativo do iOS via Web Share API (`navigator.share({ files: [file] })`) para salvar em Arquivos, WhatsApp, AirDrop ou Fotos.
   - **Cupons Promocionais de Texto**: Caso o benefício seja um código promocional (ex: `clubeuol50`), exibe caixa estilizada com botão de cópia tátil e feedback imediato.
2. **Modal de Detalhes Completos do Resgate (`#redemptionDetailsModal`)**:
   - Exibe thumbnail oficial do evento, selo de resgate confirmado com timestamp, sinopse detalhada do evento, endereço completo do local e caixa destacada de **Regras Oficiais de Resgate**.
   - Botão direto para alternar instantaneamente para a visualização do Voucher in-app.
3. **Pipeline de Resgate Headless Inteligente (`executeRedemption`)**:
   - **Tratamento Preciso de Cota Mensal**: Detecta URLs e payloads de limite (`max-redeem`, `limite de resgates excedido`, `já utilizou`) e exibe aviso claro sem desconfigurar a interface nem redirecionar para telas de erro.
   - **Transição Fluida In-App**: Ao confirmar o resgate com cota disponível, executa o resgate headless, dispara notificação NTFY com os metadados do voucher, sincroniza os resgates reais via `fetchRealRedemptions()` e faz a transição automática para a aba `Resgates` (`switchTab('redemptions')`) sem fechar o Scriptable nem abrir o Safari.
4. **Pipeline de Dados Autênticos do Editor's Choice**:
   - As sementes curadas para **Zayn**, **Maroon 5** e **C6 Fest** utilizam as imagens oficiais hospedadas na CloudFront do Clube UOL, local exato (*Nubank Parque SP* e *Parque Ibirapuera SP*), horários reais e descrições completas extraídas diretamente dos resgates do usuário.
   - O C6 Fest é vinculado diretamente à conta de Gabriel (`user: "gaoriel"`).
   - As referências a Robbie Williams e Xuxa foram descontinuadas da curadoria.

### 8.6. Arquitetura Multi-Conta e Isolamento de Sessões Headless
1. **Credenciais Duplas em `clubeuolscrip.txt`**:
   - O arquivo em `iCloud/Documents/clubeuolscrip.txt` (e fallback em `~/Downloads`) contém blocos estruturados para ambos os usuários:
     - Bloco `leo`: login/email e senha de Leonardo Saquetto.
     - Bloco `gaoriel`: login e senha de Gabriel.
   - A função `parseCredentialsText` em `build.js` realiza o parsing por cabeçalho de bloco ou chaves explícitas, populando `credentials.accounts.leosaquetto` e `credentials.accounts.gaoriel`.
2. **Modal de Confirmação de Alternância (`#confirmSwitchUserModal`)**:
   - A troca de usuário **não é executada silenciosamente**. Ao tocar na pílula de usuário em Ajustes ou no botão *"Alternar"* na aba Resgates, abre-se um modal dedicado com:
     - Comparação visual lado a lado: Avatar atual ➔ Avatar de destino com badge `Próxima`.
     - Confirmação de isolamento de sessão: salvamento dos cookies ativos e injeção dos cookies do novo perfil.
     - Status das credenciais mascaradas para privacidade (ex: `🟢 Credenciais prontas: lar••••to`).
     - Botões táteis *"Confirmar e Alternar"* (gradiente âmbar/laranja) e *"Cancelar"*.
3. **Armazenamento Simultâneo de Cookies (`settings.userSessions`)**:
   - O Scriptable mantém o dicionário `settings.userSessions = { leosaquetto: [...], gaoriel: [...] }` persistido no arquivo de ajustes do iCloud.
   - Ao confirmar a troca (`executeUserSwitch`):
     1. Tira snapshot dos cookies do usuário que está saindo e armazena em `settings.userSessions[prevUser]`.
     2. Limpa e expira os cookies do `document.cookie` no WebKit para evitar contaminação cruzada de sessões.
     3. Atualiza `settings.currentUser` para o novo perfil.
     4. Injeta os cookies exclusivos do novo perfil via `initSessionCookies(...)`.
     5. Dispara a sincronização de resgates reais via `fetchRealRedemptions()`, mesclando os vouchers sem sobrescrever os dados da outra conta.
     6. Exibe notificação tátil tipo Toast no topo da tela (ex: `✓ Sessão alternada para Gabriel (@gaoriel)`).

### 8.7. Arquitetura de Banco de Dados Permanente e Arquivamento de Vouchers
1. **Banco de Dados Permanente (`uol_redemptions_database.json`)**:
   - Localizado permanentemente no iCloud Drive em `iCloud Drive/Scriptable/uol_redemptions_database.json` e espelhado localmente no repositório em `stories-hunter/cached_redemptions_details.json`.
   - Não depende de `localStorage` da WebView nem é afetado por limpeza de cache do Safari/WebKit ou reinicializações do app.
   - Registra para cada benefício: ID, título higienizado, parceiro, thumbnail, data/hora exata do resgate, sinopse completa, regras oficiais, URL de download do voucher, código do cupom, nome do arquivo local e dados da imagem para render instantâneo.
2. **Repositório de Vouchers Físicos (`vouchers/`)**:
   - Pasta dedicada sincronizada no iCloud Drive em `iCloud Drive/Scriptable/vouchers/` e no repositório em `stories-hunter/vouchers/`.
   - Contém os arquivos binários autênticos (PDFs vetoriais originais e imagens PNG/JPG em alta resolução).
   - Acessíveis pelo usuário a qualquer momento através do aplicativo **Arquivos (Files)** do iOS ou Finder do Mac.
3. **Renderização Instantânea Zero-Rede (Offline-First)**:
   - Os vouchers com imagens embutidas (Zayn, Maroon 5, Festival Interlagos, Titaníque Teatro, etc.) contam com seus dados de alta definição embutidos diretamente no banco (`voucherImageDataUrl`), abrindo em **0 ms** mesmo em Modo Avião ou com a conexão desativada.
   - A ação *"Salvar / Compartilhar"* converte os dados do banco para arquivo nativo em memória via `Blob` e aciona o menu de compartilhamento do iOS (`navigator.share`), permitindo salvar no Rolo da Câmera ou enviar via WhatsApp/AirDrop instantaneamente.
4. **Sincronização Bidirecional Contínua**:
   - Ao inicializar o Scriptable, a função `loadRedemptionsDatabase()` mescla a base embutida e os dados do arquivo do iCloud.
   - Ao descobrir novos resgates pela sincronização nativa headless, os novos itens são agregados ao banco e persistidos imediatamente em disco via `saveRedemptionsDatabase()` e `saveSettings()`.


## 9. Arquitetura Dual-Account & Captura de Sessão (Leo & Gabriel) - 17/10/2026

- **Sessões Autenticadas**:
  - `leosaquetto`: `~/.config/uol-sentinel/leo-bootstrap.json` (12 resgates)
  - `gaoriel`: `~/.config/uol-sentinel/gaoriel-bootstrap.json` (20 resgates)
- **Cookies Pré-compilados**:
  - `BUNDLED_COOKIES_LEO` e `BUNDLED_COOKIES_GAORIEL` injetados diretamente no script compilado.
  - Alternância de perfil em 1 toque no avatar ou menu de ajustes com troca instantânea de cookies de sessão no WebKit (sem prompts de login nem loop SSO).
- **Banco de Dados Permanente**:
  - 32 resgates consolidados em `uol_redemptions_database.json` (iCloud e offline).
  - 27 arquivos de vouchers oficiais salvos em `vouchers/` e na pasta de Documentos do iCloud Scriptable.
- **Sincronização & Compartilhamento**:
  - Script autossuficiente e pronto para ser compartilhado entre amigos, com banco offline embutido e isolamento de resgates por perfil ativo.

## 10. Atualização v5.5.0: Linha do Tempo de Atividades, Agrupamento na Coleção & UI (17/10/2026)

- **Changelog Completo**: Consulte [`stories-hunter/CHANGELOG_v5.5.0.md`](file:///Users/leosaquetto/Developer/GitHub/uol-bot/stories-hunter/CHANGELOG_v5.5.0.md) para detalhes exaustivos da arquitetura.
- **Centro de Notificações no Header**: Botão de sino com badge dot dinâmico à esquerda do avatar, abrindo modal bottom-sheet com até 40 eventos cronológicos do sistema (filtros: Todas, Nova, Esgotada, Resgate).
- **Agrupamento no Catálogo Histórico**: Múltiplas sessões de um mesmo espetáculo agrupadas no mesmo card via `buildOfferGroups`, exibindo as datas inline e modal de sessões ao toque.
- **Aba Ingressos com Esgotadas Recentes**: Ofertas que saíram do ar nas últimas 24h aparecem agrupadas no final da lista com opacidade suave (`.card-expired`), mantendo ofertas ativas no topo.
- **Tela de Resgates Filtrada**: Exclusivamente itens de `Campanhas de Ingressos` (21 resgates), thumbnails ampliadas (`74px`) e destaque corrigido para C6 Fest 2026.
- **Tag HOJE com Pulsação Suave**: Pílulas de sessões de hoje renderizadas em verde esmeralda com `@keyframes pulseGreen`.
- **Avatares em Alta Resolução (Catbox CDN)**:
  - Leo: `https://files.catbox.moe/a44poo.jpg`
  - Gabriel: `https://files.catbox.moe/0hj4g2.jpg`
- **Categoria Teatro UOL**: Priorizada como `TEATRO` antes de qualquer verificação musical.
- **Taglines de Tempo**: Redução da fonte, espaçamento entre letras aumentado (`0.35px`) e sufixo `m` (`2h 15m atrás`).

