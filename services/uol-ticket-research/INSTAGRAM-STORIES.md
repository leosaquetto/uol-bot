# Stories do Clube UOL: estudo para coleta automática em servidor

Verificação: 09/10/2026. Horários abaixo em America/Sao_Paulo.

Continuação autorizada: [monitor integrado ao bot no Oracle](INSTAGRAM-MONITOR.md), com envio de imagem e link aos quatro destinos existentes. Este arquivo preserva as provas do estudo inicial; o estado operacional posterior está no documento do monitor.

**Resultado:** a sessão dedicada conseguiu ler os Stories e os destinos dos stickers tanto no navegador quanto por HTTP direto no Oracle, sem Chrome ou Mac na execução remota. A entrada genérica `/stories/clubeuol/` devolveu os dois Stories atuais em 1,06 s, com IDs, horários e links, após um redirecionamento da própria página. Transporte e extração no servidor estão comprovados em uma prova limitada; estabilidade da sessão e operação contínua ainda não. Nenhum monitor de Instagram foi ativado.

O objetivo é um processo de servidor, com detecção em poucos minutos e sem participação do Codex ou do Mac em cada consulta. Acompanhamento manual e heartbeat local não atendem a esse objetivo.

## Evidência observada

| Story | Publicação | Expiração do Story | Destino do sticker | Classificação |
| --- | --- | --- | --- | --- |
| `4004185955500703427` | 09/10/2026 11:29:43 | 10/10/2026 11:29:43 | `campanhasdeingresso/pPM-2-ingressos-bgs-distrito-anhembi-sp` | Campanha de ingressos BGS |
| `4004282829240101142` | 09/10/2026 14:42:10 | 10/10/2026 14:42:10 | `fotoregistro/pO8-ms-das-crianas-1-quebra-cabea-grtis` | Outro benefício; fora do alerta de ingressos |

O sticker da BGS foi verificado tanto no diálogo visual quanto no objeto estruturado do mesmo Story. URL limpa: <https://clube.uol.com.br/campanhasdeingresso/pPM-2-ingressos-bgs-distrito-anhembi-sp>.

A coleta UOL feita antes deste estudo já havia encontrado `pPM` fora dos quatro catálogos monitorados. A página informava validade de **07/10/2026 17:13 a 09/10/2026 17:30**. O Story foi publicado cerca de seis horas antes do fim do benefício e expiraria só no dia seguinte. Portanto, Story ainda disponível não significa benefício vigente; a validade também não informa quando a campanha foi cadastrada. Estoque continuou não confirmado.

Provas locais sanitizadas, sem cookies, senha, tokens ou HTML bruto:

- `~/.local/share/uol-ticket-research/proofs/instagram-bgs.json`: leitura do sticker com verificação do ID antes/depois.
- `~/.local/share/uol-ticket-research/proofs/instagram-bgs.png`: prova visual.
- `~/.local/share/uol-ticket-research/proofs/instagram-structured.json`: dois Stories da resposta da página específica.
- `~/.local/share/uol-ticket-research/proofs/instagram-generic-entry-confirmed.json`: mesmos dois Stories pela entrada genérica, observados às 16:18:28, sem clicar em cada publicação.
- `instagram-generic-entry.json` preserva uma primeira extração vazia/inconclusiva. Uma leitura vazia ou com estrutura diferente não pode ser classificada como “nenhum Story”.

## Prova HTTP no Oracle — 09/10/2026

A conexão existente foi recuperada do histórico do projeto. Coordenadas de SSH ficaram em `~/.config/uol-ticket-research/server.json` (0600), fora do repositório, sem chave privada ou cookies. O servidor tinha Python 3.12.3, cerca de 954 MiB de RAM total e 260 MiB disponíveis; Drops e gateway estavam ativos na verificação inicial. Não foi iniciado navegador nem instalado daemon.

O usuário autorizou o uso da sessão dedicada no Oracle. Os cookies passaram por SSH e foram usados apenas em memória no processo remoto; os arquivos temporários protegidos do Mac foram removidos ao terminar. Não foram publicados valores de cookies, cabeçalhos de autenticação ou HTML bruto.

| Prova | Resultado | Tempo |
| --- | --- | --- |
| Mac, HTTP anônimo | HTTP 200 sem estrutura validada; inconclusivo | 1,07 s |
| Oracle, HTTP anônimo | HTTP 302 para login | 0,32 s |
| Oracle, sessão e cabeçalhos mínimos | HTTP 200 sem Stories no payload | 0,69 s |
| Mac, mesma sessão mínima | Mesma estrutura incompleta | 1,04 s |
| Oracle, diagnóstico com sessão mínima | Mesma estrutura incompleta | 0,60 s |
| Oracle, cookies e cabeçalhos observados na navegação | HTTP 302 para a própria página | 0,31 s |
| Oracle, mesmos dados e seguindo esse retorno uma vez | **HTTP 200, dois Stories e destinos corretos** | **1,06 s; duas requisições** |

A prova positiva ocorreu às **16:39:26**. O processo teve pico de **25,2 MiB de RAM** e leu aproximadamente 543 kB na resposta final. IDs e destinos coincidiram exatamente com a prova anterior do navegador. Apenas BGS (`pPM`) pertence a campanhas de ingresso; FotoRegistro (`pO8`) permanece fora desse alerta.

O retorno fornecido pelo Instagram manteve HTTPS, hostname `www.instagram.com` e caminho `/stories/clubeuol/`, acrescentando o parâmetro `r`. O teste seguiu exclusivamente essa URL observada, sem inventar o valor do parâmetro. Permitido no máximo um redirecionamento para essa mesma página; retorno de login, outro domínio, outro caminho ou segundo redirecionamento não é seguido. Os valores da query não foram registrados nos relatórios.

A comparação mostra que uma requisição mínima pode devolver somente a estrutura inicial do aplicativo, mesmo com HTTP 200. Não há evidência para atribuir essa resposta apenas ao IP do Oracle: a mesma forma de requisição no Mac também não trouxe Stories. A combinação testada de sessão, cabeçalhos de navegação e tratamento do retorno funcionou; quais cabeçalhos individuais são necessários não foi isolado.

Novas provas em `~/.local/share/uol-ticket-research/proofs/`:

- `instagram-server-preflight.json`: runtime e memória, sem endereço do servidor.
- `instagram-http-local-anonymous.json`, `instagram-http-server-anonymous.json`: controles sem sessão.
- `instagram-http-server-authenticated.json`, `instagram-http-local-authenticated.json`, `instagram-http-server-diagnostic.json`: respostas incompletas, preservadas como inconclusivas.
- `instagram-http-server-matched-browser.json`: redirecionamento observado.
- `instagram-http-server-redirect-follow.json`: prova positiva e destinos sanitizados.
- `instagram-http-probe-2026-10-09.py`: código exato da prova, biblioteca padrão, sem sessão ou host privado incorporados. É um instrumento de teste; não é monitor de produção.

Essa prova confirma extração remota rápida com uma sessão já autenticada. Não confirma login autônomo, persistência após reinício, renovação de sessão, chegada de um Story novo, entrega de alertas nem funcionamento por 72 horas.

## Como o site entrega os links

1. Entrar uma vez na conta dedicada. Nesta sessão, o Instagram exigiu CAPTCHA, concluído pelo usuário. A sessão foi posteriormente reutilizada com sucesso no Oracle; login autônomo em servidor não foi comprovado.
2. Abrir <https://www.instagram.com/stories/clubeuol/>. Esse caminho foi observado ao clicar na foto do perfil e depois validado por navegação direta.
3. Usar os dados de navegação observados e tratar o redirecionamento fornecido para a própria página, dentro do limite de duas requisições. Ler o corpo da resposta HTML autenticada. Nos blocos `script[type="application/json"]`, localizar a estrutura de Stories do perfil. Não executar esses scripts. HTTP 200 sem o payload esperado permanece inconclusivo.
4. O payload observado contém `data.xdt_api__v1__feed__reels_media.reels_media`. Os wrappers `__bbox` e `require` são detalhes internos e podem mudar. O parser deve reconhecer uma estrutura validada, não fixar índices de arrays.
5. Selecionar exclusivamente o reel cujo `user.username` é `clubeuol`. Dentro de `items`, cada Story contém `pk`, `taken_at`, `expiring_at` e `story_link_stickers[].story_link.url`. Manter IDs como strings: excedem a precisão inteira segura de JavaScript.
6. O link visual passa por `l.instagram.com`; o campo estruturado permite obter o destino. Normalizar e validar a URL antes de qualquer consulta UOL. Remover somente parâmetros conhecidos de rastreamento em rotas públicas reconhecidas; destinos desconhecidos ficam para revisão.

Isso é uma estrutura interna do site autenticado, **não uma API pública com contrato ou quota documentados**. Não foi comprovado neste estudo que uma API oficial permita consultar Stories de terceiros com essa conta. Não depender de IDs de operações GraphQL inventados, enumeração de IDs do Instagram ou serviços de contorno de CAPTCHA.

O caminho visual também funciona: `Ver story` → `Pausar` → sticker → `Acessar link`. É mais frágil: no painel estreito, o controle de pausa não apareceu e houve avanço para o Story seguinte. Na janela ampla, a pausa funcionou. Preferir os objetos estruturados para associar ID, horário e link; não extrair um link do DOM e atribuí-lo ao ID anterior sem verificar a renderização.

## Fluxo de servidor proposto

```mermaid
flowchart LR
    A[Coletor autenticado Instagram] --> B[Stories do perfil validado]
    B --> C[Filtrar links de campanhas de ingresso]
    C --> D[Consultar página pública UOL]
    D --> E[Histórico e deduplicação]
    E --> F[Fila de notificações]
    F --> G[ntfy]
```

- **Coletor:** serviço independente, sessão persistente e conta dedicada. O GET autenticado da página genérica já foi comprovado no Oracle, com o retorno fornecido pelo site e sem navegador por rodada. Implementar o transporte testado antes de considerar navegador persistente. A sessão aceita nesta prova não garante aceitação de outra sessão ou renovação futura.
- **Classificação:** hostname exato `clube.uol.com.br`, HTTPS e rota pública `/campanhasdeingresso/<codigo>-<slug>`. Case dos códigos preservado. Sorteios, descontos, outros benefícios e URLs sem destino verificável não viram alertas de resgate.
- **Verificação UOL:** GET da oferta concreta, identidade/canonical, título, descrição, validade e estado público. Nenhum endpoint de resgate, login UOL ou botão “Utilizar este benefício”. Página presente não prova estoque.
- **Histórico:** guardar perfil, Story ID, publicação, expiração, primeira/última observação, URL canônica, código, validade UOL, última evidência de listagem e classificação. Preservar `first_seen_public`, `first_listed` e `instagram_published_at` separadamente. A listagem diária pode estar defasada; exibir sua data, sem afirmar “não listado agora”.
- **Deduplicação:** chave de coleta `(perfil, story_id, URL canônica)`; chave de alerta por campanha e mudança relevante. Um Story republicado não gera automaticamente outro alerta da mesma oferta. Uma URL alterada em Story já conhecido precisa gerar atualização.
- **Entrega:** outbox persistente com confirmação HTTP/recibo, deduplicação e retentativas independentes da coleta. Falha no ntfy não repete consultas nem resgates. Futuro destino pretendido: o tópico ntfy já usado no projeto; nenhuma mensagem foi enviada neste estudo.
- **Resgate:** integração futura e separada. A sentinela continua pausada. Descobrir uma oferta no Instagram não autoriza resgatá-la nem reduz as travas mensais ou a exigência de artista/data/local confirmados no conteúdo UOL.

## Frequência, limites e falhas

Proposta inicial para teste contínuo: uma coleta a cada **120 segundos**, com variação pequena e uma única coleta em andamento. São até 720 ciclos/dia; o transporte comprovado consumiu duas requisições por ciclo, o que corresponderia a até 1.440 GETs/dia. Navegador não foi necessário para a leitura remota. Medir tráfego e respostas reais durante o teste, incluindo falhas e renovações. Não há quota segura do Instagram comprovada para esse transporte e essa conta; não copiar os limites conhecidos do Clube UOL.

Meta de latência, ainda não medida: alerta em até três minutos após a publicação durante operação saudável. O tempo inclui a próxima coleta, disponibilidade do Story para a conta, leitura UOL e entrega. Só reduzir para 60 segundos após evidência de estabilidade e consumo aceitável. Polling não garante alcançar o estoque antes de outras pessoas.

| Condição | Comportamento esperado |
| --- | --- |
| Mesmos IDs e links | Atualizar observação, sem novo alerta |
| Lista vazia com estrutura válida e evidência explícita | Registrar ausência de Stories nesse instante |
| HTTP 200 com login, HTML incompleto ou formato desconhecido | Estado inconclusivo; preservar histórico |
| Timeout/5xx | Backoff limitado; sem consultas sobrepostas |
| 429 | Respeitar `Retry-After`, suspender ciclos; sem troca de contas ou IPs para contorno |
| 401/403, CAPTCHA, desafio ou sessão expirada | `auth_required`/bloqueado; parar repetição de login e sinalizar falha uma vez |
| Reinício | Retomar sessão/estado persistidos e fila, sem duplicar alertas |

Uma conta dedicada reduz o impacto sobre a conta principal, mas não elimina desafios de autenticação. Este login já exigiu intervenção humana. Não é possível prometer ausência definitiva de CAPTCHA ou renovação manual apenas com a prova local. Se a operação exigir intervenção frequente, não atende ao requisito do usuário e não deve ser tratada como solução pronta.

## Reaproveitamento do projeto

Evidência documental local complementada pela verificação inicial e pela prova HTTP no Oracle:

- [Saquetto Drops](../saquetto-drops/README.md): Oracle com Node, systemd e SQLite já faz parte do projeto. O teste anterior de Chrome/CDP ficou parado por capacidade. A leitura HTTP comprovada agora usou Python existente e cerca de 25 MiB; não precisou desse navegador. A memória disponível verificada é uma fotografia, não garantia de capacidade futura.
- [Worker de descoberta](../../cloudflare-workers/uol-telegram-shadow-worker/README.md): reutilizar o formato de oferta, deduplicação e fila existentes; ingresso descoberto por sticker pode ser outra origem. Não abrir um segundo caminho de envio que duplique os alertas.
- [Sentinela](../../cloudflare-workers/uol-redemption-sentinel/README.md): aproveitar o padrão de outbox/ntfy e isolamento das notificações, mantendo a capacidade de resgate desabilitada neste coletor.
- [Pesquisa diária](README.md): histórico de códigos, validade e listagem complementa o Instagram. A rotina de 18h criada na etapa anterior é local ao Codex e depende do Mac; não atende ao requisito de coleta rápida em servidor. Uma futura implantação deve levar esse trabalho diário ao runtime do servidor e evitar duas varreduras concorrentes.

Não criar novo serviço Cloudflare, cron ou dependência de navegador até comprovar o transporte e conferir capacidade/limites reais do ambiente escolhido.

## Critérios para uma futura implementação ser aceita

1. **Comprovado na prova limitada:** reproduzir no servidor a lista atual e os destinos, partindo apenas de `clubeuol`, sem ID previamente informado e sem navegador no servidor. Revalidar na implementação definitiva.
2. Distinguir perfil errado, login/desafio, payload incompleto e ausência real de Stories; mudança de formato deve falhar sem alertar ofertas erradas.
3. Confirmar um Story novo real durante o teste. Os dois Stories atuais provam extração, não detecção futura.
4. Testar por pelo menos 72 horas, incluindo reinício, deduplicação, falha de rede e retomada de sessão. Medir CPU/memória/tráfego, falhas e atraso real. Sessenta ou cento e vinte segundos são propostas, não intervalos validados.
5. Validar outbox com falha de entrega simulada e, quando autorizado, entrega real ao destino. Não enviar alertas de teste para grupos por padrão.
6. Nenhum resgate, credencial em log/repositório, sessão exportada em artefatos, OCR ou alteração na sentinela pausada.

**Estado final deste estudo:** extração estruturada e descoberta pela entrada genérica comprovadas no navegador e por HTTP no Oracle. O transporte levou 1,06 s e cerca de 25 MiB, sem navegador na execução remota. O [ensaio controlado de 72 horas](INSTAGRAM-TRIAL.md) foi implementado e validado com fixtures. Sua ativação está pendente: o navegador integrado não respondeu e a tentativa isolada terminou por timeout, sem exportar sessão. Monitor contínuo, durabilidade da sessão, chegada de um Story novo e entrega permanecem sem validação real. Nenhum monitor foi ativado.
