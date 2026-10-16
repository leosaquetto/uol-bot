# Instagram → UOL

Extensão Chrome Manifest V3. Consulta somente os perfis configurados usando a sessão já existente no navegador. O service worker da extensão faz a descoberta; o Worker Cloudflare existente recebe Stories elegíveis, baixa a imagem e entrega aos destinos com seus recibos e deduplicação.

## Instalar

1. Abra `chrome://extensions` no Chrome em que a sessão Instagram já existe.
2. Ative **Modo do desenvolvedor**, escolha **Carregar sem compactação** e selecione a pasta preparada `~/Applications/UOL Instagram`.
3. Abra o popup **Instagram → UOL**. O pacote privado já contém a configuração de `clubeuol`; nenhuma chave precisa ser copiada para a conversa.
4. Use **Verificar agora**. Confira o horário, o resultado de cada perfil e os recibos. A primeira consulta estabelece a referência de notificações; Stories atuais do Clube UOL com campanha válida já podem entrar na fila.

O pacote genérico do repositório não contém credenciais. Configure-o nas opções com o token dedicado `INSTAGRAM_STORY_INGEST_TOKEN`, ou importe um JSON privado com `token`, `profiles`, `intervalMinutes`, `enabled` e `notifications`. O endpoint é fixo. A chave administrativa do Worker não é aceita. Campo de chave vazio preserva a anterior.

O Chrome precisa permanecer aberto. Alarmes podem atrasar durante suspensão; não existe garantia de latência exata de cinco minutos. Não reinicie nem troque sessões para instalar. Se a sessão não estiver disponível, o monitor pausa e mostra o problema.

## Limites e comportamento

- Até três perfis; padrão `clubeuol`. Outros perfis, inclusive `leotheblueblur`, servem apenas para diagnóstico e alertas locais, sem envio aos destinos UOL.
- Consultas a cada 5, 10, 15, 30 ou 60 minutos; máximo global de 300 requisições Instagram por dia em São Paulo. A cobrança local ocorre antes do GET e persiste após reinício. **Verificar agora** respeita intervalo e orçamento.
- Identificação de perfil em cache por 24 horas; consulta de Stories em lote. Falhas fazem recuo progressivo; HTTP 429 espera pelo menos seis horas e respeita `Retry-After`. Login/challenge pausa até retomada explícita.
- Cookies são usados pelo navegador via `credentials: include`. Não há permissão `cookies`, leitura/exportação de sessão, content script, código remoto ou ponte de requisições arbitrárias. Só Instagram e o domínio fixo do Worker têm permissão de rede.
- Somente dono verificado `clubeuol`, Story válido, imagem CDN e sticker com campanha `clube.uol.com.br/campanhasdeingresso/...` produzem intake. Não extrai links de texto livre. IDs grandes preservam a string original. Imagens com proporção conhecida incompatível são rejeitadas.
- A fila é gravada antes do POST. Resposta perdida ou interrupção vira `uncertain`; a recuperação consulta o recibo da mesma identidade. Ausência de recibo mantém a pendência, sem novo POST automático. Tentativas seguras já aceitas reutilizam a identidade e os horários do Worker.
- Apenas confirmação dos quatro destinos com ID e imagem permite marcar `delivered`. Pendências não são removidas pela retenção; fila cheia interrompe descoberta. O Worker preserva recibos incompletos mesmo após expiração.
- A extensão não altera o heartbeat do coletor Oracle, que continua `push_only`. Seu estado local e o estado do coletor antigo são independentes.

## Validação

```sh
npm test --prefix extensions/uol-instagram
```

Testes usam rede simulada. Eles não comprovam uma sessão autenticada, instalação no Chrome ou entrega real. A aceitação operacional exige: extensão carregada → consulta válida → Story de `clubeuol` com campanha → intake → recibos dos quatro destinos. Um alerta local de outro perfil prova apenas descoberta desse perfil.

## Referências verificadas em 16/10/2026

Inspeção estática de pacotes públicos obtidos do serviço oficial de atualização do Google; nenhum pacote de terceiros foi instalado ou executado, e nenhum código foi copiado.

| Extensão | Versão | Mecanismo observado |
| --- | --- | --- |
| [Vivies](https://chromewebstore.google.com/detail/vivies-story-viewer-for-i/lglkighadkolikpgnmadpganmeeemjga) | 1.0.1 | Alarme a cada 3 minutos; GET em lote; contador e avisos internos, sem permissão de notificação nativa. |
| [Anonymous Story Viewer](https://chromewebstore.google.com/detail/anonymous-story-viewer/peplbgpalimccdhgjhcjmclikckonddj) | 2.2.4 | Alarme de 4 minutos com intervalo efetivo variável de 5–12 minutos; GET em lote; contador; sem permissão de notificação nativa. |
| Instagram Favorites | — | Página/pacote indisponíveis na consulta; mecanismo não comprovado. |

Ambas usam `users/web_profile_info` para identificação e `feed/reels_media` para Stories. Essas interfaces não têm contrato público estável; mudanças viram estado não confirmado, nunca uma falsa ausência de Stories.

Documentação Chrome: [requisições em extensões](https://developer.chrome.com/docs/extensions/develop/concepts/network-requests), [alarmes](https://developer.chrome.com/docs/extensions/reference/api/alarms), [ciclo do service worker](https://developer.chrome.com/docs/extensions/develop/concepts/service-workers/lifecycle).

## Investigação realtime

Em 16/10/2026, o JavaScript público servido por [instagram.com](https://www.instagram.com/) contém `wss://edge-chat.instagram.com/chat` e configuração de `gateway.instagram.com`. Porém `subscribeToPolarisRelayStoriesTray` / `subscribeToPolarisRelayViewerStories` chamam `buildSubscriptionForQuery`, que usa `environment.lookup` e `environment.subscribe`: observação do cache Relay. Isso não demonstra subscription de rede para novo Story.

A hipótese de eventos silenciosos permanece aberta. É necessário observar um frame natural correlacionado com publicação, autor e ID, excluindo DM, resposta/visualização de Story e simples mudança de cache. A extensão não injeta um interceptador especulativo nem consulta Stories a cada evento genérico. A API [chrome.webRequest](https://developer.chrome.com/docs/extensions/reference/api/webRequest) observa apenas o handshake WebSocket, não suas mensagens. Um eventual observador no contexto da página exigiria que Instagram permanecesse aberto e poderia não alcançar conexões criadas em SharedWorker.
