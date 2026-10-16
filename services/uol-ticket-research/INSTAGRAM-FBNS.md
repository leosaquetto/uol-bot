# Instagram FBNS no servidor — 16/10/2026

Objetivo atual: receber o aviso de Story no Oracle e fazer a consulta dirigida depois do evento. A extensão local não é necessária. O usuário priorizou push e autorizou o possível impacto nas notificações móveis da conta dedicada, inclusive associação como canal principal. Polling periódico de cinco minutos não foi ativado nesta etapa.

## O que a investigação estabeleceu

| Caminho | Evidência | Decisão |
| --- | --- | --- |
| Web Push atual | Registro aceito, cinco DMs reais persistidas/descriptografadas; nenhum pacote de Story observado, inclusive após aviso no celular. | Preservar receptor; conexão saudável não prova Story. |
| MQTT/Lightspeed do Instagram Web | Canal realtime comprovado no JS público. Helpers Relay de Stories observam cache local; nenhuma publicação de Story observada no canal. | Não inventar subscription nem transformar qualquer DM/presença em consulta. |
| FBNS móvel | Implementações publicam `android_mqtt`, recebem `fbpushnotif` e conhecem `first_reel_post` / `resurrected_reel_post`, com ação `user?username=...&launch_reel=1`. | Candidato principal para prova real de push no servidor. Não há prova de aviso para cada Story. |
| HTTP HTML atual | Histórico de extração e três recibos entregues no Oracle. | Recuperação disponível; não ativada como substituta automática de push. |
| HTTP JSON | Rotas verificadas em extensões; primeiro lookup no Oracle respondeu 429. Endpoint de reels ainda não foi testado com sucesso nesse servidor. | Implementado e testado offline; permanece opcional/inativo. |
| Navegador no servidor | Máquina com 977.096 KiB totais e 422.924 KiB disponíveis nesta verificação. | FBNS/HTTP dispensam navegador; não acrescentar Chrome como requisito. |

O aviso no iPhone comprova o canal do celular. Ele não é recibo do receptor Web nem do novo receptor FBNS.

## Autenticação e limites

FBNS usa TLS persistente em `mqtt-mini.facebook.com:443`, autenticação própria de dispositivo e MQTToT. O broker devolve identidade/segredos e token. O POST em `i.instagram.com/api/v1/push/register/` associa o token à conta; a implementação examinada envia `device_type=android_mqtt` e `is_main_push_channel=true`. O usuário autorizou esse efeito sobre a conta dedicada. Não copiar o token APNs do iPhone, substituir a sessão ou executar login.

A sessão Web existente contém os dados para tentar a associação, mas o aceite desse endpoint com essa sessão ainda não está provado. Não usar `login_by_sessionid()`: esse helper dispara consultas adicionais e fallbacks. O instrumento isolado usa somente o cliente HTTP protegido existente, identidade de dispositivo estável e uma intenção durável antes do único POST. Timeout, 5xx ou resposta malformada permanecem incertos; não repetir automaticamente. Reinício não cria dispositivo ou associação novos.

O primeiro GET diagnóstico de `web_profile_info` respondeu HTTP 429. O bloqueio global foi gravado no monitor até **17/10/2026 00:16:34 UTC**, equivalente a **16/10 às 21:16 em São Paulo**. Esse prazo é o piso preventivo local de seis horas, sem `Retry-After` comprovado na resposta. A coleta HTTP de Stories continua bloqueada. A exceção pontual para diagnóstico/registro FBNS está documentada abaixo; nenhum backend, reinício ou push libera a coleta durante o recuo.

## Diagnóstico e aceitação

`leotheblueblur` foi incluído no classificador diagnóstico Web Push. Seus sinais usam o campo `profile` da tabela existente. Todas as leituras de produção continuam filtrando `clubeuol`; nenhum teste vai aos destinos UOL. Não houve migração.

O diagnóstico FBNS também exige tipo e ação estruturados para o perfil de teste. Texto livre, avatar, usuário destinatário, DM, reação, enquete ou indicação genérica de cache não bastam. Eventos são gravados em journal privado antes do ACK. A primeira prova deve mostrar evento natural correspondente ao Story do teste, horário e autor verificáveis. Só então conectar esse formato ao gatilho de produção, fazer a consulta dirigida e conferir os recibos dos destinos. Connect/register/ping sozinhos não encerram esse critério.

O servidor conserva sessão, inscrição Web Push, histórico, filas e recibos. Os controles HTTP preparados permitem backend explícito `html` ou `json`, agenda e orçamento persistidos. JSON reserva um GET com identidade fresca e dois sem cache; captura a identidade junto à reserva para evitar vencer o cache entre reserva e envio. HTML pode reservar até dois GETs por ciclo. O recuo de 429 vale entre backends. Não há fallback automático durante bloqueio.

## Fontes primárias

- [FBNS do instagrapi, commit consultado](https://github.com/subzeroid/instagrapi/blob/1076ce2135878434a678ba4ceba2b17c6fe27f1a/instagrapi/realtime/fbns.py)
- [Autenticação por sessão existente](https://github.com/subzeroid/instagrapi/blob/1076ce2135878434a678ba4ceba2b17c6fe27f1a/instagrapi/mixins/auth.py#L701)
- [Tipos de notificação conhecidos](https://github.com/Nerixyz/instagram_mqtt/blob/f2672ac2c1167859e4fcc3a2d0f6a4660a33250b/src/fbns/fbns.types.ts#L105)
- [Prova upstream e seus limites](https://github.com/subzeroid/instagrapi/pull/2580)
- [API oficial Meta e escopo de contas profissionais](https://www.postman.com/meta/instagram/documentation/6yqw8pt/instagram-api): não estabeleceu acesso aos Stories de um perfil seguido sem autorização desse perfil.

O subconjunto de transporte MIT está fixado por commit em `vendor/instagram_fbns`, com hashes e alterações declaradas em `UPSTREAM.json`. Ele não inicializa o cliente de conta, não executa login nem importa módulos de publicação/DM.

## Ensaio controlado autorizado

O usuário confirmou que esta conta é dedicada e autorizou possível interferência nas notificações do celular. Isso permite o teste com `is_main_push_channel=true`; não comprova que a sessão Web será aceita pelo endpoint móvel. A origem Oracle e o uso de protocolo não oficial podem provocar recusa ou challenge. Não se atribui probabilidade numérica nem se promete ausência de bloqueio.

O serviço diagnóstico espera o recuo antes de conectar ao broker e associar a conta. Limite persistente de três conexões por hora, TLS com certificado verificado, limites de frame e descompressão, identidade e token estáveis. Nenhum login, re-registro após resposta incerta, troca de IP, proxy de contorno ou atestado de hardware inventado. Falha de autenticação/protocolo e capacidade interrompem para revisão; o systemd não cria loop de reinício. HTTP 429 no registro também atualiza o bloqueio compartilhado da coleta.

Uma associação aceita habilita somente observação privada. Ela não encaminha eventos FBNS aos destinos UOL antes da prova de formato, autor e Story natural. O receptor Web continua independente e já reconhece o perfil de teste sem introduzi-lo no intake de produção.

### Exceção pontual autorizada ao recuo

O horário de 21:16 decorreu do piso preventivo de seis horas escolhido por nós, não de um `Retry-After` comprovado na resposta vazia de lookup. O usuário pediu explicitamente avançar sem prejudicar os outros serviços. `--registration-probe` permite somente o ensaio FBNS durante aquele bloqueio de fonte: exige estado `rate_limited/http_429`, grava uma autorização de escopo/prazo antes da conexão e não ignora um bloqueio posterior maior. O limite próprio do registro FBNS permanece obrigatório; 429 recebido nele não é contornado. A coleta HTTP de Stories e os demais serviços mantêm seus limites. A exceção não permite login, troca de IP, novo dispositivo a cada falha nem repetição do POST incerto.


### Diagnóstico do HTTP 400 e correção do contrato

O broker entregou credenciais, token e PONG, mas a associação inicial da conta respondeu HTTP 400. O primeiro corpo de erro não foi preservado; a causa continua não comprovada. O pedido inicial usava User-Agent desktop e cookies. O cliente móvel fixado no commit consultado usa também perfil Android e `Bearer IGT:2`, construído localmente a partir da mesma sessão. Isso não exige senha, novo login, SDK de conta ou atestado de hardware.

`--validate-account` permite uma única leitura autenticada de `accounts/current_user/?edit=true`, com intenção gravada antes da rede. Exige HTTP 200, `status=ok` e identidade igual à sessão. O resultado fica vinculado à conta, sessão, token e contrato; dados pessoais do corpo de sucesso não são persistidos. Falha ou interrupção não autoriza repetir o GET.

`--correct-association` é uma ação explícita de diagnóstico. Exige o registro legado `rejected/http_400`, mesmo token/conta e validação positiva de até cinco minutos. Arquiva o registro anterior e reserva a nova tentativa como incerta antes do POST. Não admite uma segunda correção, nem repetição de resultado aceito, incerto, challenge ou 429. Reiniciar o serviço não executa essa ação. O corpo de erro agora é limitado e preservado somente no journal privado; a saída pública contém estados e motivos sanitizados.

O perfil móvel e Android ID são estáveis e associados ao UUID já salvo. O token, autenticação do broker e sessão original não são substituídos. Isso corrige uma divergência concreta de contrato; somente a resposta real poderá comprovar o aceite.

Fontes do contrato: [headers privados](https://github.com/subzeroid/instagrapi/blob/1076ce2135878434a678ba4ceba2b17c6fe27f1a/instagrapi/mixins/private.py#L256), [Bearer da sessão](https://github.com/subzeroid/instagrapi/blob/1076ce2135878434a678ba4ceba2b17c6fe27f1a/instagrapi/mixins/auth.py#L1586), [consulta de identidade](https://github.com/subzeroid/instagrapi/blob/1076ce2135878434a678ba4ceba2b17c6fe27f1a/instagrapi/mixins/account.py#L56).
