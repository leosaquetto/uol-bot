# Instagram Web Push no Oracle

## Operação atual — somente push, 15/10/2026

Por decisão explícita do usuário após o aviso de comportamento automatizado,
o monitor usa `pushMode: "push_only"`. Não há consulta periódica, nem a varredura
de segurança de trinta minutos. Inicialização, reinício, perda do socket e término
do backoff não iniciam uma coleta sem sinal pendente do perfil monitorado.
`activePollPeriodSeconds: 0` identifica essa ausência de polling.

Um sinal válido continua disparando a coleta HTTP existente, com reserva durável,
agrupamento, orçamento, backoff e deduplicação. A fila de entrega e os recibos
permanecem ativos. Sinal recebido durante uma coleta permanece pendente.

Cada lote conserva seu limite de quatro reservas e sua janela de dez minutos,
inclusive após crash/reinício. Respostas vazias ou com Stories anteriores não
concluem um sinal exato: os IDs esperados precisam aparecer em snapshots válidos.
Tentativas adicionais esperam pelo menos 30, 60 e 120 segundos; sinais genéricos
exigem dois snapshots separados por pelo menos sessenta segundos, com evidência
de Story novo, e continuam como correlação incerta. Essas tentativas pertencem
ao sinal recebido; não criam varredura periódica. Autenticação, 429 e orçamento
continuam impondo seus backoffs, mesmo que o lote expire enquanto aguarda.

Ao terminar sem confirmação, somente o cursor daquele lote avança, com resolução
`unresolved` explícita. O resumo `pushSettlement` guarda contagens, motivos e
horários, sem IDs. Sinal recebido depois do lote permanece pendente e não estende
seus limites. O supervisor avisa o esgotamento no ntfy particular; silêncio ou
conexão ativa não comprovam recuperação.

Essa escolha não comprova Web Push: a prova real continua pendente e não é
preenchida artificialmente. Sem push recebido, nenhum Story será descoberto
por este complemento. Sem descoberta independente, também não é possível medir
os Stories perdidos pelo push. UOL, Drops e os quatro destinos continuam separados.

O receptor é separado do X, do navegador e dos envios. A biblioteca criptográfica genérica de `saquetto-drops/src/webpush-crypto.js` é reutilizada sem importar o adaptador X ou alterar sua inscrição. Node 22.13 ou superior, SQLite nativo e a dependência `http_ece` já usada pelo Drops são necessários. Na instalação, conservar a relação de caminhos `/opt/uol-instagram-monitor/push` e `/opt/saquetto-drops/src`, com leitura somente do código/dependências pelo usuário do monitor.

No Oracle, o runtime compatível é `/usr/local/bin/node` (22.23.2). Usar esse caminho na unidade e nos comandos abaixo: `/usr/bin/node` ainda é 18 e não fornece WebSocket global nem SQLite nativo.

## Ativação em duas etapas

1. Registrar um canal Mozilla próprio com a chave VAPID **pública efetivamente usada pelo Instagram nesta conta**. Nenhum valor de chave, cookie, endpoint ou nonce aparece no stdout. Criar o arquivo privado `push-bootstrap.json` com o único campo `applicationServerKey` (65 bytes em base64url), dentro do diretório 0700 do monitor, com modo 0600.
2. Executar `node push/bootstrap.mjs /var/lib/uol-instagram-monitor/push-bootstrap.json /var/lib/uol-instagram-monitor/push-registration.json` como usuário do monitor. O script persiste UAID, canal e chaves antes de criar o canal. Não substitui arquivo existente; uma criação incompleta exige revisão do checkpoint privado.
3. Preparar `push-register-fields.json` 0600 com os campos CSRF observados na sessão autenticada: `fb_dtsg` e `jazoest`. `appId` é opcional e só deve vir do pedido observado. O `csrftoken`, `mid` e `sessionid` vêm do cookie jar privado de `session.json`; não pedir nova senha. Nonces antigos podem ser rejeitados e não são inventados pelo script.
4. Executar `node push/register.mjs /var/lib/uol-instagram-monitor/push-registration.json /var/lib/uol-instagram-monitor/session.json /var/lib/uol-instagram-monitor/push-register-fields.json`. Faz uma única tentativa `POST /api/v1/web/push/register/` com `device_type=web_vapid`, endpoint Mozilla e `subscription_keys={p256dh,auth}`. Registrar `accepted` exige HTTP 200 e resposta JSON reconhecida. Timeout é `uncertain`, sem repetição automática. Apagar o arquivo transitório dos nonces depois da tentativa.
5. Instalar e iniciar `deploy/uol-instagram-push.service`; manter o monitor Python existente. O serviço não roda navegador ou realiza login. A UAID retornada deve continuar a mesma em toda reconexão; mudança encerra a recepção para revisão, sem criar outra inscrição.

Registrar com sucesso não comprova que o Instagram envie Stories a uma inscrição Mozilla. No ensaio original, `pushMode: "pilot"` conservava o polling enquanto faltasse essa prova. O modo padrão `poll` consulta a cada 120–130 segundos; `pilot` acrescenta gatilhos de push. Nenhum deles é a configuração atual `push_only`.

## Identidade, persistência e coleta

`push.sqlite` é 0600, WAL e `synchronous=FULL`; armazenamento vive no diretório 0700 do usuário. Cada pacote criptografado, associado ao canal e à versão, é comprometido antes do ACK Mozilla. Reconexão/replay não duplica sinal; mesma versão com ciphertext diferente é recusada. Pacotes pendentes após crash são processados ao reiniciar. Limites: 5.000 pendentes, 32 MiB de payloads e 100.000 identidades. Concluídos conservam ciphertext por sete dias; depois, ou antecipadamente sob pressão, lotes atômicos de 50 compactam apenas payloads, preservando hashes/sinais. Pressão recuperável pausa a conexão sem ACK e continua compactando antes de reconectar. Limites de pendências/identidades exigem revisão. Os pacotes e o arquivo de inscrição são privados, nunca publicados nos status ou logs. Política completa em [CONTINUITY.md](CONTINUITY.md).

Um sinal só nasce de identidade estruturada (`type` de Story com `username=clubeuol`) ou URI autenticada de `instagram.com/stories/clubeuol/`. Título/prosa, URI de outro perfil, mensagens privadas e destinos externos não bastam. Um formato real desconhecido fica registrado como ignorado para revisão privada; não se amplia o classificador a partir de suposição.

O Python consulta somente sequência/identidade e saúde, nunca o corpo da notificação. Reserva o orçamento e a tentativa do lote antes dos GETs; coleta os Stories atuais usando o parser existente. Sinais próximos são agrupados por sequência. Em `push_only`, snapshots reconhecidos preservam a fila, mas o cursor só avança após a resolução do lote descrita acima. Os demais modos conservam seu consumo após snapshot reconhecido. Falha de autenticação, 429 ou orçamento impede o gatilho de furar o backoff; crash não zera o limite nem perde sinais recebidos depois do lote.

## Diagnóstico de recepção

`receiver_diagnostics` registra somente chaves de enum fechado, contagens e os
horários da primeira/última ocorrência. Há contadores para conexão, frames,
rejeições por etapa, persistência/replay, classificação/descriptografia e ACK.
Eles começam na instalação desta versão, sem reconstruir o histórico anterior.
`frames_received` inclui handshake e pong; não equivale a notificações recebidas.
`ack_sent` significa que `send()` local retornou, sem comprovar recebimento remoto.

Descartes anteriores à persistência agora deixam contadores duráveis, mesmo após
reconectar. Persistência/replay/classificação são contados junto à transação do
evento. Falha de armazenamento não permite ACK de evento não persistido. Não há
plaintext, IDs, URLs ou conteúdo da notificação nessa tabela. Eventos ignorados
continuam preservados e não são reenviados/reprocessados automaticamente.

O supervisor lê esses agregados sem consultar Instagram. Novas rejeições recentes
abrem um único incidente no ntfy particular; ausência de notificações não o fecha.
Recuperação exige evidência posterior de notificação interpretada, com conexão
recente. Isso não substitui a prova de um Story real entregue aos quatro destinos.

Até dois GETs por coleta, máximo de 1.440 GETs reservados em qualquer janela de 24 horas, e no máximo vinte buscas de mídia permanecem iguais. Links/imagem e recibos continuam na esteira atual dos quatro destinos; não há novo resgate, OCR ou reenvio de destino confirmado.

## Prova e redução de consultas

Para ativar `pushMode: "event"`, adicionar `pushProof` ao arquivo de configuração privado:

```json
{
  "signalSequence": 1,
  "storyId": "ID_REAL_DO_STORY",
  "browserClosed": true,
  "observedAt": "DATA_ISO_DA_PROVA"
}
```

Os números/dados acima são exemplos de estrutura, não prova de produção. Preencher somente após recepção **real no Oracle com navegador fechado**. O código exige sequência do perfil certo com ID exato do Story, prova UTC gravada até dez minutos após o sinal, sem data futura, e Story num snapshot bem-sucedido; a confirmação é persistida para sobreviver à expiração do Story e reinício. Sinal genérico pode disparar coleta, mas não promove o modo sozinho. Inscrição HTTP 200, evento simulado ou processo ativo não servem de comprovação.

Após essa prova, a coleta passa a push imediato mais consulta de segurança a cada 1.800 segundos (48 ciclos, até 96 GETs reservados/dia sem eventos). O laço verifica sinais a cada dois segundos; novas rajadas esperam pelo menos quinze segundos entre tentativas. Se o socket cair, permanece a consulta de segurança de trinta minutos; não volta a polling de dois minutos. `pushReceiverStatus`, `activePollPeriodSeconds`, `lastPollTrigger` e sequências constam do status local sem dados privados. O receptor reconecta automaticamente com espera crescente e mesma UAID.

A sessão continua sendo atualizada pelo cookie jar do coletor. Um desafio de login interrompe consultas por backoff; o receptor não contorna CAPTCHA, troca contas ou promete renovação ilimitada. O ensaio de 72 horas não comprovou Story push real. A automação complementar do Codex foi pausada pelo usuário; os serviços Oracle continuam ativos. A decisão posterior por `push_only` preserva essa limitação e não autoriza promover `event` sem prova.

## Validação e rollback

Testes focados: `python3 -B -m unittest test_monitor.py` e `node --test push/test.mjs`. Cobrem commits antes de ACK, replay/reinício, criptografia, identidade, reconexão, inscrição sem substituição, resposta ambígua, consumo de sequência, backoff, gate e segurança de trinta minutos em perda de socket. Não enviam mensagens reais nem fazem resgates.

Rollback: preservar `push.sqlite`, `monitor.sqlite`, inscrição, sessões e recibos; trocar somente código/configuração compatíveis. Retirar `pushMode` ou restaurar `pilot` reativa polling e exige nova decisão do usuário. Se a versão antiga não suportar `push_only` e os limites do lote durável, manter o monitor parado até uma versão compatível, com o receptor push preservado. Não restaurar banco antigo sobre eventos novos, cancelar a inscrição do X ou substituir cookies/inscrição do navegador.
