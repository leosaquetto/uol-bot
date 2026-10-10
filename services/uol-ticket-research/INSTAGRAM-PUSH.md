# Instagram Web Push no Oracle

O receptor é separado do X, do navegador e dos envios. A biblioteca criptográfica genérica de `saquetto-drops/src/webpush-crypto.js` é reutilizada sem importar o adaptador X ou alterar sua inscrição. Node 22.13 ou superior, SQLite nativo e a dependência `http_ece` já usada pelo Drops são necessários. Na instalação, conservar a relação de caminhos `/opt/uol-instagram-monitor/push` e `/opt/saquetto-drops/src`, com leitura somente do código/dependências pelo usuário do monitor.

## Ativação em duas etapas

1. Registrar um canal Mozilla próprio com a chave VAPID **pública efetivamente usada pelo Instagram nesta conta**. Nenhum valor de chave, cookie, endpoint ou nonce aparece no stdout. Criar o arquivo privado `push-bootstrap.json` com o único campo `applicationServerKey` (65 bytes em base64url), dentro do diretório 0700 do monitor, com modo 0600.
2. Executar `node push/bootstrap.mjs /var/lib/uol-instagram-monitor/push-bootstrap.json /var/lib/uol-instagram-monitor/push-registration.json` como usuário do monitor. O script persiste UAID, canal e chaves antes de criar o canal. Não substitui arquivo existente; uma criação incompleta exige revisão do checkpoint privado.
3. Preparar `push-register-fields.json` 0600 com os campos CSRF observados na sessão autenticada: `fb_dtsg` e `jazoest`. `appId` é opcional e só deve vir do pedido observado. O `csrftoken`, `mid` e `sessionid` vêm do cookie jar privado de `session.json`; não pedir nova senha. Nonces antigos podem ser rejeitados e não são inventados pelo script.
4. Executar `node push/register.mjs /var/lib/uol-instagram-monitor/push-registration.json /var/lib/uol-instagram-monitor/session.json /var/lib/uol-instagram-monitor/push-register-fields.json`. Faz uma única tentativa `POST /api/v1/web/push/register/` com `device_type=web_vapid`, endpoint Mozilla e `subscription_keys={p256dh,auth}`. Registrar `accepted` exige HTTP 200 e resposta JSON reconhecida. Timeout é `uncertain`, sem repetição automática. Apagar o arquivo transitório dos nonces depois da tentativa.
5. Instalar e iniciar `deploy/uol-instagram-push.service`; manter o monitor Python existente. O serviço não roda navegador ou realiza login. A UAID retornada deve continuar a mesma em toda reconexão; mudança encerra a recepção para revisão, sem criar outra inscrição.

Registrar com sucesso não comprova que o Instagram envie Stories a uma inscrição Mozilla. Enquanto essa prova faltar, `config.json` permanece com `pushMode: "pilot"` ou sem esse campo. O modo padrão é `poll`: comportamento atual de 120–130 segundos. `pilot` adiciona gatilhos de push e conserva esse intervalo.

## Identidade, persistência e coleta

`push.sqlite` é 0600, WAL e `synchronous=FULL`; armazenamento vive no diretório 0700 do usuário. Cada pacote criptografado, associado ao canal e à versão, é comprometido antes do ACK Mozilla. Reconexão/replay não duplica sinal; mesma versão com ciphertext diferente é recusada. Pacotes pendentes após crash são processados ao reiniciar. Um limite de 5.000 pacotes impede crescimento ilimitado: ao alcançar, o receptor para sem ACK e exige revisão, preservando o histórico de deduplicação. Os pacotes e o arquivo de inscrição são privados, nunca publicados nos status ou logs.

Um sinal só nasce de identidade estruturada (`type` de Story com `username=clubeuol`) ou URI autenticada de `instagram.com/stories/clubeuol/`. Título/prosa, URI de outro perfil, mensagens privadas e destinos externos não bastam. Um formato real desconhecido fica registrado como ignorado para revisão privada; não se amplia o classificador a partir de suposição.

O Python consulta somente sequência/identidade e saúde, nunca o corpo da notificação. Reserva o orçamento antes dos GETs; coleta os Stories atuais usando o parser existente. Sinais próximos são agrupados por sequência. Após snapshot reconhecido, persiste fila e sequência consumida na mesma transação. Um sinal recebido durante a coleta fica pendente. Falha de autenticação, 429 ou orçamento impede o gatilho de furar o backoff; crash não perde o sinal.

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

Os números/dados acima são exemplos de estrutura, não prova de produção. Preencher somente após recepção **real no Oracle com navegador fechado**. O código exige que a sequência exista na tabela de sinais do perfil certo e que o Story esteja num snapshot bem-sucedido; a confirmação é persistida para sobreviver à expiração do Story e reinício. Inscrição HTTP 200, evento simulado ou processo ativo não servem de comprovação.

Após essa prova, a coleta passa a push imediato mais consulta de segurança a cada 1.800 segundos (48 ciclos, até 96 GETs reservados/dia sem eventos). O laço verifica sinais a cada dois segundos; novas rajadas esperam pelo menos quinze segundos entre tentativas. Se o socket cair, permanece a consulta de segurança de trinta minutos; não volta a polling de dois minutos. `pushReceiverStatus`, `activePollPeriodSeconds`, `lastPollTrigger` e sequências constam do status local sem dados privados. O receptor reconecta automaticamente com espera crescente e mesma UAID.

A sessão continua sendo atualizada pelo cookie jar do coletor. Um desafio de login interrompe consultas por backoff; o receptor não contorna CAPTCHA, troca contas ou promete renovação ilimitada. A prova de detecção nova e renovação/expiração será acompanhada durante 72 horas, mantendo os serviços ativos. Sem prova real, manter `pilot` e registrar a limitação.

## Validação e rollback

Testes focados: `python3 -B -m unittest test_monitor.py` e `node --test push/test.mjs`. Cobrem commits antes de ACK, replay/reinício, criptografia, identidade, reconexão, inscrição sem substituição, resposta ambígua, consumo de sequência, backoff, gate e segurança de trinta minutos em perda de socket. Não enviam mensagens reais nem fazem resgates.

Rollback: parar apenas `uol-instagram-push`, retirar `pushMode`/`pushProof` da configuração e reiniciar o monitor Python. Preservar `push.sqlite`, `monitor.sqlite`, inscrição, sessões e recibos. A fila atual retoma o intervalo normal sem perder deduplicação. Não cancelar a inscrição do X ou substituir cookies/inscrição do navegador.
