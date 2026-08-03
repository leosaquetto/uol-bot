# Telegram: espera de foto com teto de 60 segundos

## Objetivo

Publicar cada oferta no canal principal com foto e legenda sempre que a mídia ficar disponível em até 60 segundos, sem depender de preview do Telegram. Se o prazo expirar, publicar texto. Quando a foto surgir depois, converter a mesma mensagem em foto com legenda por `editMessageMedia`.

Discord permanece sem alterações.

## Fluxo

1. A primeira detecção pela API inicia um prazo absoluto de 60 segundos.
2. O Worker tenta a foto imediatamente usando, nesta ordem, `file_id` em cache, URL da API e upload dos bytes.
3. Se nenhuma estratégia produzir foto, a oferta continua pendente. Novas tentativas usam os alarmes existentes, sem bloquear uma execução por 60 segundos.
4. Se a foto funcionar antes do prazo, o Worker envia `sendPhoto` com a legenda completa.
5. Se o prazo expirar, o Worker envia `sendMessage` com o texto completo e preview desativado.
6. Se enriquecimento posterior fornecer uma foto, o Worker chama `editMessageMedia` com `InputMediaPhoto` e a mesma legenda. A mensagem conserva o `message_id` e passa a exibir foto mais texto.

## Estado e idempotência

- O prazo é contado desde `first_seen_at`; reinícios não renovam os 60 segundos.
- Texto enviado por prazo recebe estratégia `text_timeout`.
- Tentativas tardias guardam contador, próximo horário e erro sanitizado.
- Sucesso tardio muda `main_message_kind` para `photo` e registra estratégia/cache da imagem.
- Mutações de foto param 38 segundos antes do prazo: 8 segundos de timeout e
  30 segundos para o webhook reconciliar resultado ambíguo.
- Sem confirmação após essa janela, o contrato operacional existente prioriza
  não perder a oferta e aceita duplicata rara no fallback textual.

## Falhas

- Falha de foto antes do prazo mantém oferta pendente.
- Após 60 segundos, falha de foto nunca impede o texto.
- Resultado ambíguo usa primeiro a janela de reconciliação do webhook.
- Falha de `editMessageMedia` mantém o texto e agenda nova tentativa com backoff e limite.
- Nenhum preview de link participa do fluxo.

## Configuração

- `MAIN_IMAGE_WAIT_SECONDS=60`, limitado a uma faixa segura.
- Cadência de API continua em 15 segundos.
- Manutenção/HTML continua fora do caminho crítico.
- Nenhuma fila, serviço ou custo adicional.

## Testes

- Foto disponível imediatamente envia `sendPhoto` com legenda.
- Sem foto antes do prazo não envia texto.
- Prazo expirado envia texto sem preview.
- Foto tardia usa `editMessageMedia` com `InputMediaPhoto.caption`.
- Edição tardia preserva `message_id`, atualiza estado e não duplica postagem.
- Discord não sofre alteração.
