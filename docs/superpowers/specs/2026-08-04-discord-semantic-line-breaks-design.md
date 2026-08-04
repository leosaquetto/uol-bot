# Quebras semânticas no card de ingressos do Discord

## Objetivo

Deixar a descrição dos cards de ingressos no Discord visualmente próxima da página do Clube UOL, separando os blocos de conteúdo com o mesmo espaçamento usado entre o aviso inicial e o texto da oferta.

## Decisão

O `buildDiscordPayload` continuará sendo o único ponto de montagem do embed. Antes de compor `description`, um formatador específico do Discord reconhecerá títulos semânticos já presentes no texto — como `Data`, `Local`, `Importante`, `REGRAS DE RESGATE` e `Atenção, Assinante UOL!` — e inserirá uma linha vazia (`\\n\\n`) antes de cada bloco.

Não haverá quebra por quantidade fixa de caracteres. O Discord continuará fazendo a quebra visual conforme a largura do dispositivo. O texto continuará limitado pelo teto atual do embed, e o status, thumbnail, campos, URL e edição de esgotado permanecerão inalterados.

## Limites

- Não alterar `cleanText`, o modelo de oferta, Telegram, deduplicação ou o fluxo de entrega.
- Não criar dependência externa nem nova chamada de rede.
- Não alterar o conteúdo semântico; somente a separação dos blocos.
- Reutilizar a mesma descrição formatada tanto no envio quanto na edição de esgotado.

## Validação

Adicionar teste unitário em `test/discord.test.js` verificando que o payload mantém o aviso inicial, insere `\\n\\n` entre os blocos reconhecidos e preserva os campos/URL/imagem existentes. Rodar o teste específico e a suíte rápida do Worker.
