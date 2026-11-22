# Consulta privada de fotos de perfil do WhatsApp

A consulta usa **a sessão Baileys já conectada**, sem abrir uma segunda sessão nem enviar mensagens. Disponível somente no servidor pessoal, via `POST /v1/whatsapp/profile-photos`, protegido pela autenticação Bearer existente e pelo vínculo de rede loopback `127.0.0.1`.

## Contrato

Corpo JSON: `{"numbers":["5511999990001","5511999990002"]}` (números de exemplo em E.164, somente dígitos). Limite de três números únicos por chamada.

Resposta JSON: `{"photos":[{"index":0,"status":"available","mime":"image/jpeg","base64":"..."},{"index":1,"status":"unavailable"}]}`.

- `available`: imagem presente, com MIME e conteúdo base64 (JPEG/PNG).
- `unavailable`: WhatsApp não apresentou foto ao titular da sessão, inclusive por restrições de privacidade.
- `failed`: não foi possível confirmar ou baixar o arquivo; **não equivale** a ausência de foto.
- O campo `index` segue a ordem original. O endpoint não devolve números, JIDs ou URLs das fotos.
- Imagens de até 1 MiB; HTTPS com hosts de mídia do WhatsApp autorizados e redirecionamento limitado. Não segue URLs internas ou arbitrárias.

## Operação segura

1. Validar localmente o código e os testes antes de instalar.
2. Publicar **somente** os arquivos necessários no serviço privado existente da Oracle. Preservar `/var/lib/baileys-personal` e todas as credenciais/sessões SQLite.
3. Planejar um único reinício supervisionado do serviço já configurado, sem criar instância concorrente. Conferir que ele retorna a `connected`.
4. Consultar exclusivamente os números autorizados pelo proprietário e salvar as fotos em diretório privado. Não incluir números reais, tokens, URLs ou base64 em Git, logs, PRs ou artefatos públicos.
5. Se a sessão estiver desconectada, falhar sem consulta. Não tentar novo pareamento nem apagar estado automaticamente.

**Estado atual:** esta alteração no código-fonte **não é um deploy**. O conector WhatsApp pessoal do ChatGPT não expõe fotos, e o endpoint não está publicado via MCP. A captura real requer executar a consulta por uma via administrativa autorizada depois de implantar a mudança.
