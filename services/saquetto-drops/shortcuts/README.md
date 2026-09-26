# Saquetto Drops no Atalhos

Artefatos prontos: `Saquetto Drops.js` para Scriptable e `Saquetto Drops.shortcut` assinado para importar no Atalhos. O atalho recebe texto, URLs e imagens pelo Compartilhar. Também abre sem entrada para escrever uma mensagem, consultar ou retomar o último envio.

Toda seleção fica no Atalhos: destinos com seleção múltipla, manter/editar/remover legenda, envio com/sem imagens, visualização das imagens e confirmação final. Scriptable executa em segundo plano (`runInApp=false`) e retorna dados; não apresenta telas nem copia texto para a área de transferência.

## Instalação

1. Copiar `Saquetto Drops.js` para a pasta Scriptable no iCloud, mantendo o nome **Saquetto Drops**.
2. Provisionar nessa mesma pasta o arquivo privado `Saquetto Drops-config.private.json`, com `baseUrl` HTTPS e `token`. Não colocar esse arquivo no repositório. A primeira execução importa os valores para Keychain e remove somente esse arquivo após conferir a gravação.
3. Assinar `Saquetto Drops.unsigned.shortcut` com `shortcuts sign --mode anyone --input … --output 'Saquetto Drops.shortcut'` e importar o arquivo assinado no Atalhos.

As chaves utilizadas são `saquetto-drops.base-url` e `saquetto-drops.token`. Texto do envio e registro de retomada ficam no armazenamento local privado do Scriptable; token não entra no atalho nem nesses registros.

## Comportamento

- O atalho preserva texto, links e quebras de linha; o servidor remove espaços vazios nas extremidades. A edição usa Pedir Entrada com várias linhas. A legenda original só está disponível quando o aplicativo de origem compartilha também o texto; uma foto sozinha não contém essa legenda.
- Imagens compartilhadas são redimensionadas para largura 2048, convertidas para JPEG sem metadados e codificadas sem quebras. Máximo: 10 imagens de até 8 MiB, total de 16 MiB por execução; legenda de até 1024 caracteres. Sem imagens: até 8000 caracteres. A legenda acompanha somente a primeira imagem, conforme o contrato do servidor. Os links continuam clicáveis; este envio manual não monta o cartão visual dos posts do X.
- `send` só roda depois da confirmação nativa. Antes do POST, salva UUID e payload completos. Uma falha de rede não cria automaticamente outro pedido.
- **Consultar último envio** apenas consulta. **Retomar último envio** consulta primeiro; só repete o POST se o pedido ainda não existir, usando exatamente o mesmo UUID, destinatários, texto e IDs de mídia. Se já existir, mostra o status sem reenviar.
- “Na fila” não confirma entrega. “Aceito pelo WhatsApp” também não confirma recebimento pelo destinatário.
- Nova execução é bloqueada enquanto houver pedido com resultado incerto. A retomada não tenta reenviar individualmente trabalhos já registrados ou com falha.

## Verificação

`python3 shortcuts/criar_atalho.py` gera o plist binário sem segredos. `node --test test/shortcut-client.test.js` cobre limites, preservação do texto, mapeamento dos destinos, fluxo nativo e retomada após resposta incerta. Assinatura/importação e execução no iPhone são verificações separadas.

A assinatura Scriptable e parâmetros nativos de menu, texto, seleção, imagem e Base64 foram conferidos no banco local de Atalhos, em leitura. Referências oficiais: [args](https://docs.scriptable.app/args/), [Request](https://docs.scriptable.app/request/), [Data](https://docs.scriptable.app/data/), [FileManager](https://docs.scriptable.app/filemanager/).

Validação em 25/09/2026: oito testes do cliente passaram; arquivo assinado pelo `shortcuts sign`; script e atalho copiados para iCloud. Consulta HTTPS retornou 11 destinos, sem nomes duplicados. Um teste de foto com legenda e link, restrito ao chat pessoal, recebeu aceite do servidor WhatsApp. Importação, permissões e execução completa no iPhone ainda precisam de validação no aparelho; assinatura e aceite da API não comprovam essas etapas.
