# Gatilho Instagram no iPhone

O iPhone envia apenas um aviso autenticado. O Oracle usa a sessão já existente para consultar Stories; a esteira central identifica cada Story e evita repetir entregas. Nenhuma sessão Instagram ou credencial de WhatsApp vai para o atalho ou para os amigos.

## Atalho nativo recomendado

1. Importe `UOL Instagram.shortcut` assinado. Informe **seu código individual de gatilho** na pergunta de importação.
2. Confira Instagram → Título contém `clubeuol`. Esse filtro foi preservado do atalho original; ajuste ao título que efetivamente chega ao seu aparelho. Não há restrição a Stories.
3. Deixe a automação habilitada e configure execução imediata, sem confirmação, se essa opção for oferecida pelo iOS.
4. Cada amigo importa o mesmo arquivo **sem código preenchido** e recebe um código individual. Não compartilhe uma exportação que já contenha seu código.

Todos os participantes devem seguir `@clubeuol` e habilitar os avisos desejados no Instagram, principalmente Stories. O iOS do participante precisa oferecer o gatilho **Receber notificação de app**, documentado pela Apple no [guia de gatilhos](https://support.apple.com/guide/shortcuts/event-triggers-apd932ff833f/ios). Se esse gatilho não existir no aparelho, importar as ações não habilita recepção automática.

O POST usa exclusivamente `/instagram-wake`. Um código só pode produzir avisos desse perfil; ele não permite enviar mensagens, ler inventário ou acessar configurações. Revogar um participante não exige trocar os códigos dos demais.

O arquivo exportado contém o gatilho de notificação `WFNotificationTrigger`, como o original enviado pelo usuário. A assinatura comprova a integridade do pacote. A importação do gatilho, as permissões e a execução com iPhone bloqueado precisam de prova no aparelho e na versão de iOS utilizada. Se o sistema importar somente as ações, crie a automação pessoal Instagram no aparelho e execute este atalho por ela.

## Scriptable opcional

O atalho nativo evita abrir outro executor e é a opção mais curta. Scriptable não antecipa a notificação do iOS nem fornece a imagem do Story dentro dela.

Para usar a alternativa:

1. Copie `UOL Instagram.js` para Scriptable com o nome **UOL Instagram**.
2. Rode uma vez **dentro do Scriptable**, cole seu código na tela de configuração e salve. Ele fica no Keychain local.
3. Na automação Instagram, use **Run Script** → UOL Instagram, parâmetro `wake`, execução no app desativada. O parâmetro identifica execução automática e impede abrir cadastro durante a automação.

O cliente salva cada ID pendente em um arquivo próprio antes da rede. Resposta perdida reutiliza o mesmo ID; o ACK remove somente esse arquivo. Duas execuções sobrepostas não regravam uma lista compartilhada nem apagam o aviso da outra. A admissão limita a fila a 64 sinais; execuções simultâneas podem ultrapassar brevemente esse limite e continuam drenando. Com a fila cheia e a rede indisponível, o aviso novo fica coalescido nos sinais genéricos já pendentes, sem alterar suas identidades; eles pedem uma consulta do estado atual dos Stories quando voltarem a funcionar. Na recuperação, o primeiro espaço liberado recebe o novo aviso antes de seu envio. Cada execução processa até oito, sem laço infinito. HTTP de recusa conserva a pendência. O servidor decide a captura e deduplica por Story entre todos os celulares; essa é a garantia de não duplicar mensagens. A fila do primeiro rascunho é importada sem apagar seu arquivo privado original.

## Gerar e verificar

```sh
python3 criar_atalho.py
python3 -m unittest test_shortcut.py
node --test test-scriptable.mjs
shortcuts sign --mode anyone --input 'UOL Instagram.unsigned.shortcut' --output 'UOL Instagram.shortcut'
```

O gerador e os artefatos distribuíveis não contêm códigos reais. A pergunta de importação aponta para uma ação Texto vazia. Não há upload automático ao iCloud nem alteração do link original.

Para conferir somente a renderização nativa do identificador, sem rede, código ou gatilho: `python3 criar_atalho.py --uuid-test --output '/tmp/UOL Instagram UUID.unsigned.shortcut'`. O teste contém apenas seis números aleatórios e Texto; todos os intervalos cabem em 32 bits. Os campos de versão e variante ficam fixos em `4` e `8`. Este identificador serve para idempotência, não para autenticação.

Referências oficiais do cliente opcional: [UUID](https://docs.scriptable.app/uuid/), [Request](https://docs.scriptable.app/request/) e [Keychain](https://docs.scriptable.app/keychain/).
