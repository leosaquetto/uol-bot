# Stories Hunter 5.8.0

- Fontes separadas em host nativo, updater, HTML, CSS, JavaScript da interface,
  assets, montagem e distribuição. Continua um único arquivo no aparelho.
- Extração mecânica comprovada com artefato idêntico à 5.7.9 antes da otimização.
- Logo e SVGs embutidos uma vez; HTML/CSS final idêntico à referência sintética.
- Build público reproduzível, com recursos versionados e sem leituras privadas.
- Publicação com manifest, tamanho, SHA-256 e artefato fixado em commit. Beta e
  stable podem compartilhar os mesmos bytes; espelho legado acompanha stable.
- Updater verifica download e sintaxe, prepara backup/staging/journal e confere
  instalação. Estados de erro explícitos; limites do Scriptable documentados.
- Descoberta de versão após dashboard, pelo banner existente. Nova ação explícita
  `update-auto` para Atalhos sem confirmação; fluxo manual preservado.
- Coleta, acesso, resgate, dados, callbacks, temas e ordem de inicialização
  preservados. Sem otimização de cache, prefetch ou respostas assíncronas nesta fase.

Validação: **218/218 testes offline**. Artefato: **661.663 bytes**, contra 782.163
da referência (−120.500 bytes, −15,41%). SHA-256:
`47c0d133dec0cb5df566cb73446a5870a24dbfaf8ed98d0be8c5fcd05ee48fd1`.

Publicação e confirmação física são registradas separadamente. Procedimento em
[ARCHITECTURE.md](ARCHITECTURE.md).
