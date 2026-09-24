// Variables used by Scriptable.
// These must be at the very top of the file. Do not edit.
// icon-color: red; icon-glyph: link;

// Versão 6. Envia o último post ao WhatsApp próprio pelo Beeper/Oracle.
// No Atalhos, deixe Run In App desligado.
// Parâmetro: @usuario, usuario ou https://x.com/usuario.
// Vazio: usa tvglobo. HTML como parâmetro mantém o modo antigo (tvglobo).
// Usa cartão com thumbnail, legenda e link. Cada execução envia novamente.
// Thumbnail: imagem/prévia de vídeo do post; sem mídia, foto do perfil.
// A configuração inicial é importada do iCloud para o Keychain.

var perfil = "tvglobo";
var entrada = args.shortcutParameter;
var html = null;
if (entrada !== null && entrada !== undefined && entrada !== "") {
  if (typeof entrada !== "string") {
    throw new Error("Informe um único @ como texto no parâmetro do Atalhos.");
  }
  entrada = entrada.trim();
  if (entrada[0] === "<") html = entrada;
  else if (entrada) {
    perfil = entrada.replace(/^https:\/\/(?:www\.)?(?:x\.com|twitter\.com)\//i, "");
    perfil = perfil.replace(/\/$/, "").replace(/^@/, "");
  }
}
perfil = perfil.toLowerCase();
if (!/^[a-z0-9_]{1,15}$/.test(perfil)) {
  throw new Error("@ inválido. Informe só o usuário ou o link do perfil, sem link de post.");
}
var gatewayToken = await obterToken();

if (html === null || html === undefined) {
  var request = new Request("https://x.com/" + perfil);
  request.timeoutInterval = 20;
  request.headers = { "User-Agent": "Mozilla/5.0" };
  html = await request.loadString();
  if (request.response.statusCode !== 200) {
    throw new Error("O X bloqueou a consulta. HTTP " + request.response.statusCode);
  }
}

if (typeof html !== "string") {
  throw new Error("Passe o HTML como texto ou deixe o parâmetro vazio.");
}

// Reaproveita a página já consultada, sem buscar a foto em outra conta.
var metaPerfil = lerMetadados(html);
var fotoPerfil = escolherImagem(metaPerfil, "perfil");
html = html.replace(/<!--[\s\S]*?-->/g, "");
html = html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, "");
html = html.replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, "");
var artigos = html.match(/<article\b[^>]*>[\s\S]*?<\/article>/gi) || [];
var ultimo = "";

for (var i = 0; i < artigos.length; i++) {
  var links = /<a\b[^>]*\shref\s*=\s*(["'])([^"']+)\1[^>]*>/gi;
  var link;
  while ((link = links.exec(artigos[i])) !== null) {
    var post = link[2].match(/^(?:https:\/\/(?:www\.)?(?:x\.com|twitter\.com))?\/([a-z0-9_]+)\/status\/(\d+)(?:\/?(?:[?#].*)?)$/i);
    if (!post) continue;

    // Só o primeiro permalink identifica o post; os demais podem ser citações.
    if (post[1].toLowerCase() === perfil) {
      var id = post[2];
      if (id.length > ultimo.length) ultimo = id;
      else if (id.length === ultimo.length && id > ultimo) ultimo = id;
    }
    break;
  }
}

if (ultimo === "") {
  throw new Error("O X não entregou posts no HTML. Pode exigir login ou ter alterado a página.");
}

var url = "https://x.com/" + perfil + "/status/" + ultimo;
var detalhes = await obterDetalhes(url, fotoPerfil);
var titulo = perfil === "tvglobo" ? "TV Globo • @tvglobo" : "X • @" + perfil;
var mensagem = "📺 " + titulo + "\n\n" + detalhes.legenda + "\n\n🔗 Abrir post:\n" + url;
var envio = new Request("https://163-176-194-58.sslip.io/v1/send-x-post");
envio.method = "POST";
envio.timeoutInterval = 45;
var cabecalhos = {};
cabecalhos["Authorization"] = "Bearer " + gatewayToken;
cabecalhos["Content-Type"] = "application/json";
envio.headers = cabecalhos;
// Nunca encaminhe a credencial a outro endereço por redirecionamento.
envio.onRedirect = function () { return null; };
var preview = {};
preview.title = titulo;
preview.summary = detalhes.legenda;
preview.imageUrl = detalhes.imagem;
var corpo = {};
corpo.link = url;
corpo.text = mensagem;
corpo.preview = preview;
envio.body = JSON.stringify(corpo);

var respostaTexto;
try {
  respostaTexto = await envio.loadString();
} catch (_) {
  throw new Error("Envio sem confirmação. Pode ter chegado; confira seu WhatsApp antes de executar novamente.");
}
var resultado;
try { resultado = JSON.parse(respostaTexto); } catch (_) { resultado = {}; }
var status = envio.response.statusCode;
if ((status !== 200 && status !== 202) || resultado.deliveryState !== "confirmed_by_whatsapp_bridge") {
  var codigo = resultado.code || "resposta_invalida";
  throw new Error("Beeper não confirmou a entrega. HTTP " + status + " / " + codigo);
}
console.log("Post entregue no seu WhatsApp com thumbnail.");
Script.setShortcutOutput(url);
console.log(url);
if (config.runsInApp) Pasteboard.copyString(url);
Script.complete();

async function obterToken() {
  var chave = "tvglobo-beeper-token-v1";
  if (Keychain.contains(chave)) return Keychain.get(chave);
  var fm = FileManager.iCloud();
  var caminho = fm.joinPath(fm.documentsDirectory(), "TVGlobo-Beeper-config.json");
  if (!fm.fileExists(caminho)) {
    throw new Error("A configuração TVGlobo-Beeper-config.json ainda não sincronizou com o Scriptable pelo iCloud.");
  }
  await fm.downloadFileFromiCloud(caminho);
  var dados = JSON.parse(fm.readString(caminho));
  if (typeof dados.token !== "string" || !/^[a-f0-9]{64}$/.test(dados.token)) {
    throw new Error("Configuração do Beeper inválida.");
  }
  Keychain.set(chave, dados.token);
  // Remove somente o arquivo de configuração temporário recém-importado.
  fm.remove(caminho);
  return Keychain.get(chave);
}

function decodificarHtml(texto) {
  var entidades = { amp: "&", quot: '"', apos: "'", lt: "<", gt: ">", nbsp: " " };
  return String(texto || "").replace(/&(#x[0-9a-f]+|#\d+|amp|quot|apos|lt|gt|nbsp);/gi, function (original, valor) {
    if (valor[0] !== "#") return entidades[valor.toLowerCase()];
    var numero = valor[1].toLowerCase() === "x" ? parseInt(valor.slice(2), 16) : parseInt(valor.slice(1), 10);
    return numero >= 0 && numero <= 0x10ffff ? String.fromCodePoint(numero) : original;
  });
}

function lerMetadados(pagina) {
  var tags = pagina.match(/<meta\b[^>]*>/gi) || [];
  var meta = {};
  for (var i = 0; i < tags.length; i++) {
    var atributos = {};
    var regex = /([a-zA-Z_:][a-zA-Z0-9_:.-]*)\s*=\s*(["'])([\s\S]*?)\2/g;
    var atributo;
    while ((atributo = regex.exec(tags[i])) !== null) {
      atributos[atributo[1].toLowerCase()] = decodificarHtml(atributo[3]);
    }
    var nome = atributos.property || atributos.name;
    if (nome) meta[nome.toLowerCase()] = atributos.content || "";
  }
  return meta;
}

function escolherImagem(meta, tipo) {
  var candidatas = [meta["og:image"], meta["twitter:image"]];
  var caminho = tipo === "perfil"
    ? /^https:\/\/pbs\.twimg\.com\/profile_images\//i
    : /^https:\/\/pbs\.twimg\.com\/(?:media|amplify_video_thumb|ext_tw_video_thumb)\//i;
  for (var i = 0; i < candidatas.length; i++) {
    var imagem = String(candidatas[i] || "").trim();
    if (caminho.test(imagem)) return imagem.replace("format=webp", "format=jpg");
  }
  return "";
}

async function obterDetalhes(linkPost, fotoPerfil) {
  var req = new Request(linkPost);
  req.timeoutInterval = 20;
  req.headers = { "User-Agent": "Mozilla/5.0" };
  var pagina = await req.loadString();
  if (req.response.statusCode !== 200) {
    throw new Error("O X não disponibilizou a legenda e a imagem. HTTP " + req.response.statusCode);
  }
  var meta = lerMetadados(pagina);
  var legenda = (meta["og:description"] || meta["twitter:description"] || "").trim();
  var imagem = escolherImagem(meta, "post") || fotoPerfil;
  if (!legenda || !imagem) {
    throw new Error("O X não entregou a legenda ou nenhuma imagem utilizável do post/perfil.");
  }
  if (legenda.length > 6000) throw new Error("A legenda excede o limite desta mensagem.");
  var detalhes = {};
  detalhes.legenda = legenda;
  detalhes.imagem = imagem;
  return detalhes;
}
