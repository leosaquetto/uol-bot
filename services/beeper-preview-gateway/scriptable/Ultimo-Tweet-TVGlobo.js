// Variables used by Scriptable.
// These must be at the very top of the file. Do not edit.
// icon-color: red; icon-glyph: link;

// Versão 4. Envia o último post ao WhatsApp próprio pelo Beeper/Oracle.
// No Atalhos, deixe Run In App desligado.
// Parâmetro opcional: HTML de https://x.com/tvglobo como texto.
// Usa cartão com thumbnail, legenda e link. Não duplica o mesmo post.
// A configuração inicial é importada do iCloud para o Keychain.

var perfil = "tvglobo";
var gatewayToken = await obterToken();
var html = args.shortcutParameter;
if (typeof html === "string" && html.trim() === "") html = null;

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
var ultimaChave = "tvglobo-beeper-ultimo-enviado-v1";
if (!Keychain.contains(ultimaChave) || Keychain.get(ultimaChave) !== ultimo) {
  var detalhes = await obterDetalhes(url);
  var mensagem = "📺 TV Globo • @tvglobo\n\n" + detalhes.legenda + "\n\n🔗 Abrir post:\n" + url;
  var envio = new Request("https://163-176-194-58.sslip.io/v1/send-tvglobo");
  envio.method = "POST";
  envio.timeoutInterval = 45;
  var cabecalhos = {};
  cabecalhos["Authorization"] = "Bearer " + gatewayToken;
  cabecalhos["Content-Type"] = "application/json";
  cabecalhos["Idempotency-Key"] = "tvglobo:" + ultimo + ":self:v1";
  envio.headers = cabecalhos;
  // Nunca encaminhe a credencial a outro endereço por redirecionamento.
  envio.onRedirect = function () { return null; };
  var preview = {};
  preview.title = "TV Globo • @tvglobo";
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
    throw new Error("Sem confirmação do envio. O servidor mantém a proteção contra duplicatas; não altere a chave do post.");
  }
  var resultado;
  try { resultado = JSON.parse(respostaTexto); } catch (_) { resultado = {}; }
  var status = envio.response.statusCode;
  if ((status !== 200 && status !== 202) || resultado.deliveryState !== "confirmed_by_whatsapp_bridge") {
    var codigo = resultado.code || "resposta_invalida";
    throw new Error("Beeper não confirmou a entrega. HTTP " + status + " / " + codigo);
  }
  Keychain.set(ultimaChave, ultimo);
  console.log(resultado.replayed ? "Este post já estava entregue." : "Post entregue no seu WhatsApp com thumbnail.");
} else {
  console.log("Este post já foi enviado ao seu WhatsApp.");
}
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

async function obterDetalhes(linkPost) {
  var req = new Request(linkPost);
  req.timeoutInterval = 20;
  req.headers = { "User-Agent": "Mozilla/5.0" };
  var pagina = await req.loadString();
  if (req.response.statusCode !== 200) {
    throw new Error("O X não disponibilizou a legenda e a imagem. HTTP " + req.response.statusCode);
  }
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
  var legenda = (meta["og:description"] || meta["twitter:description"] || "").trim();
  var imagem = (meta["og:image"] || meta["twitter:image"] || "").trim();
  if (!legenda || !/^https:\/\/pbs\.twimg\.com\//i.test(imagem)) {
    throw new Error("O X não entregou uma legenda e thumbnail válidas para o post.");
  }
  if (legenda.length > 6000) throw new Error("A legenda excede o limite desta mensagem.");
  var detalhes = {};
  detalhes.legenda = legenda;
  detalhes.imagem = imagem.replace("format=webp", "format=jpg");
  return detalhes;
}
