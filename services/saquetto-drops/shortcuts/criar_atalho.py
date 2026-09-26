"""Generate an unsigned, secret-free native Shortcut for Saquetto Drops.

Scriptable action signature and native text/image/base64/menu parameter names
were checked against local Shortcuts.sqlite, read-only, on 2026-09-25.
The only Scriptable parameter is a dictionary; no guessed image intent fields.
"""
import plistlib
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
actions = []


def uid():
    return str(uuid.uuid4()).upper()


def ref(identifier, name="Resultado"):
    return {"Value": {"Type": "ActionOutput", "OutputUUID": identifier, "OutputName": name}, "WFSerializationType": "WFTextTokenAttachment"}


def variable(name):
    return {"Value": {"Type": "Variable", "VariableName": name}, "WFSerializationType": "WFTextTokenAttachment"}


def text(value):
    if isinstance(value, str):
        return {"Value": {"string": value}, "WFSerializationType": "WFTextTokenString"}
    return {"Value": {"string": "\ufffc", "attachmentsByRange": {"{0, 1}": value["Value"]}}, "WFSerializationType": "WFTextTokenString"}


def mixed(*parts):
    output, attachments = "", {}
    for part in parts:
        if isinstance(part, str):
            output += part
        else:
            # NSRange uses UTF-16 offsets, including emoji in surrounding text.
            offset = len(output.encode("utf-16-le")) // 2
            attachments[f"{{{offset}, 1}}"] = part["Value"]
            output += "\ufffc"
    return {"Value": {"string": output, "attachmentsByRange": attachments}, "WFSerializationType": "WFTextTokenString"}


def add(identifier, parameters=None):
    parameters = dict(parameters or {})
    parameters.setdefault("UUID", uid())
    actions.append({"WFWorkflowActionIdentifier": identifier, "WFWorkflowActionParameters": parameters})
    return ref(parameters["UUID"])


def set_variable(name, value):
    add("is.workflow.actions.setvariable", {"WFVariableName": name, "WFInput": value})


def call(action, **fields):
    fields = {"action": action, **fields}
    items = [{"WFKey": text(key), "WFItemType": 0, "WFValue": text(value)} for key, value in fields.items()]
    dictionary = add("is.workflow.actions.dictionary", {"WFItems": {"Value": {"WFDictionaryFieldValueItems": items}, "WFSerializationType": "WFDictionaryFieldValue"}})
    return add("dk.simonbs.Scriptable.ParameterizedRunScriptIntent", {"fileName": "Saquetto Drops", "parameter": dictionary, "runInApp": False, "ShowWhenRun": False})


def get(value, key):
    return add("is.workflow.actions.getvalueforkey", {"WFInput": value, "WFDictionaryKey": key, "WFGetDictionaryValueType": "Value"})


def menu(prompt, choices):
    group = uid()
    add("is.workflow.actions.choosefrommenu", {"WFMenuPrompt": prompt, "WFControlFlowMode": 0, "WFMenuItems": choices, "GroupingIdentifier": group})
    return group


def branch(group, title):
    add("is.workflow.actions.choosefrommenu", {"WFControlFlowMode": 1, "WFMenuItemTitle": title, "GroupingIdentifier": group})


def end_menu(group):
    add("is.workflow.actions.choosefrommenu", {"WFControlFlowMode": 2, "GroupingIdentifier": group})


def show_result(value):
    add("is.workflow.actions.showresult", {"Text": text(get(value, "message"))})


add("is.workflow.actions.comment", {"WFCommentActionText": "Saquetto Drops • seleções e confirmação no Atalhos; Scriptable cuida da API. Compartilhe texto, links e até 10 imagens. Nada é enviado antes da confirmação. Resposta na fila não confirma entrega. Em caso de interrupção, use Consultar / Retomar último envio."})
shortcut_input = {"Value": {"Type": "ExtensionInput"}, "WFSerializationType": "WFTextTokenAttachment"}
set_variable("Entrada compartilhada", shortcut_input)
main = menu("Saquetto Drops", ["Novo envio", "Consultar último envio", "Retomar último envio"])

branch(main, "Novo envio")
names = get(call("destinations"), "names")
chosen = add("is.workflow.actions.choosefromlist", {"WFInput": names, "WFChooseFromListActionPrompt": "Selecione os destinos", "WFChooseFromListActionSelectMultiple": True, "WFChooseFromListActionSelectAll": False})
chosen_text = add("is.workflow.actions.text.combine", {"text": chosen, "WFTextSeparator": "New Lines"})
set_variable("Destinos selecionados", chosen_text)
original = add("is.workflow.actions.gettext", {"WFTextActionText": text(variable("Entrada compartilhada"))})
set_variable("Texto original", original)

caption = menu("Texto / legenda", ["Manter texto compartilhado", "Editar texto ou legenda", "Sem texto ou legenda"])
branch(caption, "Manter texto compartilhado")
set_variable("Texto final", variable("Texto original"))
branch(caption, "Editar texto ou legenda")
edited = add("is.workflow.actions.ask", {"WFAskActionPrompt": "Texto completo (com imagens: até 1024 caracteres; só texto: até 8000)", "WFInputType": "Text", "WFAllowsMultilineText": True, "WFAskActionDefaultAnswer": text(variable("Texto original"))})
set_variable("Texto final", edited)
branch(caption, "Sem texto ou legenda")
empty_text = add("is.workflow.actions.gettext", {"WFTextActionText": ""})
set_variable("Texto final", empty_text)
end_menu(caption)

image_menu = menu("Imagens neste envio", ["Sem imagens", "Com imagens compartilhadas"])
branch(image_menu, "Sem imagens")
empty_images = add("is.workflow.actions.gettext", {"WFTextActionText": ""})
set_variable("Imagens em Base64", empty_images)
zero = add("is.workflow.actions.gettext", {"WFTextActionText": "0"})
set_variable("Quantidade de imagens", zero)
branch(image_menu, "Com imagens compartilhadas")
images = add("is.workflow.actions.detect.images", {"WFInput": variable("Entrada compartilhada")})
set_variable("Imagens compartilhadas", images)
count = add("is.workflow.actions.count", {"Input": images, "WFInput": images})
set_variable("Quantidade de imagens", count)
# Quick Look displays the actual images before the final native confirmation.
add("is.workflow.actions.previewdocument", {"WFInput": variable("Imagens compartilhadas")})
repeat = uid()
add("is.workflow.actions.repeat.each", {"WFInput": variable("Imagens compartilhadas"), "WFControlFlowMode": 0, "GroupingIdentifier": repeat})
resized = add("is.workflow.actions.image.resize", {"WFImage": variable("Repeat Item"), "WFImageResizeKey": "Size", "WFImageResizeWidth": "2048", "WFImageResizeHeight": "", "WFImageResizeLength": ""})
# JPEG is the native Convert Image action's default format, observed locally.
jpeg = add("is.workflow.actions.image.convert", {"WFInput": resized, "WFImageFormat": "JPEG", "WFImageCompressionQuality": 0.85, "WFImagePreserveMetadata": False})
add("is.workflow.actions.base64encode", {"WFInput": jpeg, "WFBase64LineBreakMode": "None"})
encoded = add("is.workflow.actions.repeat.each", {"WFControlFlowMode": 2, "GroupingIdentifier": repeat})
encoded_text = add("is.workflow.actions.text.combine", {"text": encoded, "WFTextSeparator": "New Lines"})
set_variable("Imagens em Base64", encoded_text)
end_menu(image_menu)

add("is.workflow.actions.alert", {"WFAlertActionTitle": "Confirmar envio", "WFAlertActionCancelButtonShown": True, "WFAlertActionMessage": mixed("Destinos:\n", variable("Destinos selecionados"), "\n\nTexto / legenda:\n", variable("Texto final"), "\n\nImagens: ", variable("Quantidade de imagens"), "\nA legenda acompanha somente a primeira imagem.\n\nToque OK para colocar na fila de envio.")})
sent = call("send", destinations=variable("Destinos selecionados"), text=variable("Texto final"), imagesBase64=variable("Imagens em Base64"), confirmed="yes")
show_result(sent)

branch(main, "Consultar último envio")
show_result(call("status"))

branch(main, "Retomar último envio")
review = get(call("review"), "message")
add("is.workflow.actions.alert", {"WFAlertActionTitle": "Retomar último envio", "WFAlertActionMessage": text(review), "WFAlertActionCancelButtonShown": True})
show_result(call("retry", confirmed="yes"))
end_menu(main)

workflow = {
    "WFWorkflowName": "Saquetto Drops",
    "WFWorkflowActions": actions,
    "WFWorkflowClientVersion": "3036.0.4",
    "WFWorkflowMinimumClientVersion": 900,
    "WFWorkflowMinimumClientVersionString": "900",
    "WFWorkflowIcon": {"WFWorkflowIconStartColor": 463140863, "WFWorkflowIconGlyphNumber": 59511},
    "WFWorkflowTypes": ["ActionExtension"],
    "WFWorkflowInputContentItemClasses": ["WFStringContentItem", "WFURLContentItem", "WFImageContentItem"],
    "WFWorkflowImportQuestions": [],
}
assert len({item["WFWorkflowActionParameters"]["UUID"] for item in actions}) == len(actions)
destination = ROOT / "Saquetto Drops.unsigned.shortcut"
destination.write_bytes(plistlib.dumps(workflow, fmt=plistlib.FMT_BINARY, sort_keys=False))
print(f"Generated {destination.name}: {len(actions)} native actions; no credentials.")
