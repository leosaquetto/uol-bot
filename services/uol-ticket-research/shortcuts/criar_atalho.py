"""Build the shareable, secret-free Instagram notification Shortcut.

Notification trigger copied from the user's public Shortcut 00789ae7891c4f928cc582d42f6d8698,
inspected 2026-10-16. Native date, random, text, HTTP and import-question keys
were checked against local WorkflowKit metadata and Shortcuts.sqlite read-only.
Signing and actual execution on the iPhone remain separate checks.
"""
import argparse
import plistlib
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
URL = "https://uol-telegram-shadow-pilot.leosaquetto.workers.dev/instagram-wake"


def build():
    actions = []

    def ref(identifier):
        return {"Value": {"Type": "ActionOutput", "OutputUUID": identifier, "OutputName": "Resultado"}, "WFSerializationType": "WFTextTokenAttachment"}

    def mixed(*parts):
        result, attachments = "", {}
        for part in parts:
            if isinstance(part, str):
                result += part
            else:
                offset = len(result.encode("utf-16-le")) // 2
                attachments[f"{{{offset}, 1}}"] = part["Value"]
                result += "\ufffc"
        return {"Value": {"string": result, "attachmentsByRange": attachments}, "WFSerializationType": "WFTextTokenString"}

    def add(identifier, parameters):
        parameters = {**parameters, "UUID": str(uuid.uuid4()).upper()}
        actions.append({"WFWorkflowActionIdentifier": identifier, "WFWorkflowActionParameters": parameters})
        return ref(parameters["UUID"])

    def dictionary(items):
        return {"Value": {"WFDictionaryFieldValueItems": [{"WFKey": {"Value": {"string": key}, "WFSerializationType": "WFTextTokenString"}, "WFItemType": 0, "WFValue": value if isinstance(value, dict) and value.get("WFSerializationType") == "WFTextTokenString" else mixed(value)} for key, value in items]}, "WFSerializationType": "WFDictionaryFieldValue"}

    add("is.workflow.actions.comment", {"WFCommentActionText": "UOL Instagram: este iPhone apenas avisa o servidor. Cada participante usa seu próprio código de gatilho, sem sessão Instagram ou permissão de envio. Captura e deduplicação de Stories acontecem no servidor. Configure o título do perfil conforme a notificação recebida e execução automática sem confirmação. Não compartilhe uma cópia já preenchida com seu código."})
    code_index = len(actions)
    code = add("is.workflow.actions.gettext", {"WFTextActionText": ""})
    now = add("is.workflow.actions.date", {"WFDateActionMode": "Current Date"})
    observed = add("is.workflow.actions.format.date", {"WFDate": mixed(now), "WFDateFormatStyle": "Custom", "WFDateFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "WFLocale": "en_US_POSIX"})
    # Decimal digits are valid hexadecimal digits. Fixed version/variant and
    # independent segments avoid reliance on an unverified native UUID action.
    segments = [add("is.workflow.actions.number.random", {"WFRandomNumberMinimum": lo, "WFRandomNumberMaximum": hi}) for lo, hi in [(10000000, 99999999), (1000, 9999), (100, 999), (100, 999), (100000, 999999), (100000, 999999)]]
    event_id = add("is.workflow.actions.gettext", {"WFTextActionText": mixed(segments[0], "-", segments[1], "-4", segments[2], "-8", segments[3], "-", segments[4], segments[5])})
    add("is.workflow.actions.downloadurl", {
        "WFURL": URL, "WFHTTPMethod": "POST", "ShowHeaders": True,
        "WFHTTPHeaders": dictionary([("Authorization", mixed("Bearer ", code)), ("Content-Type", "application/json")]),
        "WFHTTPBodyType": "JSON",
        "WFJSONValues": dictionary([("eventId", event_id), ("profile", "clubeuol"), ("source", "ios_shortcuts"), ("observedAt", observed)]),
    })
    return {
        "WFWorkflowName": "UOL Instagram",
        "WFWorkflowTriggers": [{"WFTriggerSerializedParameters": {"SelectedApps": {"BundleIdentifier": "com.burbn.instagram", "Name": "Instagram", "TeamIdentifier": "777W53UFB2"}, "Conditions": {"WFSerializationType": "WFContentPredicateTableTemplate", "Value": {"WFActionParameterFilterTemplates": [{"Property": "Title", "Operator": 0, "Values": {"Title": "clubeuol"}, "Removable": True}], "WFActionParameterFilterPrefix": 1, "WFContentPredicateBoundedDate": False}}}, "WFTriggerIdentifier": "WFNotificationTrigger", "WFTriggerUUID": str(uuid.uuid4()).upper()}],
        "WFWorkflowMinimumClientVersionString": "900", "WFWorkflowMinimumClientVersion": 900,
        "WFWorkflowClientVersion": "5037.111", "WFWorkflowActions": actions,
        "WFWorkflowIcon": {"WFWorkflowIconStartColor": -20702977, "WFWorkflowIconGlyphNumber": 61440},
        "WFWorkflowTypes": ["WFWorkflowTypeShowInSearch"], "WFWorkflowHasOutputFallback": False,
        "WFWorkflowOutputContentItemClasses": [], "WFWorkflowInputContentItemClasses": [],
        "WFWorkflowHasShortcutInputVariables": False, "WFQuickActionSurfaces": [],
        "WFWorkflowImportQuestions": [{"ParameterKey": "WFTextActionText", "Category": "Parameter", "ActionIndex": code_index, "Text": "Cole somente o seu código individual de gatilho UOL (w1.…). Ele só autoriza avisar sobre o perfil clubeuol. Cada amigo precisa de um código diferente. Não compartilhe uma cópia preenchida.", "DefaultValue": ""}],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "UOL Instagram.unsigned.shortcut")
    parser.add_argument("--uuid-test", action="store_true", help="Produce a local-only UUID rendering check with no trigger, code or HTTP request.")
    args = parser.parse_args()
    workflow = build()
    if args.uuid_test:
        workflow["WFWorkflowName"] = "UOL Instagram UUID test"
        workflow["WFWorkflowTriggers"] = []
        workflow["WFWorkflowImportQuestions"] = []
        workflow["WFWorkflowActions"] = [a for a in workflow["WFWorkflowActions"] if a["WFWorkflowActionIdentifier"] == "is.workflow.actions.number.random" or (a["WFWorkflowActionIdentifier"] == "is.workflow.actions.gettext" and isinstance(a["WFWorkflowActionParameters"].get("WFTextActionText"), dict))]
        workflow["WFWorkflowOutputContentItemClasses"] = ["WFStringContentItem"]
        workflow["WFWorkflowHasOutputFallback"] = True
    args.output.write_bytes(plistlib.dumps(workflow, fmt=plistlib.FMT_BINARY, sort_keys=False))
    print(f"Generated {args.output.name}: {len(workflow['WFWorkflowActions'])} actions; no credentials.")


if __name__ == "__main__":
    main()
