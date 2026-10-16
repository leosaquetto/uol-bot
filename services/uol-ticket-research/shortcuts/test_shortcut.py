import plistlib
import re
import unittest

from criar_atalho import URL, build


class ShortcutTests(unittest.TestCase):
    def setUp(self):
        self.workflow = plistlib.loads(plistlib.dumps(build()))
        self.actions = self.workflow["WFWorkflowActions"]

    def test_original_notification_trigger_and_empty_scoped_code_import_question(self):
        trigger, = self.workflow["WFWorkflowTriggers"]
        self.assertEqual(trigger["WFTriggerIdentifier"], "WFNotificationTrigger")
        parameters = trigger["WFTriggerSerializedParameters"]
        self.assertEqual(parameters["SelectedApps"]["BundleIdentifier"], "com.burbn.instagram")
        filters = parameters["Conditions"]["Value"]["WFActionParameterFilterTemplates"]
        self.assertEqual(filters, [{"Property": "Title", "Operator": 0, "Values": {"Title": "clubeuol"}, "Removable": True}])
        question, = self.workflow["WFWorkflowImportQuestions"]
        action = self.actions[question["ActionIndex"]]
        self.assertEqual(question["ParameterKey"], "WFTextActionText")
        self.assertEqual(question["DefaultValue"], "")
        self.assertEqual(action["WFWorkflowActionParameters"]["WFTextActionText"], "")

    def test_only_http_call_is_fixed_wake_post_with_json_and_bearer_variable(self):
        calls = [a["WFWorkflowActionParameters"] for a in self.actions if a["WFWorkflowActionIdentifier"] == "is.workflow.actions.downloadurl"]
        call, = calls
        self.assertEqual(call["WFURL"], URL)
        self.assertEqual(call["WFHTTPMethod"], "POST")
        self.assertEqual(call["WFHTTPBodyType"], "JSON")
        headers = {i["WFKey"]["Value"]["string"]: i["WFValue"] for i in call["WFHTTPHeaders"]["Value"]["WFDictionaryFieldValueItems"]}
        self.assertEqual(headers["Authorization"]["Value"]["string"], "Bearer \ufffc")
        attachment, = headers["Authorization"]["Value"]["attachmentsByRange"].values()
        code = self.actions[self.workflow["WFWorkflowImportQuestions"][0]["ActionIndex"]]
        self.assertEqual(attachment["OutputUUID"], code["WFWorkflowActionParameters"]["UUID"])
        fields = {i["WFKey"]["Value"]["string"]: i["WFValue"]["Value"] for i in call["WFJSONValues"]["Value"]["WFDictionaryFieldValueItems"]}
        self.assertEqual(set(fields), {"eventId", "profile", "source", "observedAt"})
        self.assertEqual(fields["profile"]["string"], "clubeuol")
        self.assertEqual(fields["source"]["string"], "ios_shortcuts")

    def test_verified_random_and_text_actions_produce_v4_shape(self):
        values = {}
        random_count = 0
        for action in self.actions:
            p = action["WFWorkflowActionParameters"]
            identifier = action["WFWorkflowActionIdentifier"]
            if identifier == "is.workflow.actions.number.random":
                random_count += 1
                self.assertLessEqual(p["WFRandomNumberMaximum"], 2147483647)
                values[p["UUID"]] = str(p["WFRandomNumberMinimum"])
            if identifier == "is.workflow.actions.gettext" and isinstance(p["WFTextActionText"], dict):
                token = p["WFTextActionText"]["Value"]
                result = token["string"]
                # Insert from right so earlier indices remain stable.
                for position, attachment in sorted(token["attachmentsByRange"].items(), key=lambda item: int(item[0].split(",")[0][1:]), reverse=True):
                    offset = int(position.split(",")[0][1:])
                    result = result[:offset] + values[attachment["OutputUUID"]] + result[offset + 1:]
                self.assertRegex(result, r"^[0-9]{8}-[0-9]{4}-4[0-9]{3}-8[0-9]{3}-[0-9]{12}$")
        self.assertEqual(random_count, 6)


if __name__ == "__main__":
    unittest.main()
