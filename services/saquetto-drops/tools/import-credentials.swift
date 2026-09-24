import Foundation
import Security

// No secrets in argv or stdout. The existing download remains private and unchanged.
guard CommandLine.arguments.count == 2 else { exit(2) }
do {
    let path = CommandLine.arguments[1]
    let lines = try String(contentsOfFile: path, encoding: .utf8)
        .split(whereSeparator: \.isNewline).map { String($0).trimmingCharacters(in: .whitespaces) }
        .filter { !$0.isEmpty }
    guard lines.count == 2 else { exit(3) }
    let data = try JSONSerialization.data(withJSONObject: ["login": lines[0], "password": lines[1]])
    let query: [String: Any] = [kSecClass as String: kSecClassGenericPassword,
        kSecAttrService as String: "saquetto-drops-x", kSecAttrAccount as String: "automation"]
    var item = query
    item[kSecValueData as String] = data
    var result = SecItemAdd(item as CFDictionary, nil)
    if result == errSecDuplicateItem {
        result = SecItemUpdate(query as CFDictionary, [kSecValueData as String: data] as CFDictionary)
    }
    guard result == errSecSuccess else { print("keychain_import_failed"); exit(4) }
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: path)
    print("keychain_imported")
} catch { print("credential_import_failed"); exit(5) }
