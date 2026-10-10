// G3 lab: offline port of the native OpenCode v2.0.26 provider-policy preflight hook.
//
// Source SHA: anomalyco/opencode@9b4ec5714d481559990db0a816d5dec19541a814
// Reference files:
//   - packages/core/src/util/wildcard.ts            (Wildcard.match)
//   - packages/core/src/managed-policy.ts           (decision, statements)
//   - packages/core/src/config/plugin/policy.ts     (provider catalog transform)
//
// Scope: only the synchronous "provider.use" preflight that decides whether the
// provider plugin may remain in the catalog. The plugin runs at config load time,
// BEFORE any HTTP/WS handler can reach the upstream. This file ports that
// decision logic verbatim so we can prove the rejection path in isolation.
//
// What is intentionally NOT ported:
//   - Effect service wiring, PubSub/Stream, real ConfigEntryObserver
//   - tool.use / permission.evaluate hook
//   - skill/integration catalog transforms
//   - MCP catalog transforms
//
// The lab does not call any provider, network, or external service. It only
// exercises the decision function and the catalog transform.

const Win = process.platform === "win32" ? "si" : "s"

// Verbatim from packages/core/src/util/wildcard.ts at the SHA above.
export function wildcardMatch(input, pattern) {
  const normalized = input.replaceAll("\\", "/")
  let escaped = pattern
    .replaceAll("\\", "/")
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*/g, ".*")
    .replace(/\?/g, ".")

  if (escaped.endsWith(" .*")) escaped = escaped.slice(0, -3) + "( .*)?"

  return new RegExp("^" + escaped + "$", Win).test(normalized)
}

// Minimal Provider.Info shape mirrored from packages/core/src/provider.ts
// only to the depth the policy transform reads. See config/plugin/policy.ts
// which iterates `providers.list()` and reads `record.provider.id`.
export function createProviderCatalog(providers) {
  const map = new Map()
  for (const p of providers) {
    map.set(p.id, { id: p.id, transport: p.transport ?? "http", label: p.label ?? p.id })
  }
  return {
    list() { return Array.from(map.values()) },
    get(id) { return map.get(id) },
    has(id) { return map.has(id) },
    size() { return map.size },
    snapshot() { return Array.from(map.keys()).slice().sort() },
    remove(id) { return map.delete(id) },
  }
}

// Verbatim port of ManagedPolicy.decision from
// packages/core/src/managed-policy.ts. Default effect is "allow" so providers
// remain reachable unless a statement denies them.
export function decision(policies, action, resource) {
  return (
    policies.findLast(
      (policy) => policy.action === action && wildcardMatch(resource, policy.resource),
    )?.effect ?? "allow"
  )
}

// Verbatim port of ManagedPolicy.statements from the same file. Document
// statements reverse so user-global outranks repository; managed statements
// are appended last with their own message.
export function buildStatements(authoredEntries, managed) {
  const out = []
  // Reverse authored documents first, then append organization-managed.
  for (let i = authoredEntries.length - 1; i >= 0; i--) {
    const policies = authoredEntries[i].info?.experimental?.policies ?? []
    for (const policy of policies) {
      out.push({
        action: policy.action,
        resource: policy.resource,
        effect: policy.effect,
        message: "Blocked by configuration policy",
      })
    }
  }
  for (const policy of managed.statements) {
    out.push({
      action: policy.action,
      resource: policy.resource,
      effect: policy.effect,
      message: managed.organization
        ? `Blocked by ${managed.organization}'s policy`
        : "Blocked by your organization's policy",
    })
  }
  return out
}

// Port of the provider catalog transform in packages/core/src/config/plugin/policy.ts
// (opencode.config.policy plugin). This is the synchronous "preflight" the spec
// calls out: providers whose final matching statement is "deny" are removed from
// the catalog before any handler is constructed. Returns {catalog, denied}.
//
// We do not port the mcp/skill/permission transforms here; the test scope is
// only the provider path the user asked about.
export function applyProviderPreflight({ providers, authoredEntries, managed }) {
  const policies = buildStatements(authoredEntries, managed)
  const catalog = createProviderCatalog(providers)
  const denied = []
  for (const record of catalog.list()) {
    if (decision(policies, "provider.use", record.id) === "deny") {
      catalog.remove(record.id)
      denied.push(record.id)
    }
  }
  return { catalog, denied, policies }
}

// Fake request admission. A real handler module would call
// `applyProviderPreflight(...)` once and only build transport for catalog
// members. We mirror that boundary so the test can prove no transport is built
// when the catalog is empty for the requested provider.
export function tryAdmitRequest({ catalog, providerId }) {
  if (!catalog.has(providerId)) {
    return { admitted: false, reason: "denied_by_preflight" }
  }
  return { admitted: true }
}