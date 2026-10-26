# Security Policy

AgentSpaces Connect :: celld holds an operator's credential for a celld or
Workers deployment and calls that deployment's routes on behalf of fleet
members. Its job is to let agents reach exactly the routes and parameters the
operator's manifest allows, under the fleet's scopes, and nothing else. It
inherits the security model of the AgentSpaces core (signed, self-certifying
peers and agents; the threat model in the AgentSpaces specification, SPEC §11).

## Reporting a vulnerability

Please report vulnerabilities **privately** via GitHub's security advisories:

> https://github.com/badmonkeyai/agentspaces-connect-celld/security/advisories/new

A vulnerability in the connector SPI, the claim lattice for invocations, or
the fleet's authorizers belongs in the core repository's advisories:
https://github.com/badmonkeyai/agentspaces/security/advisories/new

Do not open public issues for suspected vulnerabilities, and do not include
exploit details in public discussions until a fix is released.

We will acknowledge your report within five business days, keep you informed of
progress, credit you in the advisory unless you prefer otherwise, and
coordinate the disclosure timeline with you. There is currently no bug bounty
program.

Questions can be sent to `oss [at] badmonkey.ai`

## Scope

In scope: anything in this repository that lets an attacker get past the
manifest or the fleet's scopes. For example:
- a request that reaches a route, operation, path, query parameter, header, or
  body field the manifest did not declare;
- a requester without `aspace:connector-invoke:<asset>:<operation>` whose
  invocation reaches the deployment;
- the operator's bearer token or client key reaching an agent, a card, a
  result, an error message, a log line, or a host other than the manifest's
  base URL;
- an invocation that runs a route twice under one invocation id while the
  connector remembers the first result;
- a response from the deployment that crashes the connector, exhausts its
  memory past `max-body-bytes`, or holds it past `timeouts.request`;
- a manifest setting that silently weakens transport security, such as an
  `http://` base URL accepted without `allow-plaintext`.

The connector trusts its operator: whoever controls its manifest, its keystore,
and its credential controls what it serves. What a route does with a request it
was allowed to receive is the deployment's own concern.

## Supported versions

During 0.x, only the latest minor release receives security fixes.
