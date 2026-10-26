# Contributing to AgentSpaces Connect :: celld

Thank you for your interest in contributing. AgentSpaces Connect :: celld is an
Apache 2.0 project from Bad Monkey, Inc., and everything in this repository
stays Apache 2.0.

This repository holds a Java connector that puts a celld or Workers deployment
on an AgentSpaces fleet from outside it: an operator's manifest of allowlisted
routes becomes `workers://` AssetCards, GET routes answer data queries, and
POST routes answer asset invocations. The connector SPI and its protocols live
in the [AgentSpaces core repository](https://github.com/badmonkeyai/agentspaces)
(`agentspaces-connect-core`); the plain-Java peer it starts lives in
[agentspaces-langchain4j](https://github.com/badmonkeyai/agentspaces-langchain4j)
(`agentspaces-runtime`).

## Before you start

- **Questions and ideas** are welcome as [GitHub issues](https://github.com/badmonkeyai/agentspaces-connect-celld/issues).
- **Security issues** must not be filed as public issues. See
  [SECURITY.md](SECURITY.md) for private reporting.
- **Code of conduct:** by participating you agree to the
  [code of conduct](CODE_OF_CONDUCT.md).

## The rules a change must keep

- **The manifest is the allowlist.** The connector calls only the routes the
  manifest lists and sends only the parameters each operation declares. A
  change that lets a request choose a path, a header, or a parameter the
  manifest did not name does not belong here.
- **Refuse before calling.** An unlisted route, an unlisted operation, an
  undeclared parameter, or a missing scope is answered with an error naming
  its rule, and the deployment never sees the request. Every refusal has a
  test that proves the stub Worker received nothing.
- **The credential stays with the connector.** The operator's bearer token or
  client certificate goes only to the manifest's base URL. It never appears in
  a card, a result, an error, or a log line.
- **Every bound is a manifest setting.** Timeouts and the body size limit come
  from the manifest, with defaults in `CelldManifest`, and a route that breaks
  one answers an error result naming the setting.
- **No framework.** The enforcer bans Spring and Micronaut from the classpath.
  A host integration belongs in its own project.
- **Nothing new on the wire.** Cards follow the AssetCard fields the
  specification defines. A change that needs a new field or entry type is a
  ticket against the core.

## Building and testing

```bash
mvn -B -ntp verify                                                  # the connector and its tests
mvn -B -ntp -Pcentral-publish -DskipTests -Dgpg.skip=true package   # the release dry run, as CI runs it
```

Java 21 and Maven. The tests stand up a local HTTP server as the Worker
(`StubWorker`) and an in-process fleet, plus one loopback TCP fleet for the
main; they need no other network. Name a test for the sentence it proves, give
every fleet test a timeout, and close the runtime when the test ends.

## Pull requests

Open an issue first for anything beyond a fix. Keep the change small, add the
test that fails without it, and sign off your commits (DCO). CI runs the build
and a dependency scan on every push and pull request.
