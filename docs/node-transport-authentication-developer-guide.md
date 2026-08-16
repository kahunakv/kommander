# Node Transport Authentication — Developer Guide

This guide explains how Kommander authenticates **node-to-node** traffic: the three modes, what each one
actually proves, the properties they do *not* provide, and how to operate them. It is written for two
readers — an operator choosing a mode and rotating its credentials, and a developer extending the
transport layer. It covers the REST and gRPC transports together, because they share one implementation
and must not drift.

The short version: `SharedSecret` authenticates *membership in the cluster*; `MutualTls` authenticates
*which node* is calling. If you need to tell your own nodes apart from each other, you need `MutualTls`.

---

## Table of contents

1. [Summary](#summary)
2. [The three modes](#the-three-modes)
3. [What shared-secret mode does and does not prove](#what-shared-secret-mode-does-and-does-not-prove)
4. [What a signature covers](#what-a-signature-covers)
5. [Mutual TLS and the pinning model](#mutual-tls-and-the-pinning-model)
6. [Transport security is separate from node authentication](#transport-security-is-separate-from-node-authentication)
7. [Configuration](#configuration)
8. [Rotating credentials](#rotating-credentials)
9. [Fail-closed startup checks](#fail-closed-startup-checks)
10. [Snapshot integrity](#snapshot-integrity)
11. [Code map](#code-map)
12. [Glossary](#glossary)

---

## Summary

Every inbound Raft request — REST or gRPC — passes through one authentication decision before it reaches
the state machine. Three modes are available:

| Mode | What it proves | Credential |
|---|---|---|
| `Disabled` | Nothing. Any host that can reach the port can drive consensus. | none |
| `SharedSecret` | The caller holds the cluster secret. | one symmetric secret, cluster-wide |
| `MutualTls` | The caller holds a specific trusted private key. | per-node certificate |

`Disabled` is the default, and the server **refuses to start** in that mode unless it is bound to
loopback or the operator passes `--allow-unauthenticated-cluster`. That refusal exists because the
unauthenticated surface is not a read-only one: it includes log append, snapshot install, vote, and
membership change.

---

## The three modes

### Disabled

No check is performed. Suitable only for a single-host development cluster on loopback.

### SharedSecret

Every request carries four headers — a signature, the sending node's name, a timestamp, and a nonce.
The signature is an HMAC-SHA256 over the request method, path (or gRPC method), sender, timestamp,
nonce, and a digest of the body, keyed by the shared secret.

The receiver recomputes the signature and compares it in constant time, then checks that the timestamp
is within the allowed clock skew and that the nonce has not been seen before. The nonce is recorded
**only after the signature verifies**, so unauthenticated traffic cannot grow the replay cache.

### MutualTls

Authentication happens during the TLS handshake: each node presents a client certificate, and the
receiver checks it against a thumbprint allow-list and its validity window. No per-request signature is
sent or expected — there is nothing to sign, because the identity is a property of the connection.

---

## What shared-secret mode does and does not prove

This is the most important thing to understand about `SharedSecret`, and the reason it is not simply
"the easy mTLS".

There is **one secret for the whole cluster**. Every node signs with it and verifies with it. The
sending node's name travels as a header and is covered by the signature — but it is a *claim*, not a
proof. Any holder of the secret can put any name there.

The consequences:

- **A compromised node can impersonate any other node.** It can cast votes as a peer, fabricate
  acknowledgements attributed to a peer, and send gossip claiming to originate from one.
- **Revoking one node means rotating the cluster.** There is no per-node credential to remove.
- **The mode answers "is this from inside the cluster?", not "which node is this?"**

None of this is a defect in the HMAC construction, which is sound. It is inherent to a symmetric
cluster-wide key. If your threat model includes one node being compromised, use `MutualTls`, where each
node holds a distinct private key and can be individually removed from every peer's allow-list.

---

## What a signature covers

A `SharedSecret` signature binds:

```
method \n path-or-grpc-method \n sender \n timestamp \n nonce \n SHA256(body)
```

Both transports bind their body. REST signs the raw JSON payload; gRPC signs the serialized protobuf
message. The digests necessarily differ between transports for the same logical request — they are
different encodings — but neither leaves the payload unsigned. An on-path attacker cannot alter a
request's contents and keep its signature valid.

**One exception, by design.** The gRPC duplex `BatchRequests` stream is authenticated once, when the
stream is established, before any message is read. There is no single request message to bind, so the
per-message integrity of that stream rests on the transport (TLS) rather than on the signature. This is
the hot replication path; if you require per-message cryptographic integrity there, run with TLS
enforced, which is the default.

---

## Mutual TLS and the pinning model

`MutualTls` validates peers by **SHA-256 thumbprint pinning** plus the certificate's validity window.
It deliberately does **not** build or verify a certificate chain, and it does not check revocation
lists. That is a coherent model for a private cluster of self-signed per-node certificates, which is
the deployment it targets — but be clear about what it implies:

- **Revocation is unobservable.** A compromised certificate stays trusted until its thumbprint is
  removed from every peer's allow-list.
- **Removal requires a restart.** The presented certificate is loaded once and cached for the process
  lifetime, so rotating it is a restart-and-redeploy operation.
- **There is no CA trust mode.** A certificate signed by your PKI is not trusted unless its thumbprint
  is listed.

Both directions are checked symmetrically: an expired *client* certificate is rejected by the server,
and an expired *server* certificate is rejected by the client, even when the thumbprint matches.

Thumbprints may be pasted in the format `openssl x509 -fingerprint -sha256` emits (colon-separated,
any case); separators and case are normalized before comparison. Note that this is the SHA-256
thumbprint, **not** the SHA-1 value shown by some tools and by `X509Certificate2.Thumbprint`.

---

## Transport security is separate from node authentication

TLS and node authentication answer different questions, and both matter:

- **TLS** protects confidentiality and integrity on the wire and validates the *server* certificate.
- **Node authentication** decides whether an authenticated *peer* may drive this node's consensus.

`RequireTls` (default **on**) makes the two interlock: in `SharedSecret` mode a signed request arriving
over cleartext is rejected outright, because a valid signature over an observable channel still leaks
the payload. Leave it on unless you have a specific reason not to.

The development flag that disables peer certificate validation applies **only to Kommander's own
cluster clients** — it does not alter certificate validation for any other HTTP client in the process.
It cannot be combined with `MutualTls`, and that combination is rejected at startup.

---

## Configuration

| Setting | Default | Notes |
|---|---|---|
| `TransportSecurity.NodeAuthenticationMode` | `Disabled` | `Disabled`, `SharedSecret`, `MutualTls` |
| `TransportSecurity.SharedSecret` | none | required in `SharedSecret` mode |
| `TransportSecurity.RequireTls` | `true` | rejects cleartext when authentication is enabled |
| `TransportSecurity.AllowedClockSkew` | 60 s | bounds timestamp drift and the replay window |
| `TransportSecurity.HeaderName` | `X-Kommander-Cluster-Auth` | signature header |
| `TransportSecurity.TrustedClientCertificateThumbprints` | empty | required in `MutualTls` mode |
| `TransportSecurity.TrustedServerCertificateThumbprints` | empty | pins the peers this node dials |
| `TransportSecurity.ClientCertificatePath` / `Password` | none | certificate presented in `MutualTls` |
| `MaxPreAuthRequestBodyBytes` | 32 MiB | ceiling on the REST body read before a signature is verified |

Server command-line equivalents: `--node-auth-mode`, `--node-shared-secret`, `--node-auth-header`,
`--trusted-client-cert-thumbprint`, `--trusted-server-cert-thumbprint`, `--client-certificate`,
`--client-certificate-password`, `--allow-insecure-certificate-validation`.

Clock skew is a real operational dependency: nodes whose clocks differ by more than `AllowedClockSkew`
will reject each other's requests with a timestamp error. Run NTP.

---

## Rotating credentials

### The shared secret

`HttpAuthBearerToken` is a **legacy alias for the same credential**. When `SharedSecret` is empty, the
bearer token is used as the shared secret. They are not two independent credentials, and rotating only
one of them may leave the other still accepted, or rotate a value you did not realize was live. When
in doubt, set `SharedSecret` explicitly — it takes precedence — and leave the bearer token unset.

Rotating the secret is a cluster-wide operation: signatures are verified with a single key, so nodes
holding the old and new secret cannot authenticate each other. Plan a maintenance window, or move to
`MutualTls`, where credentials are per node and can be rolled one at a time.

### Certificates

1. Issue the new certificate and add its thumbprint to every peer's
   `TrustedClientCertificateThumbprints` (and `TrustedServerCertificateThumbprints`, if the node is
   dialed).
2. Restart peers so the new allow-list is loaded.
3. Switch the rotating node to the new certificate and restart it.
4. Remove the old thumbprint from every peer and restart them.

The allow-list holding both thumbprints during the overlap is what makes this rollable one node at a
time.

---

## Fail-closed startup checks

The server refuses to start rather than run in a configuration that looks secure but is not:

- authentication `Disabled` on a non-loopback bind, without `--allow-unauthenticated-cluster`;
- `MutualTls` with no trusted client certificate thumbprint — an empty allow-list would trust every
  certificate that completes the handshake;
- `MutualTls` combined with the insecure-certificate-validation flag;
- `MutualTls` without an HTTPS certificate;
- `SharedSecret` without a secret;
- a cleartext listener requested alongside `MutualTls`.

The cleartext HTTP listener is not bound at all when an HTTPS certificate is configured, unless
`--allow-plaintext-listener` is passed. `--host` controls the bind address; use `--host localhost` to
keep a development node off the network.

---

## Snapshot integrity

A snapshot install writes bytes straight into the application state machine and seeds a WAL checkpoint,
which makes it the highest-leverage message in the protocol. Independently of transport security, the
sender computes a SHA-256 over the whole snapshot and the receiver verifies it against what it actually
staged before anything is imported. A mismatch — tampering, or a truncated transfer that still satisfied
the chunk-ordering rules — rejects the install.

Senders that predate this field are refused unless `AllowLegacySnapshotSenders` is set, which exists as
a rolling-upgrade window and should be turned back off afterwards.

---

## Code map

| Concern | Type |
|---|---|
| Signing and verification | `RaftTransportAuthenticator` |
| Settings | `RaftTransportSecurityOptions` |
| Certificate trust decisions (both directions) | `RaftClientCertificateValidator` |
| REST server-side enforcement | `RestCommunicationExtensions` |
| gRPC server-side enforcement | `RaftService.ValidateAuth` |
| gRPC body digest | `GrpcMessageBodyHash` |
| Startup policy | `KommanderServerBindingPolicy` |

Both transports route their trust decisions through the shared validator on purpose: a divergence
between them would be invisible to a test that exercises only one transport, while leaving the other
permanently more permissive.

---

## Glossary

**Nonce** — a single-use random value that makes each signature unique, so a captured request cannot be
replayed.

**Replay cache** — the record of recently seen nonces, keyed per sender and bounded by the allowed clock
skew.

**Thumbprint** — a hash of a certificate's DER encoding, used here as its identity. Kommander uses
SHA-256.

**Pinning** — trusting a specific certificate by its thumbprint, rather than trusting whatever a
certificate authority has signed.
