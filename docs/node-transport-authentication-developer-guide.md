# Node Transport Authentication — Developer Guide

This guide covers authentication of **node-to-node** traffic over REST and gRPC, for operators who
choose a mode and rotate credentials, and for developers who extend the transport layer.

`SharedSecret` authenticates *membership in the cluster*. `MutualTls` authenticates *which node* is
calling. If you need to tell your own nodes apart, use `MutualTls`.

---

## Table of contents

1. [Summary](#summary)
2. [The three modes](#the-three-modes)
3. [Shared-secret mode proves cluster membership only](#shared-secret-mode-proves-cluster-membership-only)
4. [What a signature covers](#what-a-signature-covers)
5. [Mutual TLS and the pinning model](#mutual-tls-and-the-pinning-model)
6. [Transport security is separate from node authentication](#transport-security-is-separate-from-node-authentication)
7. [Configuration](#configuration)
8. [Rotating credentials](#rotating-credentials)
9. [Fail-closed startup checks](#fail-closed-startup-checks)
10. [Snapshot integrity](#snapshot-integrity)
11. [Code map](#code-map)

---

## Summary

Each inbound Raft request, REST or gRPC, is authenticated before it reaches the state machine.

| Mode | What it proves | Credential |
|---|---|---|
| `Disabled` | Any host that can reach the port can drive consensus. | none |
| `SharedSecret` | The caller holds the cluster secret. | one symmetric secret, cluster-wide |
| `MutualTls` | The caller holds a specific trusted private key. | per-node certificate |

`Disabled` is the default. In that mode the server **does not start** unless it is bound to loopback
or the operator passes `--allow-unauthenticated-cluster`. The unauthenticated surface includes log
append, snapshot install, vote, and membership change.

---

## The three modes

### Disabled

No check is performed. Use it only for a single-host development cluster on loopback.

### SharedSecret

Each request carries four headers: a signature, the name of the sending node, a timestamp, and a
single-use random nonce. The signature is an HMAC-SHA256 that is keyed by the shared secret.

The receiver recomputes the signature and compares it in constant time. It then checks that the
timestamp is within the allowed clock skew and that the nonce is new. The nonce is recorded **only
after the signature verifies**. This means that unauthenticated traffic cannot grow the replay cache,
which is keyed per sender and bounded by the allowed clock skew.

### MutualTls

Each node presents a client certificate in the TLS handshake. The receiver checks it against a
thumbprint allow-list and its validity window. A thumbprint is the SHA-256 hash of the DER encoding of
the certificate. Requests carry no signature because the identity is a property of the connection.

---

## Shared-secret mode proves cluster membership only

All nodes sign and verify with **one secret**. The name of the sending node is covered by the
signature, but it is a *claim*: any holder of the secret can put any name there.

- **A compromised node can impersonate any other node.** It can cast votes as a peer, fabricate
  acknowledgements from a peer, and send gossip that claims to come from one.
- **To revoke one node, you must rotate the secret of the cluster.** There is no per-node credential
  to remove.

If your threat model includes one compromised node, use `MutualTls`. Each node then has a distinct
private key that you can remove from the allow-list of each peer.

---

## What a signature covers

A `SharedSecret` signature is an HMAC over these fields.

```
method \n path-or-grpc-method \n sender \n timestamp \n nonce \n SHA256(body)
```

REST signs the raw JSON payload, and gRPC signs the serialized protobuf message. An on-path attacker
cannot alter the contents of a request and leave its signature valid.

**The gRPC duplex `BatchRequests` stream is the exception.** It is authenticated once, when the stream
is established and before any message is read. The per-message integrity of this hot replication path
depends on TLS. If you require per-message cryptographic integrity there, enforce TLS, which is the
default.

---

## Mutual TLS and the pinning model

`MutualTls` validates peers by **SHA-256 thumbprint pinning** and the validity window of the
certificate. It does **not** build or verify a certificate chain, and it does not check revocation
lists. The model suits a private cluster of self-signed per-node certificates.

- **Revocation is not visible.** A compromised certificate is trusted until you remove its thumbprint
  from the allow-list of each peer.
- **Rotation needs a restart.** The presented certificate is loaded once and cached for the process
  lifetime.
- **There is no CA trust mode.** A certificate that your PKI signed is trusted only if its thumbprint
  is listed.

Both directions are checked. The server denies an expired *client* certificate and the client denies
an expired *server* certificate, even when the thumbprint matches.

You can paste thumbprints in the format of `openssl x509 -fingerprint -sha256`. Separators and case
are normalized before comparison. Use the SHA-256 thumbprint: `X509Certificate2.Thumbprint` and some
tools show a SHA-1 value, which does not match.

---

## Transport security is separate from node authentication

- **TLS** protects confidentiality and integrity on the wire and validates the *server* certificate.
- **Node authentication** decides whether an authenticated *peer* can drive the consensus of this node.

With `RequireTls` (default **on**), a signed request that arrives over cleartext in `SharedSecret` mode
is denied. A valid signature over an observable channel would leak the payload.

The development flag that disables peer certificate validation applies **only to the cluster clients of
Kommander**. Other HTTP clients in the process are not affected. The server does not start if you
combine this flag with `MutualTls`.

---

## Configuration

| Setting | Default | Notes |
|---|---|---|
| `TransportSecurity.NodeAuthenticationMode` | `Disabled` | `Disabled`, `SharedSecret`, `MutualTls` |
| `TransportSecurity.SharedSecret` | none | required in `SharedSecret` mode |
| `TransportSecurity.RequireTls` | `true` | denies cleartext when authentication is enabled |
| `TransportSecurity.AllowedClockSkew` | 60 s | bounds timestamp drift and the replay window |
| `TransportSecurity.HeaderName` | `X-Kommander-Cluster-Auth` | signature header |
| `TransportSecurity.TrustedClientCertificateThumbprints` | empty | required in `MutualTls` mode |
| `TransportSecurity.TrustedServerCertificateThumbprints` | empty | pins the peers this node dials |
| `TransportSecurity.ClientCertificatePath` / `Password` | none | certificate presented in `MutualTls` |
| `MaxPreAuthRequestBodyBytes` | 32 MiB | ceiling on the REST body read before a signature is verified |

Server command-line equivalents: `--node-auth-mode`, `--node-shared-secret`, `--node-auth-header`,
`--trusted-client-cert-thumbprint`, `--trusted-server-cert-thumbprint`, `--client-certificate`,
`--client-certificate-password`, `--allow-insecure-certificate-validation`.

Nodes whose clocks differ by more than `AllowedClockSkew` will deny each other's requests with a
timestamp error. Use NTP.

---

## Rotating credentials

### The shared secret

`HttpAuthBearerToken` is a **legacy alias for the same credential**. When `SharedSecret` is empty, the
bearer token is used as the shared secret. If you rotate only one of them, the other can still be
valid. Set `SharedSecret` explicitly, which takes precedence, and leave the bearer token unset.

Signatures are verified with a single key, and nodes that hold the old and the new secret cannot
authenticate each other. Plan a maintenance window, or move to `MutualTls`, where you can roll
credentials one node at a time.

### Certificates

1. Issue the new certificate. Add its thumbprint to `TrustedClientCertificateThumbprints` on each peer
   (and to `TrustedServerCertificateThumbprints`, if the node is dialed).
2. Restart the peers to load the new allow-list.
3. Switch the rotating node to the new certificate and restart it.
4. Remove the old thumbprint from each peer and restart them.

The allow-list must contain both thumbprints during the overlap.

---

## Fail-closed startup checks

The server does not start in these insecure configurations.

- Authentication is `Disabled` on a non-loopback bind and `--allow-unauthenticated-cluster` is absent.
- `MutualTls` has an empty list of trusted client certificate thumbprints. An empty allow-list would
  trust any certificate that completes the handshake.
- `MutualTls` is combined with the insecure-certificate-validation flag.
- `MutualTls` is set and no HTTPS certificate is configured.
- `SharedSecret` is set and the secret is missing.
- A cleartext listener is requested alongside `MutualTls`.

When an HTTPS certificate is configured, the cleartext HTTP listener is not bound unless you pass
`--allow-plaintext-listener`. Use `--host localhost` to limit a development node to loopback.

---

## Snapshot integrity

A snapshot install writes bytes into the application state machine and seeds a WAL checkpoint.
Independently of transport security, the sender computes a SHA-256 over the full snapshot. The receiver
verifies it against the staged bytes before the import. The install fails on a mismatch, which can come
from tampering or from a truncated transfer that satisfied the chunk-ordering rules.

Senders that predate this field are denied unless `AllowLegacySnapshotSenders` is set. Set it for a
rolling upgrade, and turn it off afterwards.

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

Both transports must use the shared validator for trust decisions. A divergence would leave one
transport permanently more permissive, and a test of the other transport would not detect it.
