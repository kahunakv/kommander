# Development TLS Certificates

These certificates are used by Kommander nodes for HTTPS and gRPC transport in local development and Docker Compose deployments. They are self-signed and must not be used in production.

## Files

| File | Description |
|------|-------------|
| `san.cnf` | OpenSSL config with Subject Alternative Names for all nodes |
| `development-private.key` | RSA private key (2048-bit) |
| `development-certificate.csr` | Certificate signing request |
| `development-certificate.crt` | Self-signed certificate (PEM) |
| `development-certificate.pfx` | PKCS#12 bundle loaded by Kestrel (no password) |

## Subject Alternative Names

`san.cnf` covers all endpoints used in Docker Compose and local development:

| Type | Value |
|------|-------|
| DNS | `localhost` |
| DNS | `node1`, `node2`, `node3` |
| IP  | `172.20.0.2`, `172.20.0.3`, `172.20.0.4` |
| IP  | `127.0.0.1` |

If you change node hostnames or IPs in `docker-compose.yml`, update `san.cnf` and regenerate.

## Generate (or regenerate) the certificate

Run all four commands from this directory:

```sh
openssl genrsa -out development-private.key 2048

openssl req -new -nodes -config san.cnf -keyout development-private.key -out development-certificate.csr

openssl x509 -req -in development-certificate.csr -signkey development-private.key \
  -out development-certificate.crt -days 365 -extensions req_ext -extfile san.cnf

openssl pkcs12 -export -out development-certificate.pfx \
  -inkey development-private.key -in development-certificate.crt -passout pass:
```

Then rebuild the Docker images so the new `.pfx` is baked in:

```sh
docker compose build
```

## Verify the certificate SANs

```sh
openssl x509 -in development-certificate.crt -text -noout | grep -A5 "Subject Alternative Name"
```

Expected output includes all configured DNS names and IP addresses.

## Trust the certificate (macOS)

Required if you want browsers or local tooling to accept the certificate without warnings:

```sh
sudo security add-trusted-cert -d -r trustRoot \
  -k /Library/Keychains/System.keychain development-certificate.crt
```

## Certificate validation in Docker

The nodes connect to each other using their container IPs. Because the certificate is self-signed (no trusted CA), gRPC channel validation must be relaxed via `--allow-insecure-certificate-validation true` in each node's configuration. This is already set in `docker-compose.yml` via the `KOMMANDER_ALLOW_INSECURE_CERT` build arg.

This flag is safe for development. Remove it (or set it to `false`) when switching to a CA-signed certificate.
