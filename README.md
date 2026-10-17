# Rust S3 Upload Service

A small Axum service that streams raw HTTP request bodies into a private S3 bucket.

## Endpoints

- `GET /health` — liveness check.
- `PUT /v1/files?filename=report.pdf` — upload raw file bytes.

The response includes the opaque S3 object key. The service does not make the bucket public and does not return a public URL.

## Run locally

Install Rust and configure AWS credentials with an identity that can write objects to the target bucket.

```bash
cp .env.example .env
# Edit .env and export the values in your shell:
set -a
. ./.env
set +a
cargo run
```

Example upload:

```bash
curl -i -X PUT \
  'http://localhost:3000/v1/files?filename=hello.txt' \
  -H 'Content-Type: text/plain' \
  --data-binary @hello.txt
```

Check health:

```bash
curl http://localhost:3000/health
```

## AWS permissions

Attach a least-privilege policy to the service's IAM role (or use equivalent credentials locally). Replace `YOUR_BUCKET`:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject", "s3:HeadObject"],
    "Resource": "arn:aws:s3:::YOUR_BUCKET/uploads/*"
  }]
}
```

Keep S3 Block Public Access enabled. Use bucket default encryption (SSE-S3 or SSE-KMS) and TLS at the load balancer / reverse proxy.

## Configuration

- `S3_BUCKET` — required bucket name.
- `AWS_REGION` — AWS region (default `us-east-1`).
- `BIND_ADDR` — listen address (default `0.0.0.0:3000`).
- `MAX_UPLOAD_BYTES` — maximum accepted upload size (default 100 MiB).
- `RUST_LOG` — tracing filter.

AWS SDK credential resolution is used. In production, prefer an IAM role or workload identity over long-lived access keys.

## Important production follow-ups

This is a minimal starting point, not a complete multi-tenant file platform. Before public deployment, add authentication and authorization, per-user quotas/rate limits, malware scanning where appropriate, and structured audit logs. Restrict who can read/delete each object. Put the service behind HTTPS and configure request timeouts and limits at the proxy. If clients need large files (over S3's single `PutObject` limits) use S3 multipart upload or presigned multipart uploads.
