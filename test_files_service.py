#!/usr/bin/env python3
"""End-to-end test for the files service (auth, upload, list, download, delete).

Standard library only (works in Termux/Alpine without pip installs).

Usage:
    python3 test_files_service.py --cookie 'session=<uuid>; permission=<uuid>'
    COOKIE='session=...; permission=...' python3 test_files_service.py http://host:8080

With DEV_SEED_SESSION=1 the service prints a ready-made cookie at startup.

Optional:
    --other-cookie '...'   a second user's cookie; also checks that users cannot see
                           each other's files
    --file ./photo.jpg     also round-trip a real file
    --part-size N          the service's PART_SIZE_BYTES (default 8 MiB)
    --limit N              the service's MAX_UPLOAD_BYTES; also checks that limit+1 bytes gets 413
"""
import argparse
import hashlib
import json
import mimetypes
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

TIMEOUT = 60
results = []


def request(method, url, data=None, headers=None):
    """Return (status, headers, body). Never raises on HTTP error statuses."""
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()


def check(name, condition, detail=""):
    results.append(bool(condition))
    print(f"  [{'PASS' if condition else 'FAIL'}] {name}" + (f"  ({detail})" if detail and not condition else ""))
    return condition


def as_json(body):
    try:
        return json.loads(body)
    except ValueError:
        return {}


class Api:
    def __init__(self, base, cookie):
        self.base = base.rstrip("/") + "/api/files/v1/files"
        self.cookie = cookie

    def _h(self, extra=None):
        h = dict(extra or {})
        if self.cookie:
            h["Cookie"] = self.cookie
        return h

    def upload(self, payload, filename, content_type):
        url = f"{self.base}?filename={urllib.parse.quote(filename)}"
        status, _, body = request("PUT", url, payload, self._h({"Content-Type": content_type}))
        return status, as_json(body)

    def list(self, query=""):
        status, _, body = request("GET", f"{self.base}{query}", None, self._h())
        return status, as_json(body)

    def metadata(self, file_id):
        status, _, body = request("GET", f"{self.base}/{file_id}", None, self._h())
        return status, as_json(body)

    def download(self, file_id):
        return request("GET", f"{self.base}/{file_id}/download", None, self._h())

    def delete(self, file_id):
        status, _, _ = request("DELETE", f"{self.base}/{file_id}", None, self._h())
        return status


def roundtrip(api, label, payload, filename, content_type):
    print(f"\n== Round trip: {label} ({len(payload)} bytes) ==")
    status, info = api.upload(payload, filename, content_type)
    if not check("upload returns 201", status == 201, f"got {status}: {info}"):
        return None
    file_id = info.get("id", "")
    check("response has an id", len(file_id) == 36, file_id)
    check("response size_bytes matches", info.get("size_bytes") == len(payload), str(info.get("size_bytes")))
    check("response content_type echoed", info.get("content_type") == content_type, str(info.get("content_type")))
    check("filename sanitised", info.get("filename") == os.path.basename(filename), str(info.get("filename")))
    check("internal key not exposed", "object_key" not in info and "owner_id" not in info)

    status, headers, body = api.download(file_id)
    check("download returns 200", status == 200, f"got {status}")
    check("downloaded bytes identical", hashlib.sha256(body).digest() == hashlib.sha256(payload).digest(),
          f"got {len(body)} bytes")
    check("download Content-Type matches", (headers.get("Content-Type") or "").startswith(content_type),
          str(headers.get("Content-Type")))
    check("download Content-Length matches", headers.get("Content-Length") == str(len(payload)),
          str(headers.get("Content-Length")))
    disposition = headers.get("Content-Disposition") or ""
    check("download is an attachment with filename*", disposition.startswith("attachment") and "filename*=" in disposition,
          disposition)
    return file_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("base_url", nargs="?", default=os.environ.get("BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--cookie", default=os.environ.get("COOKIE", ""))
    parser.add_argument("--other-cookie", default=os.environ.get("OTHER_COOKIE", ""))
    parser.add_argument("--file", help="also upload/download this real file and compare")
    parser.add_argument("--part-size", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    print(f"Testing {base}")

    if not args.cookie:
        print("No cookie given. Pass --cookie 'session=<uuid>; permission=<uuid>' (see DEV_SEED_SESSION=1).")
        return 2
    api = Api(base, args.cookie)

    print("\n== Health ==")
    try:
        status, _, body = request("GET", f"{base}/api/health")
    except Exception as e:
        print(f"  [FAIL] cannot reach service: {e}")
        return 1
    # Informational only: this route is provided by ferrumec's Infra, not by this service.
    print(f"  [INFO] GET /api/health -> {status} {body[:80]!r}")

    print("\n== Authentication ==")
    anon = Api(base, "")
    status, _ = anon.list()
    check("no cookie -> 401", status == 401, f"got {status}")
    status, _ = Api(base, "session=00000000-0000-0000-0000-000000000000; permission=00000000-0000-0000-0000-000000000000").list()
    check("unknown session -> 401/403", status in (401, 403), f"got {status}")
    session_only = "; ".join(p.strip() for p in args.cookie.split(";") if p.strip().startswith("session="))
    status, _ = Api(base, session_only).list()
    check("session without permission cookie -> 401", status == 401, f"got {status}")
    status, _ = anon.upload(b"x", "x.txt", "text/plain")
    check("anonymous upload -> 401", status == 401, f"got {status}")

    created = []
    for label, payload, name, ctype in [
        ("small text", b"hello from the python test\n", "hello.txt", "text/plain"),
        ("1 MiB random binary", os.urandom(1024 * 1024), "random.bin", "application/octet-stream"),
        ("1 byte", b"x", "one-byte.txt", "text/plain"),
        ("path in filename is stripped", b"sneaky", "../../etc/passwd", "text/plain"),
        ("non-ASCII filename", b"unicode name", "résumé 日本.txt", "text/plain"),
        ("exactly one part (single PutObject)", os.urandom(args.part_size), "one-part.bin", "application/octet-stream"),
        ("one part + 1 byte (2-part multipart)", os.urandom(args.part_size + 1), "two-parts.bin", "application/octet-stream"),
        ("2.5 parts (3-part multipart)", os.urandom(args.part_size * 5 // 2), "three-parts.bin", "application/octet-stream"),
    ]:
        file_id = roundtrip(api, label, payload, name, ctype)
        if file_id:
            created.append((file_id, name))

    if args.file:
        with open(args.file, "rb") as f:
            data = f.read()
        ctype = mimetypes.guess_type(args.file)[0] or "application/octet-stream"
        file_id = roundtrip(api, f"real file {args.file}", data, os.path.basename(args.file), ctype)
        if file_id:
            created.append((file_id, os.path.basename(args.file)))

    print("\n== Metadata and listing ==")
    if created:
        first_id = created[0][0]
        status, meta = api.metadata(first_id)
        check("GET metadata -> 200", status == 200 and meta.get("id") == first_id, f"got {status}: {meta}")
        status, page = api.list("?page_size=100&sort=-created_at")
        ids = {item.get("id") for item in page.get("items", [])}
        check("list -> 200", status == 200, f"got {status}")
        check("list contains every uploaded file", all(fid in ids for fid, _ in created), f"{len(ids)} listed")
        check("list has pagination fields", {"page", "page_size", "total", "total_pages"} <= set(page))
        status, page = api.list("?page=0&page_size=0")
        check("page=0&page_size=0 is clamped, not an error", status == 200, f"got {status}")
        status, page = api.list("?content_type=text/plain")
        check("content_type filter works", status == 200 and all(i["content_type"] == "text/plain" for i in page.get("items", [])))

    print("\n== Ownership ==")
    if args.other_cookie and created:
        other = Api(base, args.other_cookie)
        fid = created[0][0]
        status, _ = other.metadata(fid)
        check("other user: metadata -> 404", status == 404, f"got {status}")
        status, _, _ = other.download(fid)
        check("other user: download -> 404", status == 404, f"got {status}")
        check("other user: delete -> 404", other.delete(fid) == 404)
        status, page = other.list("?page_size=100")
        check("other user: list does not include it", fid not in {i.get("id") for i in page.get("items", [])})
        status, _, _ = api.download(fid)
        check("owner can still download after those attempts", status == 200, f"got {status}")
    else:
        print("  (skipped: pass --other-cookie to test isolation between users)")

    print("\n== Error cases ==")
    status, _ = api.metadata("00000000-0000-0000-0000-000000000000")
    check("missing id -> 404", status == 404, f"got {status}")
    status, _ = api.metadata("not-a-uuid")
    check("malformed id -> 404", status == 404, f"got {status}")
    if args.limit:
        status, info = api.upload(os.urandom(args.limit + 1), "too-big.bin", "application/octet-stream")
        check(f"over-limit upload ({args.limit}+1 bytes) -> 413", status == 413, f"got {status}: {info}")

    print("\n== Delete ==")
    for file_id, _ in created:
        if api.delete(file_id) != 204:
            check(f"delete {file_id} -> 204", False)
    if created:
        check("delete -> 204 (all)", True)
        file_id = created[0][0]
        status, _, _ = api.download(file_id)
        check("deleted file: download -> 404", status == 404, f"got {status}")
        status, _ = api.metadata(file_id)
        check("deleted file: metadata -> 404", status == 404, f"got {status}")
        status, page = api.list("?page_size=100")
        gone = all(fid not in {i.get("id") for i in page.get("items", [])} for fid, _ in created)
        check("deleted files no longer listed (list cache cleared)", gone)
        check("second delete -> 404", api.delete(file_id) == 404)

    passed, total = sum(results), len(results)
    print(f"\n{passed}/{total} checks passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
