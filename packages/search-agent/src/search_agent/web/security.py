"""URL normalization and public-network validation for webpage retrieval."""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from urllib.parse import SplitResult, urlsplit, urlunsplit


@dataclass
class WebAddressError(ValueError):
    """A URL cannot be used safely by the webpage service."""

    status: str
    reason: str

    def __str__(self) -> str:
        return self.reason


def normalize_web_url(raw_url: str) -> str:
    """Return a stable HTTP(S) URL or raise a structured validation error.

    Fragments never affect the fetched representation, so they are removed.
    Paths and query ordering are otherwise preserved.  Hostnames are converted
    to their IDNA form so policy and authorization comparisons cannot disagree
    over Unicode spelling.
    """

    candidate = raw_url.strip()
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError as exc:
        raise WebAddressError("not_authorized", f"invalid URL: {exc}") from exc

    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise WebAddressError("not_authorized", "only HTTP(S) URLs are supported")
    if parsed.username is not None or parsed.password is not None:
        raise WebAddressError("not_authorized", "URL credentials are not allowed")
    if not parsed.hostname:
        raise WebAddressError("not_authorized", "URL must include a hostname")

    try:
        host = parsed.hostname.rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise WebAddressError("not_authorized", "hostname is not valid IDNA") from exc
    if not host:
        raise WebAddressError("not_authorized", "URL must include a hostname")

    default_port = (scheme == "http" and port == 80) or (
        scheme == "https" and port == 443
    )
    rendered_host = f"[{host}]" if ":" in host else host
    netloc = (
        rendered_host if port is None or default_port else f"{rendered_host}:{port}"
    )
    path = parsed.path or "/"
    return urlunsplit(SplitResult(scheme, netloc, path, parsed.query, ""))


def hostname_for_url(url: str) -> str:
    """Return the already-normalized hostname used for policy matching."""

    hostname = urlsplit(url).hostname
    assert hostname is not None, f"normalized URL unexpectedly lacks a host: {url}"
    return hostname.lower()


def same_origin(first: str, second: str) -> bool:
    """Return whether two normalized URLs have identical effective origins."""

    left = urlsplit(first)
    right = urlsplit(second)

    def effective_port(parsed: SplitResult) -> int:
        if parsed.port is not None:
            return parsed.port
        return 443 if parsed.scheme == "https" else 80

    return (
        left.scheme,
        left.hostname,
        effective_port(left),
    ) == (
        right.scheme,
        right.hostname,
        effective_port(right),
    )


AddressResolver = Callable[..., Sequence[tuple]]


def validate_public_destination(
    url: str,
    *,
    resolver: AddressResolver = socket.getaddrinfo,
) -> tuple[str, ...]:
    """Resolve a URL and reject any non-public destination address.

    This check runs immediately before each request, including redirects.  It
    prevents ordinary SSRF targets such as loopback, link-local, private, and
    documentation ranges.  Returning the addresses makes the decision visible
    to tests and future connection-pinning work.
    """

    parsed = urlsplit(url)
    hostname = parsed.hostname
    assert hostname is not None, f"normalized URL unexpectedly lacks a host: {url}"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    try:
        records = resolver(hostname, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise WebAddressError(
            "http_error", f"DNS lookup failed for {hostname}: {exc}"
        ) from exc

    addresses = sorted({str(record[4][0]) for record in records})
    if not addresses:
        raise WebAddressError(
            "http_error", f"DNS lookup returned no addresses for {hostname}"
        )

    for address in addresses:
        try:
            parsed_address = ipaddress.ip_address(address)
        except ValueError as exc:
            raise WebAddressError(
                "not_authorized", f"DNS returned invalid address {address}"
            ) from exc
        if not parsed_address.is_global:
            raise WebAddressError(
                "not_authorized",
                f"destination resolves to non-public address {address}",
            )
    return tuple(addresses)


def looks_like_pdf_url(url: str) -> bool:
    """Return whether the URL path itself identifies a PDF resource."""

    return urlsplit(url).path.lower().rstrip("/").endswith(".pdf")
