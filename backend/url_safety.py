"""Shared SSRF protection for all outbound HTTP requests.

Every outbound fetch of a user-controlled URL must go through
is_safe_domain() or resolve_safe_url() before making the request.
"""

import ipaddress
import re
import socket
from urllib.parse import urlparse


def is_safe_domain(domain: str) -> bool:
    """Check that a domain resolves only to public (non-internal) IP addresses.

    Returns False if:
    - The domain format is invalid
    - Any resolved IP is private, loopback, link-local, reserved, or multicast
    - DNS resolution fails
    """
    if not domain or not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9._-]*\.[a-zA-Z]{2,}$', domain):
        return False
    domain_lower = domain.lower()
    if domain_lower in ("localhost", "localhost.localdomain"):
        return False
    if domain_lower.endswith(".local") or domain_lower.endswith(".internal"):
        return False
    # Block cloud metadata endpoints
    if domain_lower in ("metadata.google.internal", "metadata.aws"):
        return False
    try:
        addrs = socket.getaddrinfo(domain, 443, proto=socket.IPPROTO_TCP)
        if not addrs:
            return False
        for family, _, _, _, sockaddr in addrs:
            ip = ipaddress.ip_address(sockaddr[0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                return False
        return True
    except (socket.gaierror, ValueError, OSError):
        return False


def resolve_safe_url(url: str) -> str | None:
    """Resolve URL and validate all IPs are public.

    Returns the first safe resolved IP address, or None if the URL is unsafe.
    Only HTTPS URLs to public hosts are allowed.

    Performs a single DNS resolution (is_safe_domain already resolves, so we
    do the validation inline here to avoid double lookups).
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    if parsed.scheme != "https":
        return None
    host = parsed.hostname or ""
    if not host or "." not in host:
        return None
    # Inline the same checks as is_safe_domain but resolve only once
    if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9._-]*\.[a-zA-Z]{2,}$', host):
        return None
    domain_lower = host.lower()
    if domain_lower in ("localhost", "localhost.localdomain"):
        return None
    if domain_lower.endswith(".local") or domain_lower.endswith(".internal"):
        return None
    if domain_lower in ("metadata.google.internal", "metadata.aws"):
        return None
    try:
        addr_info = socket.getaddrinfo(host, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
        if not addr_info:
            return None
        first_ip = None
        for family, _, _, _, sockaddr in addr_info:
            ip = ipaddress.ip_address(sockaddr[0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                return None
            if first_ip is None:
                first_ip = str(ip)
        return first_ip
    except (socket.gaierror, ValueError, OSError):
        return None


def is_safe_url(url: str) -> bool:
    """Block SSRF: only allow https URLs to public hosts."""
    return resolve_safe_url(url) is not None


def resolve_domain_to_safe_ip(domain: str) -> str | None:
    """Resolve a domain to a single safe public IP, or None if unsafe.

    Use this IP to connect directly (DNS-pinning) and avoid TOCTOU rebinding.
    """
    if not is_safe_domain(domain):
        return None
    try:
        addrs = socket.getaddrinfo(domain, 443, proto=socket.IPPROTO_TCP)
        if not addrs:
            return None
        safe_ip = None
        for _, _, _, _, sockaddr in addrs:
            ip = ipaddress.ip_address(sockaddr[0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                return None
            if safe_ip is None:
                safe_ip = sockaddr[0]
        return safe_ip
    except (socket.gaierror, ValueError, OSError):
        return None
