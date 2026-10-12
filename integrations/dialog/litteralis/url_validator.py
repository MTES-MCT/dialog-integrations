"""Symfony's `Url` constraint (symfony/validator 7.4.18) as the back end applies it to
the document URL of a Litteralis regulation: `https` only, a TLD required.

In PHP, the import validates that URL together with the regulation: an invalid one
makes the whole regulation fail. Through the API the document is a second call, made
after the regulation exists, so the URL has to be judged before the regulation is sent.

The pattern is Symfony's, translated: PCRE's `\\pL`, `\\pN`, `\\pS` and `\\pM` become
explicit ranges built from `unicodedata`, `$` with the `D` modifier becomes `\\Z`.
"""

import ipaddress
import re
import sys
import unicodedata
from functools import lru_cache


def _ranges(prefix: str) -> str:
    """Every code point whose Unicode category starts with `prefix`, as class ranges."""
    parts = []
    start = previous = None
    for code in range(sys.maxunicode + 1):
        inside = unicodedata.category(chr(code)).startswith(prefix)
        if inside and start is None:
            start = code
        if not inside and start is not None:
            parts.append((start, previous))
            start = None
        previous = code
    if start is not None:
        parts.append((start, previous))
    return "".join(
        re.escape(chr(a)) if a == b else f"{re.escape(chr(a))}-{re.escape(chr(b))}"
        for a, b in parts
    )


_IPV6 = r"(?:(?:(?:(?:(?:(?:(?:[0-9a-f]{1,4})):){6})(?:(?:(?:(?:(?:[0-9a-f]{1,4})):(?:(?:[0-9a-f]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:::(?:(?:(?:[0-9a-f]{1,4})):){5})(?:(?:(?:(?:(?:[0-9a-f]{1,4})):(?:(?:[0-9a-f]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:[0-9a-f]{1,4})))?::(?:(?:(?:[0-9a-f]{1,4})):){4})(?:(?:(?:(?:(?:[0-9a-f]{1,4})):(?:(?:[0-9a-f]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-f]{1,4})):){0,1}(?:(?:[0-9a-f]{1,4})))?::(?:(?:(?:[0-9a-f]{1,4})):){3})(?:(?:(?:(?:(?:[0-9a-f]{1,4})):(?:(?:[0-9a-f]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-f]{1,4})):){0,2}(?:(?:[0-9a-f]{1,4})))?::(?:(?:(?:[0-9a-f]{1,4})):){2})(?:(?:(?:(?:(?:[0-9a-f]{1,4})):(?:(?:[0-9a-f]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-f]{1,4})):){0,3}(?:(?:[0-9a-f]{1,4})))?::(?:(?:[0-9a-f]{1,4})):)(?:(?:(?:(?:(?:[0-9a-f]{1,4})):(?:(?:[0-9a-f]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-f]{1,4})):){0,4}(?:(?:[0-9a-f]{1,4})))?::)(?:(?:(?:(?:(?:[0-9a-f]{1,4})):(?:(?:[0-9a-f]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-f]{1,4})):){0,5}(?:(?:[0-9a-f]{1,4})))?::)(?:(?:[0-9a-f]{1,4})))|(?:(?:(?:(?:(?:(?:[0-9a-f]{1,4})):){0,6}(?:(?:[0-9a-f]{1,4})))?::))))"  # noqa: E501


@lru_cache(maxsize=1)
def _pattern() -> re.Pattern:
    letter, number, symbol, mark = _ranges("L"), _ranges("N"), _ranges("S"), _ranges("M")
    pattern = (
        r"^(https)://"
        r"((?:[" + letter + number + r"\-._~!$&'()*+,;=]|%[0-9A-Fa-f]{2})++"
        r"(?::(?:[:" + letter + number + r"\-._~!$&'()*+,;=]|%[0-9A-Fa-f]{2})*+)?@)?"
        r"("
        r"(?:"
        r"(?:(?:[" + letter + number + symbol + mark + r"\-_]++\.)+"
        r"(?:(?:xn--[a-z0-9-]++)|(?:[" + letter + number + mark + r"]++)))"
        r"|[a-z0-9\-_]++"
        r")\.?"
        r"|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
        r"|\[" + _IPV6 + r"\]"
        r")"
        r"(:[0-9]+)?"
        r"(?:/(?:[" + letter + number + symbol + mark + r"\-._~!$&'()*+,;=:@]|%[0-9A-Fa-f]{2})*)*"
        r"(?:\?(?:[" + letter + number + r"\-._~!$&'()*+,;=:@/?\[\]]|%[0-9A-Fa-f]{2})*)?"
        r"(?:\#(?:[" + letter + number + r"\-._~!$&'()*+,;=:@/?\#]|%[0-9A-Fa-f]{2})*)?"
        r"\Z"
    )
    return re.compile(pattern, re.IGNORECASE)


def is_valid_https_url(value: str) -> bool:
    """`new Url(protocols: ['https'], requireTld: true)` on a non-null value.

    An empty string is valid: Symfony validators skip it.
    """
    if value == "":
        return True
    if not _pattern().match(value):
        return False
    host = _php_url_host(value)
    # A host with a TLD contains a dot, and is not an IP address.
    return host is not None and "." in host and not _is_ip(host)


def _php_url_host(value: str) -> str | None:
    """The host `parse_url` returns for a URL the pattern accepted."""
    rest = value.split("://", 1)[1]
    authority = re.split(r"[/?#]", rest, maxsplit=1)[0]
    if "@" in authority:
        authority = authority.rsplit("@", 1)[1]
    if authority.startswith("["):
        return authority[: authority.find("]") + 1]
    return authority.split(":", 1)[0]


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True
