import html
import re

ELLIPSIS = "..."


def plain_text(value: str | None) -> str:
    """Eudonet memos are HTML: strip the markup, decode the entities, unwrap.

    Some records hold a whole Word paste, comments included.
    """
    if not value:
        return ""
    without_comments = re.sub(r"<!--.*?-->", " ", value, flags=re.DOTALL)
    without_tags = re.sub(r"<[^>]+>", " ", without_comments)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def truncate(value: str, limit: int, ellipsis: str = ELLIPSIS) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - len(ellipsis)] + ellipsis
