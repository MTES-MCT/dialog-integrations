import html
import re

# Formatting tags fall inside words ("1</font><font>4", "C<strong>oncernant", "16<sup>ème"):
# removed without a space, unlike the block tags that separate them.
_INLINE_TAGS = re.compile(r"</?(?:font|strong|b|i|u|em|sup|sub|span|a)\b[^>]*>", re.IGNORECASE)


def plain_text(value: str | None) -> str:
    """Eudonet memos are HTML: strip the markup, decode the entities, unwrap.

    Some records hold a whole Word paste, comments included.
    """
    if not value:
        return ""
    without_comments = re.sub(r"<!--.*?-->", " ", value, flags=re.DOTALL)
    without_inline = _INLINE_TAGS.sub("", without_comments)
    without_tags = re.sub(r"<[^>]+>", " ", without_inline)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()
