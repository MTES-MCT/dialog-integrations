"""Why a write failed, a refusal or an outage, and the causes counted."""

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from http import HTTPStatus

OUTAGE_4XX = frozenset(
    {
        HTTPStatus.UNAUTHORIZED,
        HTTPStatus.FORBIDDEN,
        HTTPStatus.PROXY_AUTHENTICATION_REQUIRED,
        HTTPStatus.REQUEST_TIMEOUT,
        HTTPStatus.TOO_MANY_REQUESTS,
    }
)


UNRECORDED_CAUSE = "autre"


@dataclass(frozen=True)
class WriteFailure:
    """Why a write failed. `status` is None when the call got no answer."""

    status: int | None

    @property
    def is_rejection(self) -> bool:
        return (
            self.status is not None and 400 <= self.status < 500 and self.status not in OUTAGE_4XX
        )

    @property
    def cause(self) -> str:
        return f"HTTP {self.status}" if self.status is not None else "sans réponse"


def split_failures(
    identifiers: Iterable[str], failures: Mapping[str, WriteFailure]
) -> tuple[int, dict[str, int]]:
    """Regulations that were not written: (rejections, outages by cause).

    A refusal's motive is only in the log, which keeps the API's whole answer.
    """
    rejections = 0
    outages: Counter[str] = Counter()
    for identifier in identifiers:
        failure = failures.get(identifier)
        if failure is None:
            outages[UNRECORDED_CAUSE] += 1
        elif failure.is_rejection:
            rejections += 1
        else:
            outages[failure.cause] += 1
    return rejections, dict(outages.most_common())
