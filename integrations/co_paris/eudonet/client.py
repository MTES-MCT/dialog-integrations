"""Read-only client for the Eudonet REST API used by the City of Paris.

Eudonet exposes one generic endpoint per table (`POST /Search/{table}`) with a page size
capped at 50 rows. Everything else — the criteria tree, the catalog values, the parent
links — is expressed with numeric field identifiers (`descId`), documented in
`ai/docs/…` and in the exploration dump.

The account we use has read permissions only: this module never calls `/CUD`.
"""

import time
from datetime import datetime, timedelta
from typing import Any, Iterable, Iterator, Sequence

import requests
from loguru import logger

DEFAULT_BASE_URL = "https://eudonet-partage.apps.paris.fr/EudoAPI"

# Criteria operators, as accepted in `WhereCustom.Criteria.Operator`.
EQUALS = 0
GREATER_THAN = 3
GREATER_OR_EQUAL = 4
IN_LIST = 8  # value is a ";"-separated list, at least 300 entries accepted
CONTAINS = 9
IS_EMPTY = 10
IS_TRUE = 11
IS_NOT_EMPTY = 17

# `InterOperator`, ignored on the first criterion of a group.
INTER_AND = 1
INTER_OR = 2

# `ResultInfos.ErrorNumber` values we know how to recover from.
EXPIRED_TOKEN_ERRORS = (101, 102, 103)
QUOTA_ERROR = 300

ROWS_PER_PAGE = 50
QUOTA_PAUSE_SECONDS = 20
EMPTY_BATCH_PAUSE_SECONDS = 5
DEFAULT_ATTEMPTS = 6
DEFAULT_TIMEOUT = (10, 120)
# Re-authenticate this long before the token actually expires.
TOKEN_MARGIN = timedelta(minutes=5)
# Used when the server does not hand back a parsable expiration date.
FALLBACK_TOKEN_LIFETIME = timedelta(minutes=20)

# `ExpirationDate` has been seen as ISO 8601; the other two are the formats the rest of the
# API uses for dates, kept as a safety net.
EXPIRATION_DATE_FORMATS = ("%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S")

Criterion = dict[str, Any]


class EudonetError(Exception):
    """Any failure that leaves us without usable data."""


def criterion(field: int | str, operator: int, value: Any = "") -> Criterion:
    """A single leaf of the criteria tree.

    `field` is a descId; catalog fields compare on their `DBValue` with `EQUALS`, and on
    their label with `CONTAINS`. Dates go in as `YYYY/MM/DD`.
    """
    return {
        "WhereCustoms": None,
        "Criteria": {"Field": str(field), "Operator": operator, "Value": str(value)},
        "InterOperator": 0,
    }


def _combine(inter_operator: int, criteria: Sequence[Criterion]) -> Criterion:
    if not criteria:
        raise ValueError("A criteria group needs at least one criterion")
    members = []
    for index, member in enumerate(criteria):
        member = dict(member)
        # The operator of the first member is meaningless and must stay 0.
        member["InterOperator"] = 0 if index == 0 else inter_operator
        members.append(member)
    return {"WhereCustoms": members, "Criteria": None, "InterOperator": 0}


def all_of(*criteria: Criterion) -> Criterion:
    """Group of criteria joined by AND. Groups can be nested."""
    return _combine(INTER_AND, criteria)


def any_of(*criteria: Criterion) -> Criterion:
    """Group of criteria joined by OR. Groups can be nested."""
    return _combine(INTER_OR, criteria)


def flatten_row(row: dict[str, Any]) -> dict[str, Any]:
    """Turn one API row into the flat shape the exploration dumps use.

    `{"FileId": 1, "<descId>": Value, "<descId>_db": DbValue, "<descId>_fid": parent FileId}`
    The `_fid` key only appears on link fields; that is how a measure points at its
    regulation (`1101_fid`) and a location at its measure (`1202_fid`).
    """
    flat: dict[str, Any] = {"FileId": row["FileId"]}
    for field in row.get("Fields") or []:
        key = str(field["DescId"])
        flat[key] = field.get("Value")
        flat[f"{key}_db"] = field.get("DbValue")
        if field.get("FileId"):
            flat[f"{key}_fid"] = field["FileId"]
    return flat


class EudonetClient:
    """Authenticated, paginated, retrying access to `POST /Search/{table}`."""

    def __init__(
        self,
        credentials: dict[str, Any],
        base_url: str = DEFAULT_BASE_URL,
        rows_per_page: int = ROWS_PER_PAGE,
        attempts: int = DEFAULT_ATTEMPTS,
        timeout: tuple[int, int] = DEFAULT_TIMEOUT,
        session: requests.Session | None = None,
    ):
        self.credentials = credentials
        self.base_url = base_url.rstrip("/")
        self.rows_per_page = rows_per_page
        self.attempts = attempts
        self.timeout = timeout
        self.session = session or requests.Session()
        self._token: str | None = None
        self._token_expires_at: datetime | None = None

    # -- authentication ---------------------------------------------------------------

    def authenticate(self) -> str:
        """Fetch a fresh token and remember when it expires."""
        response = self.session.post(
            f"{self.base_url}/Authenticate/Token",
            json=self.credentials,
            timeout=self.timeout,
        )
        payload = response.json()
        infos = payload.get("ResultInfos") or {}
        if not infos.get("Success"):
            # Never log the credentials, only the server's own message.
            raise EudonetError(f"Eudonet authentication failed: {infos.get('ErrorMessage')}")

        data = payload["ResultData"]
        self._token = data["Token"]
        self._token_expires_at = _parse_expiration(data.get("ExpirationDate"))
        logger.debug(f"Eudonet token obtained, expires at {self._token_expires_at}")
        return self._token  # type: ignore[return-value]

    @property
    def token(self) -> str:
        """The current token, renewed before it expires."""
        expired = (
            self._token_expires_at is not None
            and datetime.now() >= self._token_expires_at - TOKEN_MARGIN
        )
        if self._token is None or expired:
            return self.authenticate()
        return self._token

    # -- searching --------------------------------------------------------------------

    def search(
        self,
        table: int,
        columns: Sequence[int],
        where: Criterion,
        page: int = 1,
        with_metadata: bool = False,
    ) -> dict[str, Any]:
        """One page of results, as the raw JSON payload."""
        body = {
            "ShowMetadata": with_metadata,
            "RowsPerPage": self.rows_per_page,
            "NumPage": page,
            "ListCols": list(columns),
            "WhereCustom": where,
        }
        return self._post(f"/Search/{table}", body)

    def search_all(
        self, table: int, columns: Sequence[int], where: Criterion, label: str = ""
    ) -> list[dict[str, Any]]:
        """Every matching row of `table`, flattened. Logs the volume before paging."""
        return list(self.iter_search(table, columns, where, label=label))

    def iter_search(
        self, table: int, columns: Sequence[int], where: Criterion, label: str = ""
    ) -> Iterator[dict[str, Any]]:
        """Same as `search_all`, one row at a time."""
        name = label or f"table {table}"
        first = self.search(table, columns, where, page=1, with_metadata=True)
        metadata = first.get("ResultMetaData") or {}
        total_rows = metadata.get("TotalRows", 0)
        total_pages = metadata.get("TotalPages", 1)
        logger.info(f"Eudonet {name}: {total_rows} rows over {total_pages} pages")

        for row in first["ResultData"]["Rows"]:
            yield flatten_row(row)
        for page in range(2, total_pages + 1):
            payload = self.search(table, columns, where, page=page)
            for row in payload["ResultData"]["Rows"]:
                yield flatten_row(row)
        logger.debug(f"Eudonet {name}: {total_pages} pages read")

    def search_by_ids(
        self,
        table: int,
        columns: Sequence[int],
        id_field: int,
        ids: Iterable[int],
        chunk: int = 300,
        label: str = "",
    ) -> list[dict[str, Any]]:
        """Rows whose `id_field` is one of `ids`, queried in chunks (operator `IN_LIST`).

        Used to pull the locations of a known set of measures: the parent table cannot be
        filtered from two levels down (error 204), but its link field can.
        """
        name = label or f"table {table}"
        identifiers = list(ids)
        rows: list[dict[str, Any]] = []
        starts = list(range(0, len(identifiers), chunk))
        for index, start in enumerate(starts, start=1):
            batch = identifiers[start : start + chunk]
            where = criterion(id_field, IN_LIST, ";".join(str(value) for value in batch))
            batch_rows = list(
                self.iter_search(table, columns, where, label=f"{name} batch {index}")
            )
            if not batch_rows:
                # Seen live on 2026-09-08: a batch of 50 measures answered `TotalRows = 0`
                # once, then 64 rows on every replay. An empty batch is plausible but rare,
                # so it is read a second time before being believed.
                logger.warning(
                    f"Eudonet {name} batch {index}: empty answer for {len(batch)} "
                    f"identifiers, reading it again in {EMPTY_BATCH_PAUSE_SECONDS} s"
                )
                time.sleep(EMPTY_BATCH_PAUSE_SECONDS)
                batch_rows = list(
                    self.iter_search(table, columns, where, label=f"{name} batch {index}")
                )
            rows.extend(batch_rows)
        logger.info(
            f"Eudonet {name}: {len(rows)} rows for {len(identifiers)} identifiers "
            f"in {len(starts)} batches"
        )
        return rows

    def count(self, table: int, where: Criterion) -> int:
        """How many rows match, without reading them.

        The API refuses an empty column list (error 701), so the table's label field
        (`table + 1`, e.g. 1101 for 1100) is requested on a single row.
        """
        body = {
            "ShowMetadata": True,
            "RowsPerPage": 1,
            "NumPage": 1,
            "ListCols": [table + 1],
            "WhereCustom": where,
        }
        payload = self._post(f"/Search/{table}", body)
        return (payload.get("ResultMetaData") or {}).get("TotalRows", 0)

    # -- transport --------------------------------------------------------------------

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST with the retries the API demands: network, expired token, quota."""
        url = f"{self.base_url}{path}"
        last_error: str = "no attempt was made"

        for attempt in range(1, self.attempts + 1):
            try:
                response = self.session.post(
                    url, headers={"x-auth": self.token}, json=body, timeout=self.timeout
                )
                payload = response.json()
            except (requests.RequestException, ValueError) as error:
                last_error = f"{type(error).__name__}: {error}"
                logger.warning(f"Eudonet {path} failed ({last_error}), attempt {attempt}")
                time.sleep(10 * attempt)
                continue

            infos = payload.get("ResultInfos") or {}
            if infos.get("Success"):
                return payload

            number = infos.get("ErrorNumber")
            last_error = f"error {number}: {infos.get('ErrorMessage')}"
            if number in EXPIRED_TOKEN_ERRORS:
                logger.info(f"Eudonet token rejected ({number}), re-authenticating")
                self.authenticate()
                continue
            if number == QUOTA_ERROR:
                logger.warning(f"Eudonet quota reached, pausing {QUOTA_PAUSE_SECONDS}s")
                time.sleep(QUOTA_PAUSE_SECONDS)
                continue
            raise EudonetError(f"Eudonet {path} rejected the request - {last_error}")

        raise EudonetError(f"Eudonet {path} failed after {self.attempts} attempts - {last_error}")


def _parse_expiration(value: Any) -> datetime | None:
    """Read `ExpirationDate` as a naive local datetime, whatever shape it comes in."""
    if not value:
        return datetime.now() + FALLBACK_TOKEN_LIFETIME

    text = str(value)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = None
        for pattern in EXPIRATION_DATE_FORMATS:
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue

    if parsed is None:
        logger.warning(f"Unreadable Eudonet ExpirationDate {text!r}, assuming a short lifetime")
        return datetime.now() + FALLBACK_TOKEN_LIFETIME

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed
