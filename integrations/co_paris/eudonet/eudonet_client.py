from datetime import datetime
from zoneinfo import ZoneInfo

import requests


class EudonetClient:
    def __init__(self, base_url: str, credentials: str, logger=None, session=None):
        self.base_url = base_url.rstrip("/")
        self.credentials = credentials
        self.logger = logger
        self.session = session or requests.Session()
        self.token = None
        self.token_expiry_date = None

    def ensure_authenticated(self):
        if (
            self.token
            and self.token_expiry_date
            and self.token_expiry_date > datetime.now(ZoneInfo("Europe/Paris"))
        ):
            return

        self.token = None
        self.token_expiry_date = None

        url = f"{self.base_url}/Authenticate/Token"
        headers = {"Content-Type": "application/json"}

        response = self.session.post(url, headers=headers, data=self.credentials)
        response.raise_for_status()

        data = response.json()
        self.token = data["ResultData"]["Token"]
        self.token_expiry_date = datetime.strptime(
            data["ResultData"]["ExpirationDate"],
            "%Y/%m/%d %H:%M:%S",
        ).replace(tzinfo=ZoneInfo("Europe/Paris"))

    def request(self, method: str, path: str, headers=None, json_body=None, data=None):
        self.ensure_authenticated()

        if headers is None:
            headers = {}
        headers = {**headers, "X-Auth": self.token}

        url = path if path.startswith("http") else f"{self.base_url}{path}"

        if self.logger:
            self.logger.debug(
                "request",
                {
                    "method": method,
                    "url": url,
                    "body": json_body if json_body is not None else data,
                },
            )

        response = self.session.request(
            method,
            url,
            headers=headers,
            json=json_body,
            data=data,
        )

        response_body = None
        try:
            response_body = response.json()
        except ValueError:
            response_body = response.text

        if self.logger:
            self.logger.debug("response", {"body": response_body})

        response.raise_for_status()
        return response

    def search(self, tab_id: int, list_cols: list, where_custom: dict | None = None):
        rows = []
        page_number = 1
        max_pages = 1
        where_custom = where_custom or {}

        while True:
            payload = {
                "ShowMetadata": True,
                "RowsPerPage": 50,
                "NumPage": page_number,
                "ListCols": list_cols,
                "WhereCustom": where_custom,
            }

            response = self.request(
                "POST",
                f"/Search/{tab_id}",
                headers={"Content-Type": "application/json"},
                json_body=payload,
            )

            data = response.json()

            if not data["ResultInfos"]["Success"]:
                raise Exception(
                    f"""API Error : """
                    f"""{data["ResultInfos"]["ErrorNumber"]}"""
                    f""" - {data["ResultInfos"]["ErrorMessage"]}"""
                )

            if self.logger:
                self.logger.debug(f""" Found {data["ResultMetaData"]["TotalRows"]} total rows""")

            for row in data["ResultData"]["Rows"]:
                fields = {field["DescId"]: field["Value"] for field in row["Fields"]}
                rows.append({"fileId": row["FileId"], "fields": fields})

            total_pages = data["ResultMetaData"]["TotalPages"]
            if page_number >= total_pages or page_number >= max_pages:
                break

            page_number += 1

        return rows

    def count(self, tab_id: int, list_cols: list, where_custom: dict | None = None) -> int:
        where_custom = where_custom or {}

        payload = {
            "ShowMetadata": True,
            "RowsPerPage": 1,
            "NumPage": 1,
            "ListCols": list_cols,
            "WhereCustom": where_custom,
        }

        response = self.request(
            "POST",
            f"/Search/{tab_id}",
            headers={"Content-Type": "application/json"},
            json_body=payload,
        )

        data = response.json()
        return data["ResultMetaData"]["TotalRows"]
