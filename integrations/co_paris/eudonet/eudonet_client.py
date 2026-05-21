import json
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

# Eudonet Paris table fields
# Values come from the "MetaInfos" endpoint
# See: https://eudonet-partage.apps.paris.fr/eudoapi/eudoapidoc/swaggerui/#!/MetaInfos/post_MetaInfos


# ARRETE fields
ARRETE_TAB_ID = 1100;
ARRETE_ID = 1101;
ARRETE_COMPLEMENT_DE_TITRE = 1102;
ARRETE_TYPE = 1108;
ARRETE_DATE_DEBUT = 1109;
ARRETE_DATE_FIN = 1110;

# ARRETE_TYPE values
TEMPORAIRE = 8;
PERMANENT = 7;

# MESURE fields
MESURE_TAB_ID = 1200;
MESURE_ID = 1201;
MESURE_NOM = 1202;
MESURE_ALINEA = 1294;
MEASURE_NOM_CIRCULATION_INTERDITE_DB_VALUE = '103';

# LOCALISATION fields
LOCALISATION_TAB_ID = 2700;
LOCALISATION_ID = 2701;
LOCALISATION_PORTE_SUR = 2705;
LOCALISATION_ARRONDISSEMENT = 2708;
LOCALISATION_LIBELLE_VOIE = 2710;
LOCALISATION_LIBELLE_VOIE_DEBUT = 2730;
LOCALISATION_LIBELLE_VOIE_FIN = 2740;
LOCALISATION_N_ADRESSE_DEBUT = 2720;
LOCALISATION_N_ADRESSE_FIN = 2737;

# ADRESSE fields
ADRESSE_TAB_ID = 3400;
ADRESSE_LIBELLE = 3401; # Called 'Liste des Adresses', but it contains the full label of the address
ADRESSE_X_WGS84 = 3414;
ADRESSE_Y_WGS84 = 3411;

# Operators
# See: https://eudonet-partage.apps.paris.fr/eudoapi/eudoapidoc/lexique_FR.html
EQUALS = 0;
AND = 1;
OR = 2;
GREATER_THAN = 3;
NOT_EQUALS = 5;
NOT_IN_LIST = 15;

class EudonetClient:
    def __init__(self, base_url: str, credentials: str, logger=None, session=None):
        self.base_url = base_url.rstrip("/")
        self.credentials = credentials
        self.logger = logger
        self.session = session or requests.Session()
        self.token = None
        self.token_expiry_date = None

    def ensure_authenticated(self):
        if self.token and self.token_expiry_date and self.token_expiry_date > datetime.now(ZoneInfo("Europe/Paris")):
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
                raise Exception(f"""API Error : {data["ResultInfos"]["ErrorNumber"]} - {data["ResultInfos"]["ErrorMessage"]}""")
            
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