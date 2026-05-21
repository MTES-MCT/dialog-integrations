from datetime import datetime
from typing import Iterator
from zoneinfo import ZoneInfo

from .eudonet_client import EudonetClient


class EudonetParisExtractor:
    # ARRETE fields
    ARRETE_TAB_ID = 1100
    ARRETE_ID = 1101
    ARRETE_COMPLEMENT_DE_TITRE = 1102
    ARRETE_TYPE = 1108
    ARRETE_DATE_DEBUT = 1109
    ARRETE_DATE_FIN = 1110

    # ARRETE_TYPE values
    PERMANENT = 7
    TEMPORAIRE = 8

    # MESURE fields
    MESURE_TAB_ID = 1200
    MESURE_ID = 1201
    MESURE_NOM = 1202
    MESURE_ALINEA = 1294
    MEASURE_NOM_CIRCULATION_INTERDITE_DB_VALUE = "103"

    # LOCALISATION fields
    LOCALISATION_TAB_ID = 2700
    LOCALISATION_ID = 2701
    LOCALISATION_PORTE_SUR = 2705
    LOCALISATION_ARRONDISSEMENT = 2708
    LOCALISATION_LIBELLE_VOIE = 2710
    LOCALISATION_LIBELLE_VOIE_DEBUT = 2730
    LOCALISATION_LIBELLE_VOIE_FIN = 2740
    LOCALISATION_N_ADRESSE_DEBUT = 2720
    LOCALISATION_N_ADRESSE_FIN = 2737

    # ADRESSE fields
    ADRESSE_TAB_ID = 3400
    ADRESSE_LIBELLE = 3401
    ADRESSE_X_WGS84 = 3414
    ADRESSE_Y_WGS84 = 3411

    # Operators
    EQUALS = 0
    AND = 1
    OR = 2
    GREATER_THAN = 3
    NOT_EQUALS = 5
    NOT_IN_LIST = 15

    def __init__(self, eudonet_client: EudonetClient):
        self.eudonet_client = eudonet_client

    def get_number_of_regulations(self) -> int:
        where_customs_all_regulations = [
            {
                "Criteria": {
                    "Field": self.ARRETE_TYPE,
                    "Operator": self.EQUALS,
                    "Value": self.TEMPORAIRE,
                }
            },
            {
                "Criteria": {
                    "Field": self.ARRETE_TYPE,
                    "Operator": self.NOT_EQUALS,
                    "Value": self.TEMPORAIRE,
                },
                "InterOperator": self.OR,
            },
        ]
        return self.eudonet_client.count(
            tab_id=self.ARRETE_TAB_ID,
            list_cols=[self.ARRETE_ID],
            where_custom={"WhereCustoms": where_customs_all_regulations},
        )

    def get_number_of_measures(self) -> int:
        where_customs_all_measures = [
            {
                "Criteria": {
                    "Field": self.MESURE_NOM,
                    "Operator": self.EQUALS,
                    "Value": self.MEASURE_NOM_CIRCULATION_INTERDITE_DB_VALUE,
                }
            },
            {
                "Criteria": {
                    "Field": self.MESURE_NOM,
                    "Operator": self.NOT_EQUALS,
                    "Value": self.MEASURE_NOM_CIRCULATION_INTERDITE_DB_VALUE,
                },
                "InterOperator": self.OR,
            },
        ]
        return self.eudonet_client.count(
            tab_id=self.MESURE_TAB_ID,
            list_cols=[self.MESURE_ID],
            where_custom={"WhereCustoms": where_customs_all_measures},
        )

    def iter_extract(self, later_than_utc: datetime, ignore_ids: list | None = None) -> Iterator:
        if ignore_ids is None:
            ignore_ids = []

        later_than_paris = later_than_utc.astimezone(ZoneInfo("Europe/Paris"))

        where_customs = [
            {
                "Criteria": {
                    "Field": self.ARRETE_TYPE,
                    "Operator": self.EQUALS,
                    "Value": self.TEMPORAIRE,
                }
            },
            {
                "Criteria": {
                    "Field": self.ARRETE_DATE_FIN,
                    "Operator": self.GREATER_THAN,
                    "Value": later_than_paris.strftime("%Y/%m/%d %H:%M:%S"),
                },
                "InterOperator": self.AND,
            },
        ]

        if len(ignore_ids) > 0:
            where_customs.append(
                {
                    "Criteria": {
                        "Field": self.ARRETE_ID,
                        "Operator": self.NOT_IN_LIST,
                        "Value": ";".join(str(id) for id in ignore_ids),
                    },
                    "InterOperator": self.AND,
                }
            )

        regulation_order_rows = self.eudonet_client.search(
            tab_id=self.ARRETE_TAB_ID,
            list_cols=[
                self.ARRETE_ID,
                self.ARRETE_COMPLEMENT_DE_TITRE,
                self.ARRETE_TYPE,
                self.ARRETE_DATE_DEBUT,
                self.ARRETE_DATE_FIN,
            ],
            where_custom={"WhereCustoms": where_customs},
        )

        for regulation_order_row in regulation_order_rows:
            row = {
                "fileId": regulation_order_row["fileId"],
                "fields": regulation_order_row["fields"],
                "measures": [],
            }

            mesure_rows = self.eudonet_client.search(
                tab_id=self.MESURE_TAB_ID,
                list_cols=[
                    self.MESURE_ID,
                    self.MESURE_NOM,
                    self.MESURE_ALINEA,
                ],
                where_custom={
                    "WhereCustoms": [
                        {
                            "Criteria": {
                                "Field": self.ARRETE_TAB_ID,
                                "Operator": self.EQUALS,
                                "Value": regulation_order_row["fileId"],
                            }
                        },
                        {
                            "Criteria": {
                                "Field": self.MESURE_NOM,
                                "Operator": self.EQUALS,
                                "Value": self.MEASURE_NOM_CIRCULATION_INTERDITE_DB_VALUE,
                            },
                            "InterOperator": self.AND,
                        },
                    ]
                },
            )

            for mesure_row in mesure_rows:
                measure_row = {
                    "fileId": mesure_row["fileId"],
                    "fields": mesure_row["fields"],
                    "locations": [],
                }

                location_rows = self.eudonet_client.search(
                    tab_id=self.LOCALISATION_TAB_ID,
                    list_cols=[
                        self.LOCALISATION_ID,
                        self.LOCALISATION_PORTE_SUR,
                        self.LOCALISATION_ARRONDISSEMENT,
                        self.LOCALISATION_LIBELLE_VOIE,
                        self.LOCALISATION_LIBELLE_VOIE_DEBUT,
                        self.LOCALISATION_LIBELLE_VOIE_FIN,
                        self.LOCALISATION_N_ADRESSE_DEBUT,
                        self.LOCALISATION_N_ADRESSE_FIN,
                    ],
                    where_custom={
                        "Criteria": {
                            "Field": self.MESURE_TAB_ID,
                            "Operator": self.EQUALS,
                            "Value": mesure_row["fileId"],
                        }
                    },
                )

                for location_row in location_rows:
                    from_coords = None
                    to_coords = None

                    if location_row["fields"].get(self.LOCALISATION_N_ADRESSE_DEBUT):
                        from_coords = self._get_address_coords(
                            house_number=location_row["fields"][self.LOCALISATION_N_ADRESSE_DEBUT],
                            road_name=location_row["fields"][self.LOCALISATION_LIBELLE_VOIE],
                        )

                    if location_row["fields"].get(self.LOCALISATION_N_ADRESSE_FIN):
                        to_coords = self._get_address_coords(
                            house_number=location_row["fields"][self.LOCALISATION_N_ADRESSE_FIN],
                            road_name=location_row["fields"][self.LOCALISATION_LIBELLE_VOIE],
                        )

                    measure_row["locations"].append(
                        {
                            "fileId": location_row["fileId"],
                            "fields": location_row["fields"],
                            "fromCoords": from_coords,
                            "toCoords": to_coords,
                        }
                    )

                row["measures"].append(measure_row)

            yield row

    def _get_address_coords(self, house_number: str, road_name: str) -> dict | None:
        rows = self.eudonet_client.search(
            tab_id=self.ADRESSE_TAB_ID,
            list_cols=[
                self.ADRESSE_X_WGS84,
                self.ADRESSE_Y_WGS84,
            ],
            where_custom={
                "Criteria": {
                    "Field": self.ADRESSE_LIBELLE,
                    "Operator": self.EQUALS,
                    "Value": f"{house_number} {road_name}",
                }
            },
        )

        if not rows:
            return None

        x = self._parse_coordinate(rows[0]["fields"][self.ADRESSE_X_WGS84])
        y = self._parse_coordinate(rows[0]["fields"][self.ADRESSE_Y_WGS84])

        return {"lon": x, "lat": y}

    @staticmethod
    def _parse_coordinate(value: str) -> float:
        # '45,12345' -> 45.12345
        return float(value.replace(",", "."))
