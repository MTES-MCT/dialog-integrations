"""Simulated Litteralis data: no producer's real data, no Sogelink credentials needed.

Deterministic (seeded), so that the same cases can be fed to the back end's PHP code
and to this port, and their outputs compared. The values imitate what the back end's
ADR and tests document for the MEL, Montpellier, Mandelieu, Corrèze and Savoie: the
measure names, the "NAME N | key : value" parameters, the free-text hours.

`cases()` returns the comparison cases, each run as one import of one organization:
the bulk ones, then one case per behaviour that aborts the whole import in PHP (each
needs its own import). `layers()` returns a WFS dataset for the simulated end-to-end
run, without the fatal behaviours.
"""

import random
from copy import deepcopy

LATER_THAN = "2026-10-12T00:00:00+02:00"

SUPPORTED_MEASURES = [
    "SOGELINK - Circulation interdite",
    "Circulation interdite",
    "Limitation tonnage (Arrêté Permanent)",
    "SOGELINK - Limitation de vitesse",
    "Limitation de vitesse",
    "Limitation de vitesse (Arrêté Permanent)",
    "Interdiction de stationnement",
    "SOGELINK - Interdiction de stationnement",
    "Interdiction de dépasser",
    "Interdiction de dépasser (Arrêté Permanent)",
    "2021 - Circulation interdite RD",
    "2024 - Limitation de vitesse RD",
    "AP - Stationnement - Interdiction",
    "ATemp - Circulation alternée",
    "3 - Limitation de vitesse",
    "SOGELINK - Circulation alternée",
]

UNSUPPORTED_MEASURES = [
    "Déviation",
    "SOGELINK - Neutralisation de voie",
    "ATP - Gaz",
    "Mesure libre circulation",
    "SOGELINK - Interruption de circulation travaux",
    "SOGELINK - Rétrécissement temporaire de largeur de voie",
]

HOURS = [
    "de 08 h 00 à 18 h 00",
    "08 h 00 à 18 h 00",
    "DE 7 H 30 À 17 H 30",
    "DE 7 H 30 0 17 H 30",
    "DE 8H00 A 17H00",
    "8h à 18h",
    "de 8h à 18h",
    "de 7 heures 30 à 17 heures 30",
    "08:00 à 18:00",
    "de nuit de 21H00 à 6H00",
    "la nuit de 22h à 5h.",
    "19h à 7h",
    "de 23 h 30 à 0 h 30",
    "de 0h à 24h",
    "jour et nuit",
    "TOUS",
    "la journée",
    "24/24 7j/7j",
    "chaque jour de la période, 24h/24",
]

UNPARSABLE_HOURS = [
    "de 8h à 9h et de 16h à 17h",
    "Lundi, mardi, jeudi, vendredi de 8h à 9h et de 16h à 17h",
    "le matin",
    "les nuits du 05/10/2023, 06/10/2023 et 07/10/2023 de 21h00 à 06h00",
    "de 21h00 à 06h00 du lundi soir au samedi matin",
    "Samedis, Dimanches et jours fériés",
]

VEHICLES = [
    "piétons, cycles,poids lourds,véhicules légers",
    "véhicules de plus de 3.5 tonnes",
    "poids lourds",
    "bus de transport en commun",
    "poids lourds, bus de transport en commun, cars",
    "cycles",
    "0",
]

DEROGATIONS = [
    "véhicules de l'entreprise exécutant les travaux",
    "véhicules de police,véhicules de secours,véhicules de l'entreprise exécutant les travaux",
    "riverains",
    "0",
]

SPEEDS = ["30 km/h", "50", "70km/h", "environ 30 km/h", "0 km/h"]

STREETS = [
    "RUE JEAN JAURÈS (LILLE)",
    "AVENUE DENIS CORDONNIER (LILLE) - FACE AUX 7 BIS - 90 - 176",
    "BOULEVARD DE VERDUN (LILLE) - OPP AU 127",
    "RUE DE LA MARLACQUE (FROMELLES) ENTRE LES PR0+066 ET PR0+750",
    " PLACE DE LA DISTILLERIE ",
    "  CHEMIN DES DAMES  ",
    "RD 12 / RD 45",
]

DESCRIPTIONS = [
    "Description des travaux : sur réseaux ou ouvrages d'eaux usées / assainissement",
    "Description évènement : Braderie annuelle",
    "Description de l'évènement : Course cycliste",
    "Description des travaux : 0",
    "ajout annexe : N",
    "malformé sans deux-points",
]

AGENCIES = [117374, 125777, "126055", None]
CATEGORIES = ["Travaux", "Evenements", "Réglementation permanente", "ODP", None]
DOCUMENT_TYPES = ["ARRETE TEMPORAIRE", "ARRETE PERMANENT", "AUTORISATION VOIRIE"]


def _line(rng: random.Random) -> dict:
    lon, lat = 3.0 + rng.random() * 0.2, 50.6 + rng.random() * 0.1
    return {
        "type": "LineString",
        "coordinates": [
            [round(lon, 10), round(lat, 10)],
            [round(lon + 0.0004, 10), round(lat + 0.0002, 10)],
            [round(lon + 0.0007, 10), round(lat + 0.0005, 10)],
        ],
    }


def _geometry(rng: random.Random) -> dict | None:
    kind = rng.choices(
        ["LineString", "MultiLineString", "Polygon", "MultiPolygon", "Point", None],
        weights=[60, 12, 12, 4, 4, 8],
    )[0]
    if kind is None:
        return None
    line = _line(rng)
    if kind == "LineString":
        return line
    if kind == "MultiLineString":
        return {
            "type": "MultiLineString",
            "coordinates": [line["coordinates"], _line(rng)["coordinates"]],
        }
    if kind == "Point":
        return {"type": "Point", "coordinates": line["coordinates"][0]}
    ring = line["coordinates"] + [line["coordinates"][0]]
    if kind == "Polygon":
        return {"type": "Polygon", "coordinates": [ring]}
    return {"type": "MultiPolygon", "coordinates": [[ring]]}


def _date(rng: random.Random, year: int, month: int, day: int, hour: int) -> str:
    style = rng.choices(["Z", "+02:00", "+0100", "ms"], weights=[80, 8, 8, 4])[0]
    base = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:00:00"
    if style == "ms":
        return base + ".000Z"
    return base + style


def _measure_parameters(rng: random.Random, names: list[str]) -> str:
    """`parametresmesures`, with the numbering habits the ADR describes."""
    items = []
    for position, name in enumerate(names, start=1):
        numbering = rng.choices(["none", "right", "wrong"], weights=[70, 22, 8])[0]
        label = name
        if numbering == "right" and position > 1:
            label = f"{name} {position}"
        elif numbering == "wrong":
            label = f"{name} {position + 1}"

        if "vitesse" in name.lower() and rng.random() < 0.85:
            key = rng.choice(["limite de vitesse", "limite de vitesse", "vitesse"])
            items.append(f"{label} | {key} : {rng.choice(SPEEDS)}")
        if rng.random() < 0.5:
            hours = rng.choice(HOURS if rng.random() < 0.85 else UNPARSABLE_HOURS)
            items.append(f"{label} | jours et horaires : {hours}")
        if rng.random() < 0.35:
            items.append(f"{label} | véhicules concernés : {rng.choice(VEHICLES)}")
        if rng.random() < 0.45:
            items.append(f"{label} | dérogations : {rng.choice(DEROGATIONS)}")
        if rng.random() < 0.15:
            items.append(f"{label} | caractère aggravant : gênant")
        if rng.random() < 0.03:
            items.append(f"{label} sans barre verticale")
    rng.shuffle(items)
    return " ; ".join(items)


def _regulation(rng: random.Random, number: int) -> list[dict]:
    """The emprises of one simulated regulation, in the standard flow's shape."""
    agency = rng.choice(AGENCIES)
    document_type = rng.choices(DOCUMENT_TYPES, weights=[75, 20, 5])[0]
    start_day = rng.randint(1, 28)
    month = rng.choice([9, 10, 11, 12])
    start = _date(rng, 2026, month, start_day, rng.choice([0, 1, 7, 22]))
    end = None
    if document_type != "ARRETE PERMANENT" or rng.random() < 0.1:
        end = rng.choice(
            [
                start,  # one-off event: same start and end
                _date(rng, 2026, 12, rng.randint(1, 30), 1),
                _date(rng, 2027, 2, 30, 1),  # rolled over by PHP to 2 March
            ]
        )
        if rng.random() < 0.08:
            end = None  # a temporary regulation without end: a permanent period
    description = rng.choice(DESCRIPTIONS)
    shorturl = rng.choices(
        [f"https://dl.sogelink.fr/?{number:08X}", None, "", "http://dl.sogelink.fr/?x"],
        weights=[88, 5, 4, 3],
    )[0]
    arretesrcid = rng.choice([f"2026-{number:04d}", f"26/{number}", f"A-{number}", f"{number}"])
    categorie = rng.choice(CATEGORIES)

    emprises = []
    for emprise_number in range(1, rng.choice([1, 1, 1, 2, 3]) + 1):
        count = rng.choice([1, 1, 2, 3])
        names = rng.sample(SUPPORTED_MEASURES, k=count)
        if rng.random() < 0.3:
            names.insert(rng.randrange(len(names) + 1), rng.choice(UNSUPPORTED_MEASURES))
        if rng.random() < 0.05:
            names.append(names[0])  # the same measure twice

        emprise_start = emprise_end = None
        if end and rng.random() < 0.2:
            emprise_start = _date(rng, 2026, month, min(start_day + 1, 28), 6)
            emprise_end = _date(rng, 2026, month, min(start_day + 3, 28), 18)

        emprise_parameters = ""
        roll = rng.random()
        if roll < 0.15 and emprise_start:
            emprise_parameters = (
                "Dates de l'emprise : Du 01/10/2026 00:00:00 au 03/10/2026 00:00:00"
            )
        elif roll < 0.25:
            emprise_parameters = "jours et horaires : " + rng.choice(HOURS)

        streets = rng.sample(STREETS, k=rng.choice([1, 1, 2]))
        idemprise = number * 10 + emprise_number
        emprises.append(
            {
                "type": "Feature",
                "id": f"litteralis.{idemprise}",
                "geometry": _geometry(rng),
                "geometry_name": "geometry",
                "properties": {
                    "idemprise": idemprise,
                    "idarrete": 850000000 + number,
                    "shorturl": shorturl,
                    "arretesrcid": arretesrcid,
                    "collectivitelibelle": "COLLECTIVITE SIMULEE",
                    "collectiviteid": None,
                    "collectiviteagencelibelle": "Agence simulée",
                    "collectiviteagenceid": agency,
                    "documenttype": document_type,
                    "arretedebut": start,
                    "arretefin": end,
                    "empriseno": emprise_number,
                    "emprisetype": "CIRCULATION",
                    "emprisedebut": emprise_start,
                    "emprisefin": emprise_end,
                    "mesures": rng.choice([";", " ; "]).join(names),
                    "localisations": rng.choice([";".join(streets), None])
                    if rng.random() < 0.1
                    else ";".join(streets),
                    "idagence": 117374,
                    "fournisseur": "LIPRIME",
                    "publicationinternet": True,
                    "emetteurlibelle": "Service simulé",
                    "emetteurid": None,
                    "categoriesmodele": categorie,
                    "nommodele": "[SIM] - Arrêté simulé",
                    "parametresarrete": (
                        "Date de réception de la demande : 01/09/2026 00:00:00 ; "
                        f"{description} ; chargé de MEP de la signalisation : Le demandeur"
                    ),
                    "parametresemprise": emprise_parameters,
                    "parametresmesures": _measure_parameters(rng, names),
                    "datecreation": "2026-09-01T10:29:53.053Z",
                    "datemodification": "2026-09-01T10:29:55.326Z",
                },
            }
        )
    return emprises


def standard_features(seed: int = 20261012, regulations: int = 200) -> list[dict]:
    rng = random.Random(seed)
    features = []
    for number in range(1, regulations + 1):
        features.extend(_regulation(rng, number))
    return features


def communication_features(seed: int = 20261013, regulations: int = 80) -> list[dict]:
    """The same regulations, one feature per measure, as `LIcommunication` serves them."""
    features = []
    for emprise in standard_features(seed, regulations):
        properties = emprise["properties"]
        names = [n.strip() for n in properties["mesures"].split(";") if n.strip()]
        parameters = [p.strip() for p in properties["parametresmesures"].split(";")]
        for index, name in enumerate(names):
            own = [
                p.split("|", 1)[1].strip()
                for p in parameters
                if "|" in p and p.split("|", 1)[0].strip() in (name, f"{name} {index + 1}")
            ]
            measure_properties = {
                k: v for k, v in properties.items() if k not in ("mesures", "parametresmesures")
            }
            measure_properties["mesure"] = name
            measure_properties["parametresmesure"] = " ; ".join(own)
            if index % 4 == 3:
                measure_properties.pop("collectiviteagenceid")
            features.append(
                {
                    "type": "Feature",
                    "id": f"LIcommunication.{properties['idemprise']}.{index}",
                    "geometry": deepcopy(emprise["geometry"]),
                    "properties": measure_properties,
                }
            )
    return features


def _fatal(name: str, mutate) -> dict:
    """Two small regulations, altered so that PHP aborts the whole import."""
    features = standard_features(seed=7, regulations=2)
    for feature in features:
        feature["geometry"] = {"type": "LineString", "coordinates": [[3.0, 50.6], [3.001, 50.601]]}
        feature["properties"].update(
            mesures="Circulation interdite",
            parametresmesures="",
            arretedebut="2026-10-01T00:00:00Z",
            arretefin="2026-12-01T00:00:00Z",
        )
        mutate(feature["properties"])
    return {"name": name, "flow": "standard", "laterThan": LATER_THAN, "features": features}


def pagination_features(count: int = 1203) -> list[dict]:
    """Enough emprises for three pages of 500, kept small: one measure each."""
    features = []
    for index in range(count):
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[3.0, 50.6 + index / 1e5], [3.001, 50.6]],
                },
                "properties": {
                    "idemprise": index,
                    "shorturl": f"https://dl.sogelink.fr/?P{index}",
                    "arretesrcid": f"P-{index // 3}",
                    "collectiviteagenceid": 117374,
                    "documenttype": "ARRETE TEMPORAIRE",
                    "arretedebut": "2026-10-01T00:00:00Z",
                    "arretefin": "2026-12-01T00:00:00Z",
                    "emprisedebut": None,
                    "emprisefin": None,
                    "mesures": "Circulation interdite",
                    "localisations": f"RUE {index}",
                    "categoriesmodele": "Travaux",
                    "nommodele": "[SIM] - Arrêté simulé",
                    "parametresarrete": "",
                    "parametresemprise": "",
                    "parametresmesures": "",
                },
            }
        )
    return features


ODD_DATES = [
    "2026-1-5T1:00:00Z",
    "026-10-31T01:00:00Z",
    "2026-10-31T01:0:00Z",
    "2026-10-31T01:00:00+2",
    "2026-10-31T01:00:00UTC",
    "2026-10-31T01:00:00GMT",
    "2026-10-31T01:00:00Europe/Paris",
    "2026-10-31T01:00:00CEST",
    "2026-10-31T01:00:00z",
    "2026-13-31T01:00:00Z",
    "2026-00-00T01:00:00Z",
    "2026-10-31T24:60:60Z",
    "2026-10-31T01:00:00-0930",
    "2026-10-31T01:00:00+1:00",
    "+2026-10-31T01:00:00Z",
    "2026-10-31T01:00:00 Z",
    "2026-10-31T01:00:00Z ",
    "2026-10-31",
    "2026-10-31T01:00:00.123456Z",
]

ODD_GEOMETRIES = [
    {"type": "LineString", "coordinates": [[3.0, 50.0], [1e-5, 1.5e20], [-0.0, 0.1], [2, 3]]},
    {
        "type": "LineString",
        "coordinates": [[3.1234567890123457, 50.1], [123456789012345678.0, 1e16]],
    },
    {
        "type": "LineString",
        "coordinates": [[3, 50], [3.5, 50.5]],
        "crs": {"type": "EPSG", "properties": {}},
    },
    {
        "type": "GeometryCollection",
        "geometries": [{"type": "Point", "coordinates": [3, 50]}],
        "name": 'é/x\x7f\x01😀\u2028"\\\\',
    },
    {"type": "LineString", "coordinates": []},
]


def formats_features() -> list[dict]:
    """Dates and geometries at the edges of what PHP parses and writes."""
    features = []
    template = pagination_features(1)[0]
    for index, value in enumerate(ODD_DATES):
        for field in ("arretedebut", "arretefin", "emprisedebut"):
            feature = deepcopy(template)
            feature["properties"].update(arretesrcid=f"D-{field}-{index}", idemprise=index)
            feature["properties"][field] = value
            if field == "emprisedebut":
                feature["properties"]["emprisefin"] = "2026-12-01T00:00:00Z"
            features.append(feature)
    for index, geometry in enumerate(ODD_GEOMETRIES):
        feature = deepcopy(template)
        feature["properties"].update(arretesrcid=f"G-{index}", idemprise=100 + index)
        feature["geometry"] = geometry
        features.append(feature)
    for index, hours in enumerate(HOURS + ["de 25h à 99h", "de 23h75 à 7h"]):
        feature = deepcopy(template)
        feature["properties"].update(
            arretesrcid=f"H-{index}",
            idemprise=200 + index,
            parametresmesures=f"Circulation interdite | jours et horaires : {hours}",
        )
        features.append(feature)
    return features


def _set(**values):
    return lambda properties: properties.update(values)


def _drop(key: str):
    return lambda properties: properties.pop(key)


def cases() -> list[dict]:
    """Every comparison case, in a stable order."""
    return [
        {
            "name": "standard",
            "flow": "standard",
            "laterThan": LATER_THAN,
            "features": standard_features(),
        },
        {
            "name": "standard-pagination",
            "flow": "standard",
            "laterThan": LATER_THAN,
            "features": pagination_features(),
        },
        {
            "name": "communication",
            "flow": "communication",
            "laterThan": LATER_THAN,
            "features": communication_features(),
        },
        {"name": "standard-empty", "flow": "standard", "laterThan": LATER_THAN, "features": []},
        {
            "name": "formats",
            "flow": "standard",
            "laterThan": LATER_THAN,
            "features": formats_features(),
        },
        # A zone at UTC+1 all year: what a run in winter does to time slots.
        {
            "name": "formats-winter-offset",
            "flow": "standard",
            "laterThan": LATER_THAN,
            "zone": "Africa/Lagos",
            "features": formats_features(),
        },
        {
            "name": "standard-winter-offset",
            "flow": "standard",
            "laterThan": LATER_THAN,
            "zone": "Africa/Lagos",
            "features": standard_features(seed=5, regulations=60),
        },
        _fatal(
            "fatal-one-digit-minute",
            _set(
                mesures="Circulation interdite",
                parametresmesures="Circulation interdite | jours et horaires : de 8h5 à 17h",
            ),
        ),
        _fatal("fatal-missing-parametresemprise", _drop("parametresemprise")),
        _fatal("fatal-null-start-date", _set(arretedebut=None, emprisedebut=None)),
        _fatal("fatal-null-mesures", _set(mesures=None)),
        _fatal(
            "fatal-null-nommodele",
            _set(nommodele=None, parametresarrete="ajout annexe : N"),
        ),
        _fatal("fatal-missing-agency", _drop("collectiviteagenceid")),
    ]


def layers() -> dict[str, list[dict]]:
    """A WFS dataset for the simulated end-to-end run: no fatal case in it."""
    return {
        "litteralis:litteralis": standard_features(regulations=60),
        "litteralis:LIcommunication": communication_features(regulations=20),
    }


# Document URLs, judged by Symfony's Url validator in PHP and by the port.
URLS = [
    "https://dl.sogelink.fr/?0dbjHha7",
    "https://dl.sogelink.fr/?RTCIgldJ",
    "http://dl.sogelink.fr/?x",
    "",
    "https://localhost/x",
    "https://127.0.0.1/x",
    "https://[::1]/x",
    "https://dl.sogelink.fr/a b",
    "https://dl.sogelink.fr/?a=é&b=ç",
    "https://é.fr/chemin",
    "https://user:pass@dl.sogelink.fr/x",
    "https://dl.sogelink.fr:8443/x#frag",
    "https://dl.sogelink.fr./x",
    "HTTPS://DL.SOGELINK.FR/X",
    "https://xn--e1afmkfd.xn--p1ai/",
    "https://dl.sogelink.fr/%zz",
    "https://dl.sogelink.fr/%2F",
    "ftp://dl.sogelink.fr",
    "https://",
    "https://a.b",
    "https://dl.sogelink.fr/ ",
    "https://exa_mple.com/x",
    "https://-a.fr",
    "https://a.fr/~x!$&'()*+,;=:@",
    "https://a.fr?q=[1]",
    "https://a.fr#a#b",
    "https://999.999.999.999/x",
    "https://1.2.3.4.5/x",
    "https://a.fr/€",
    "https://a.fr/́",
    "https://dl.sogelink.fr/?x\n",
]
