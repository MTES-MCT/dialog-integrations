from datetime import datetime

from integrations.co_paris.eudonet import EudonetClient, EudonetParisExtractor

from .schema import ParisEudonetSchema

EUDONET_PARIS_URL = "https://eudonet-partage.apps.paris.fr/eudoapi/eudoapi/records/search"
PAGE_SIZE = 1000


class ParisEudonetDataSourceIntegration:
    name = "permanent_lineaire_paris"

    def __init__(self, config):
        self.config = config
        self.schema = ParisEudonetSchema()

    def fetch_raw_data(self):
        url = self.config.get("eudonet_search_url", EUDONET_PARIS_URL)
        credentials = self.config.get("eudonet_credentials")
        eudonet_client = EudonetClient(url, credentials)
        extractor = EudonetParisExtractor(eudonet_client)

        arretes = []
        for a in extractor.iter_extract(datetime.now()):
            arretes.append(a)
        return arretes

        raise NotImplementedError()
