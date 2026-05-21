from .eudonet.data_source_integration import ParisEudonetDataSourceIntegration

class ParisIntegration:
    source_name = "co_paris"

    def __init__(self, config):
        self.config = config
        self.data_sources = [
            ParisEudonetDataSourceIntegration(config)
        ]

    def run(self):
        for source in self.data_sources:
            source.run()