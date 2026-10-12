"""Which Litteralis organizations to import, and how to reach their WFS.

The back end's variables, renamed with this repository's `DIALOG_` prefix. They are
read from the process environment first (CI), then from `.env.dialog.{env}`:

    DIALOG_LITTERALIS_ENABLED_ORGS='["mel"]'                 standard flow, JSON list
    DIALOG_LITTERALIS_COMMUNICATION_ENABLED_ORGS='[]'        "Communication" flow
    DIALOG_LITTERALIS_ORG_MEL_CREDENTIALS='user:password'    one per organization
    DIALOG_LITTERALIS_ORG_MEL_CLIENT_ID=…                    optional, with the next one
    DIALOG_LITTERALIS_ORG_MEL_CLIENT_SECRET=…
    DIALOG_LITTERALIS_WFS_BASE_URL=https://apps.sogelink.fr  optional
    DIALOG_LITTERALIS_SIMULATION=path/to/features.json       optional, see simulation.py

The back end's `APP_LITTERALIS_ORG_<NAME>_ID` names the DiaLog organization to write to;
here the API key decides it. An organization without its own key writes with the
`dialog` organization's key. Two Litteralis organizations sharing a DiaLog organization
share its cleanup too: the second one's deletes what the first one just created, as
PHP would with two names pointing at the same organization.
"""

import json
import os
from dataclasses import dataclass, field
from types import SimpleNamespace

import httpx

from integrations.api import DialogApi, build_client
from integrations.dialog.litteralis.client import (
    TIMEOUT,
    TYPENAME_COMMUNICATION,
    TYPENAME_STANDARD,
    LitteralisClient,
)
from integrations.dialog.litteralis.executor import COMMUNICATION, STANDARD
from integrations.dialog.litteralis.simulation import load_layers, simulated_transport
from settings import OrganizationSettings

DEFAULT_WFS_BASE_URL = "https://apps.sogelink.fr"
SIMULATION_CREDENTIALS = "simulation:simulation"


@dataclass
class LitteralisConfig:
    enabled_orgs: list[str]
    communication_enabled_orgs: list[str]
    credentials: dict[str, str]
    wfs_base_url: str
    simulation: str | None
    # (client id, client secret) of the DiaLog organization a Litteralis one writes to.
    api_keys: dict[str, tuple[str, str]] = field(default_factory=dict)

    @classmethod
    def from_settings(cls, settings: OrganizationSettings) -> "LitteralisConfig":
        def read(name: str) -> str | None:
            return os.environ.get(f"DIALOG_{name}") or getattr(settings, "extra", {}).get(
                name.lower()
            )

        enabled = json.loads(read("LITTERALIS_ENABLED_ORGS") or "[]")
        communication = json.loads(read("LITTERALIS_COMMUNICATION_ENABLED_ORGS") or "[]")
        simulation = read("LITTERALIS_SIMULATION")

        credentials = {}
        api_keys: dict[str, tuple[str, str]] = {}
        # As in the back end, every organization named in either list must be complete,
        # or nothing runs.
        for name in dict.fromkeys(enabled + communication):
            variable = f"LITTERALIS_ORG_{name.upper()}_CREDENTIALS"
            value = read(variable) or (SIMULATION_CREDENTIALS if simulation else None)
            if not value:
                raise RuntimeError(f"Environment variable DIALOG_{variable} must not be empty")
            credentials[name] = value

            client_id = read(f"LITTERALIS_ORG_{name.upper()}_CLIENT_ID")
            client_secret = read(f"LITTERALIS_ORG_{name.upper()}_CLIENT_SECRET")
            if client_id and client_secret:
                api_keys[name] = (client_id, client_secret)

        return cls(
            enabled_orgs=enabled,
            communication_enabled_orgs=communication,
            credentials=credentials,
            wfs_base_url=read("LITTERALIS_WFS_BASE_URL") or DEFAULT_WFS_BASE_URL,
            simulation=simulation,
            api_keys=api_keys,
        )

    def runs(self) -> list[tuple[str, str]]:
        """(flow, organization) in the order the back end's CI runs them."""
        return [(STANDARD, name) for name in self.enabled_orgs] + [
            (COMMUNICATION, name) for name in self.communication_enabled_orgs
        ]

    def api(self, name: str, default: DialogApi, base_url: str) -> DialogApi:
        """The API of the DiaLog organization `name` writes to."""
        if name not in self.api_keys:
            return default
        client_id, client_secret = self.api_keys[name]
        keys = SimpleNamespace(base_url=base_url, client_id=client_id, client_secret=client_secret)
        return DialogApi(build_client(keys))  # type: ignore[arg-type]

    def client(self, name: str, flow: str) -> LitteralisClient:
        transport = simulated_transport(load_layers(self.simulation)) if self.simulation else None
        http = httpx.Client(base_url=self.wfs_base_url, timeout=TIMEOUT, transport=transport)
        typename = TYPENAME_COMMUNICATION if flow == COMMUNICATION else TYPENAME_STANDARD
        return LitteralisClient(http, typename, self.credentials[name])
