"""A zone regulation is created in four calls: POST draft, GET sections, DELETE, POST filtered."""

import json
from types import SimpleNamespace

import pytest

from api.dia_log_client.models import PostApiRegulationsAddBody, PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration
from integrations.shared.zone_sections import MAX_SECTIONS_PER_LENGTH, MIN_SECTION_LENGTH_M
from integrations.zone_flow import create_zone_regulation as _create
from integrations.zone_flow import has_zone

# In Lyon, 0.001° of longitude is ~78 m and 0.00003° is ~2.3 m.
LONG = [[4.83, 45.76], [4.831, 45.76]]
SHORT = [[4.84, 45.76], [4.84003, 45.76]]
COMPUTED = json.dumps({"type": "MultiLineString", "coordinates": [LONG, SHORT]})
# Four road lengths inside a polygon one road long: several roads side by side.
PARALLEL = json.dumps({"type": "MultiLineString", "coordinates": [LONG, LONG, LONG, LONG]})
POLYGON = json.dumps(
    {
        "type": "Polygon",
        "coordinates": [
            [[4.83, 45.76], [4.831, 45.76], [4.831, 45.76007], [4.83, 45.76007], [4.83, 45.76]]
        ],
    }
)


def zone_regulation(status="published") -> PostApiRegulationsAddBody:
    return PostApiRegulationsAddBody.from_dict(
        {
            "identifier": "MGL-CHP-1",
            "status": status,
            "category": "temporaryRegulation",
            "subject": "roadMaintenance",
            "title": "Travaux – Rue X",
            "measures": [
                {
                    "type": "noEntry",
                    "vehicleSet": {"allVehicles": True},
                    "periods": [],
                    "locations": [
                        {"roadType": "zone", "zone": {"label": "Rue X – Lyon", "geometry": POLYGON}}
                    ],
                }
            ],
        }
    )


def create_zone_regulation(api, regulation):
    return _create(
        api,
        regulation,
        min_length_m=MIN_SECTION_LENGTH_M,
        max_sections_per_length=MAX_SECTIONS_PER_LENGTH,
    )


class FakeApi:
    """Stands in for `DialogApi`: records the calls, answers as configured."""

    def __init__(self, computed=COMPUTED, get_ok=True, delete_ok=True, publish_ok=True):
        self.calls: list[str] = []
        self.posts: list[dict] = []
        self.computed, self.get_ok, self.delete_ok, self.publish_ok = (
            computed,
            get_ok,
            delete_ok,
            publish_ok,
        )
        self.refuse_raw_geojson = False
        self.refuse_all = False

    def add(self, regulation):
        payload = regulation.to_dict()
        self.posts.append(payload)
        self.calls.append("post")
        if self.refuse_all:
            return False
        road_type = payload["measures"][0]["locations"][0]["roadType"]
        return not (self.refuse_raw_geojson and road_type == "rawGeoJSON")

    def get(self, identifier):
        self.calls.append("get")
        if not self.get_ok:
            return None
        return {"measures": [{"locations": [{"roadType": "zone", "geometry": self.computed}]}]}

    def delete(self, identifier):
        self.calls.append("delete")
        return self.delete_ok

    def publish(self, identifier):
        self.calls.append("publish")
        return self.publish_ok


def test_a_zone_regulation_is_recognised():
    assert has_zone(zone_regulation())


def test_zone_is_drafted_read_deleted_and_recreated_with_its_long_sections():
    api = FakeApi()
    assert create_zone_regulation(api, zone_regulation()) == "created"  # type: ignore[arg-type]

    assert api.calls == ["post", "get", "delete", "post"]
    draft, final = api.posts
    assert draft["status"] == "draft"
    assert draft["measures"][0]["locations"][0]["roadType"] == "zone"
    assert final["identifier"] == "MGL-CHP-1"
    assert final["status"] == "published"
    (location,) = final["measures"][0]["locations"]
    assert location["roadType"] == "rawGeoJSON"
    assert location["rawGeoJSON"]["label"] == "Rue X – Lyon"
    assert json.loads(location["rawGeoJSON"]["geometry"])["coordinates"] == [LONG]


def test_when_the_sections_cannot_be_read_the_draft_is_published_as_is():
    api = FakeApi(get_ok=False)
    assert create_zone_regulation(api, zone_regulation()) == "created"  # type: ignore[arg-type]
    assert api.calls == ["post", "get", "publish"]


def test_when_the_draft_cannot_be_deleted_it_is_published_as_is():
    api = FakeApi(delete_ok=False)
    assert create_zone_regulation(api, zone_regulation()) == "created"  # type: ignore[arg-type]
    assert api.calls == ["post", "get", "delete", "publish"]


def test_when_the_sections_are_refused_the_zone_is_recreated():
    api = FakeApi()
    api.refuse_raw_geojson = True
    assert create_zone_regulation(api, zone_regulation()) == "created"  # type: ignore[arg-type]

    assert api.calls == ["post", "get", "delete", "post", "post"]
    assert api.posts[1]["measures"][0]["locations"][0]["roadType"] == "rawGeoJSON"
    assert api.posts[2]["measures"][0]["locations"][0]["roadType"] == "zone"
    assert api.posts[2]["status"] == "published"


def test_when_the_draft_is_refused_nothing_else_is_tried():
    api = FakeApi()
    api.refuse_all = True
    assert create_zone_regulation(api, zone_regulation()) == "failed"  # type: ignore[arg-type]
    assert api.calls == ["post"]


def test_a_draft_target_status_needs_no_promotion():
    api = FakeApi(get_ok=False)
    assert create_zone_regulation(api, zone_regulation(status="draft")) == "created"  # type: ignore[arg-type]
    assert api.calls == ["post", "get"]


def test_a_zone_covering_parallel_roads_is_refused_and_its_draft_deleted():
    api = FakeApi(computed=PARALLEL)
    assert create_zone_regulation(api, zone_regulation()) == "refused"  # type: ignore[arg-type]
    assert api.calls == ["post", "get", "delete"]


# --- routing from the orchestrator --------------------------------------------------


@pytest.fixture
def integration():
    it = BaseIntegration(organization_settings=SimpleNamespace(), client=None)  # type: ignore[arg-type]
    it.status = PostApiRegulationsAddBodyStatus.PUBLISHED
    it.api = FakeApi()  # type: ignore[assignment]
    return it


def test_the_flag_routes_zone_regulations_through_the_flow(integration):
    integration.resolve_zones_to_sections = True
    integration._integrate_regulations_add([zone_regulation()])
    assert integration.api.calls == ["post", "get", "delete", "post"]


def test_the_flag_off_keeps_the_single_post(integration):
    integration.resolve_zones_to_sections = False
    integration._integrate_regulations_add([zone_regulation()])
    assert integration.api.calls == ["post"]
    assert integration.api.posts[0]["status"] == "published"


def test_a_regulation_without_zone_is_posted_once_whatever_the_flag(integration):
    integration.resolve_zones_to_sections = True
    regulation = zone_regulation()
    regulation.measures[0].locations[0] = type(regulation.measures[0].locations[0]).from_dict(  # type: ignore
        {"roadType": "rawGeoJSON", "rawGeoJSON": {"label": "x", "geometry": COMPUTED}}
    )
    integration._integrate_regulations_add([regulation])
    assert integration.api.calls == ["post"]
