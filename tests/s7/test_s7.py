
from fastapi.testclient import TestClient


class TestS7Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """

    def test_unit_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s7/aircraft/prepare")
            assert response.status_code == 200
            assert response.json() == "OK"

    def test_unit_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s7/aircraft")
            assert response.status_code == 200
            assert isinstance(response.json(), list)

    def test_unit_positions(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}/positions")
            assert response.status_code == 200
            assert isinstance(response.json(), list)

    def test_unit_positions_invalid_icao(self, client: TestClient) -> None:
        icao = "invalid_icao"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}")
            assert response.status_code == 404
            assert response.json() == {"detail": "Not Found"}

    def test_unit_stats(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}/stats")
            assert response.status_code == 200
            assert isinstance(response.json(), dict)

    def test_unit_stats_invalid_icao(self, client: TestClient) -> None:
        icao = "invalid_icao"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}/stats")
            assert response.status_code == 200
            assert response.json() == {}


    def test_integration(self, client: TestClient) -> None:
        self.test_unit_prepare(client)
        self.test_unit_aircraft(client)
        self.test_unit_positions(client)
        self.test_unit_stats(client)
