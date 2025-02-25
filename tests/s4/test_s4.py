
import coverage
from fastapi.testclient import TestClient


class TestS4Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """

    def test_unit_download_10(self, client: TestClient, file_limit: int = 10) -> None:
        with client as client:
            response = client.post(f"/api/s1/aircraft/download?file_limit={file_limit}")
            assert response.status_code == 200
            assert response.json() == "OK"

    def test_unit_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/prepare")
            assert response.status_code == 200
            assert response.json() == "OK"


    def test_integration(self, client: TestClient) -> None:
        self.test_unit_download_10(client)
        self.test_unit_prepare(client)

    def test_coverage(self, client: TestClient) -> None:
        cov = coverage.Coverage()
        cov.start()
        self.test_integration(client)
        cov.stop()
        cov.save()
        cov.report()
