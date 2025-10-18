from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from proxy_collector.models import ActiveProxy
from proxy_collector.result_publisher import ResultPublisher


class ResultPublisherIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_path = Path(self.temp_dir.name)
        self.results_dir = self.base_path / "results"
        partials_dir = self.base_path / "partials"
        self.partial_txt = partials_dir / "proxies.partial.txt"
        self.partial_json = partials_dir / "proxies.partial.json"
        self.publisher = ResultPublisher(
            output_dir=str(self.results_dir),
            latest_dir_name="latest",
            retention=2,
            log_tail_bytes=0,
            txt_filename="proxies.txt",
            json_filename="proxies.json",
            zip_filename="proxies.zip",
            partial_txt_filename=str(self.partial_txt),
            partial_json_filename=str(self.partial_json),
            active_json_filename="active_proxies.json",
            classified_json_filename="classified_proxies.json",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _sample_proxies(self, seed: int) -> list[ActiveProxy]:
        return [
            ActiveProxy(host=f"1.1.1.{seed}", port=1080, latency=0.5 + seed * 0.1),
            ActiveProxy(host=f"2.2.2.{seed}", port=2080, latency=0.6 + seed * 0.1),
        ]

    def test_publish_creates_versioned_results_and_latest(self) -> None:
        for run in range(3):
            proxies = self._sample_proxies(run + 1)
            candidates = [(proxy.host, proxy.port) for proxy in proxies]
            self.publisher.publish(proxies, candidates=candidates)

        run_dirs = [
            path
            for path in self.results_dir.iterdir()
            if path.is_dir() and path.name != "latest"
        ]
        self.assertLessEqual(len(run_dirs), 2)

        latest_dir = self.results_dir / "latest"
        self.assertTrue(latest_dir.exists())
        latest_files = {path.name for path in latest_dir.iterdir()}
        self.assertTrue({"proxies.txt", "proxies.json", "active_proxies.json", "proxies.zip"} <= latest_files)

        for filename in ("proxies.txt", "proxies.json", "proxies.zip"):
            self.assertTrue((self.base_path / filename).exists(), f"{filename} not mirrored to root")

        with (latest_dir / "proxies.json").open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        self.assertIn("generated_at", payload)
        self.assertEqual(payload["total_candidates"], len(payload["candidates"]))
        self.assertLessEqual(len(payload.get("preview", [])), 3)
        self.assertLessEqual(len(payload.get("active_preview", [])), 3)
        self.assertEqual(payload["total_active"], 2)

        self.assertTrue(self.partial_txt.exists())
        self.assertTrue(self.partial_json.exists())


if __name__ == "__main__":
    unittest.main()
