from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Iterable, List
from zipfile import ZIP_DEFLATED, ZipFile

from .models import ActiveProxy


class ResultPublisher:
    def __init__(
        self,
        *,
        output_dir: str,
        txt_filename: str,
        json_filename: str,
        zip_filename: str,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.txt_filename = txt_filename
        self.json_filename = json_filename
        self.zip_filename = zip_filename
        self.logger = logging.getLogger(__name__ + ".ResultPublisher")

    def _ensure_output_dir(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _write_txt(self, proxies: Iterable[ActiveProxy]) -> Path:
        path = self.output_dir / self.txt_filename
        with path.open("w", encoding="utf-8") as handle:
            for proxy in proxies:
                handle.write(f"{proxy.host}:{proxy.port}\n")
        return path

    def _write_json(self, proxies: Iterable[ActiveProxy]) -> Path:
        path = self.output_dir / self.json_filename
        payload = [
            {
                "host": proxy.host,
                "port": proxy.port,
                "latency": proxy.latency,
            }
            for proxy in proxies
        ]
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return path

    def _write_zip(self, files: List[Path]) -> Path:
        path = self.output_dir / self.zip_filename
        with ZipFile(path, "w", ZIP_DEFLATED) as archive:
            for file_path in files:
                archive.write(file_path, arcname=file_path.name)
        return path

    def publish(self, proxies: List[ActiveProxy]) -> None:
        self._ensure_output_dir()
        sorted_proxies = sorted(proxies, key=lambda proxy: proxy.latency)
        txt_path = self._write_txt(sorted_proxies)
        json_path = self._write_json(sorted_proxies)
        zip_path = self._write_zip([txt_path, json_path])
        self.logger.info(
            "Published %d proxies to %s",
            len(sorted_proxies),
            os.path.relpath(zip_path, os.getcwd()),
        )
