from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple
from zipfile import ZIP_DEFLATED, ZipFile

from .models import ActiveProxy, ProxyClassification, ProxyValidationSummary


class ResultPublisher:
    def __init__(
        self,
        *,
        output_dir: str,
        txt_filename: str,
        active_json_filename: str,
        classified_json_filename: str,
        zip_filename: str,
        partial_txt_filename: str,
        partial_json_filename: str,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.txt_filename = txt_filename
        self.active_json_filename = active_json_filename
        self.classified_json_filename = classified_json_filename
        self.zip_filename = zip_filename
        self.partial_txt_filename = partial_txt_filename
        self.partial_json_filename = partial_json_filename
        self.logger = logging.getLogger(__name__ + ".ResultPublisher")

    def _ensure_output_dir(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _write_txt(self, proxies: Sequence[ActiveProxy]) -> Path:
        path = self.output_dir / self.txt_filename
        with path.open("w", encoding="utf-8") as handle:
            for proxy in proxies:
                handle.write(f"{proxy.host}:{proxy.port}\n")
        return path

    def _serialize_active(self, proxies: Sequence[ActiveProxy]) -> List[dict]:
        return [
            {
                "host": proxy.host,
                "port": proxy.port,
                "protocol": proxy.protocol,
                "latency": proxy.latency,
                "egress_ip": proxy.egress_ip,
                "endpoint": proxy.endpoint,
                "verified_at": proxy.verified_at,
            }
            for proxy in proxies
        ]

    def _serialize_classification(self, classification: ProxyClassification) -> dict:
        return {
            "host": classification.host,
            "port": classification.port,
            "protocol": classification.protocol,
            "requires_auth": classification.requires_auth,
            "category": classification.category,
            "latency": classification.latency,
            "endpoint": classification.endpoint,
            "egress_ip": classification.egress_ip,
            "verified_at": classification.verified_at,
            "error": classification.error,
        }

    def _write_active_json(self, proxies: Sequence[ActiveProxy]) -> Path:
        path = self.output_dir / self.active_json_filename
        payload = self._serialize_active(proxies)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return path

    def _write_classified_json(self, classified: Sequence[ProxyClassification]) -> Path:
        path = self.output_dir / self.classified_json_filename
        payload = [self._serialize_classification(item) for item in classified]
        payload.sort(key=lambda item: (item["category"], item["protocol"], item["host"], item["port"]))
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return path

    def _write_zip(self, files: Sequence[Path]) -> Path:
        path = self.output_dir / self.zip_filename
        with ZipFile(path, "w", ZIP_DEFLATED) as archive:
            for file_path in files:
                archive.write(file_path, arcname=file_path.name)
        return path

    def _partial_txt_path(self) -> Path:
        return Path(self.partial_txt_filename)

    def _partial_json_path(self) -> Path:
        return Path(self.partial_json_filename)

    def _write_partial(self, candidates: Iterable[Tuple[str, int]], summary: ProxyValidationSummary) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        candidate_entries = sorted({f"{host}:{port}" for host, port in candidates})
        sorted_active = sorted(summary.active_proxies, key=lambda proxy: proxy.latency)
        payload = {
            "generated_at": timestamp,
            "candidate_count": len(candidate_entries),
            "active_count": summary.success_count,
            "classified_count": len(summary.classified),
            "candidates": candidate_entries,
            "active": self._serialize_active(sorted_active),
            "category_counts": dict(summary.category_counts),
            "protocol_counts": dict(summary.protocol_counts),
            "failure_reasons": dict(summary.failure_reasons),
        }
        try:
            txt_path = self._partial_txt_path()
            txt_path.parent.mkdir(parents=True, exist_ok=True)
            with txt_path.open("w", encoding="utf-8") as handle:
                for line in candidate_entries:
                    handle.write(f"{line}\n")
            json_path = self._partial_json_path()
            with json_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
        except OSError as exc:
            self.logger.warning("Unable to persist partial results: %s", exc)

    def write_partial_results(self, candidates: Iterable[Tuple[str, int]], summary: ProxyValidationSummary) -> None:
        self._write_partial(candidates, summary)

    def publish(
        self,
        summary: ProxyValidationSummary,
        *,
        candidates: Optional[Iterable[Tuple[str, int]]] = None,
    ) -> None:
        self._ensure_output_dir()
        sorted_active = sorted(summary.active_proxies, key=lambda proxy: proxy.latency)
        txt_path = self._write_txt(sorted_active)
        active_json_path = self._write_active_json(sorted_active)
        classified_json_path = self._write_classified_json(summary.classified)
        zip_path = self._write_zip([txt_path, active_json_path, classified_json_path])
        self.logger.info(
            "Published %d active proxies to %s",
            len(sorted_active),
            zip_path.resolve(),
        )
        try:
            for source in (txt_path, active_json_path, classified_json_path, zip_path):
                destination = Path(source.name)
                if destination.resolve() == source.resolve():
                    continue
                destination.write_bytes(source.read_bytes())
        except OSError as exc:
            self.logger.warning("Unable to mirror published artefacts: %s", exc)
        final_candidates = list(candidates) if candidates is not None else [(proxy.host, proxy.port) for proxy in sorted_active]
        self._write_partial(final_candidates, summary)


__all__ = [
    "ResultPublisher",
]
