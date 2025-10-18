from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, TYPE_CHECKING
from zipfile import ZIP_DEFLATED, ZipFile

from .models import ActiveProxy

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .proxy_validator import ValidationResult


class ResultPublisher:
    def __init__(
        self,
        *,
        output_dir: str,
        txt_filename: str,
        json_filename: str,
        zip_filename: str,
        partial_txt_filename: str,
        partial_json_filename: str,
        active_json_filename: str,
        classified_json_filename: str,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.txt_filename = txt_filename
        self.json_filename = json_filename
        self.zip_filename = zip_filename
        self.partial_txt_filename = partial_txt_filename
        self.partial_json_filename = partial_json_filename
        self.active_json_filename = active_json_filename
        self.classified_json_filename = classified_json_filename
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
                "latency": proxy.latency,
            }
            for proxy in proxies
        ]

    def _serialize_extended(self, proxies: Sequence[ActiveProxy]) -> List[Dict[str, object]]:
        extended: List[Dict[str, object]] = []
        for proxy in proxies:
            extended.append(
                {
                    "host": proxy.host,
                    "port": proxy.port,
                    "latency": proxy.latency,
                    "protocol": proxy.protocol,
                    "egress_ip": proxy.egress_ip,
                    "endpoint": proxy.endpoint,
                    "classification": proxy.classification,
                }
            )
        return extended

    def _write_json(self, proxies: Sequence[ActiveProxy]) -> Path:
        path = self.output_dir / self.json_filename
        payload = self._serialize_active(proxies)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return path

    def _write_zip(self, files: List[Path]) -> Path:
        path = self.output_dir / self.zip_filename
        with ZipFile(path, "w", ZIP_DEFLATED) as archive:
            for file_path in files:
                archive.write(file_path, arcname=file_path.name)
        return path

    def _write_active_json(self, proxies: Sequence[ActiveProxy]) -> Path:
        path = self.output_dir / self.active_json_filename
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(proxies),
            "proxies": self._serialize_extended(proxies),
        }
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return path

    def _write_classified_json(self, report: Optional["ValidationResult"]) -> Optional[Path]:
        if report is None:
            return None
        classifications: List[Dict[str, object]] = []
        for name, proxies in sorted(report.classified.items()):
            sorted_proxies = sorted(proxies, key=lambda proxy: proxy.latency)
            classifications.append(
                {
                    "classification": name,
                    "count": len(sorted_proxies),
                    "proxies": self._serialize_extended(sorted_proxies),
                }
            )
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_candidates": report.total_candidates,
            "success_count": len(report),
            "failures_by_reason": dict(report.failures_by_reason),
            "egress_ips": dict(report.egress_ips),
            "classifications": classifications,
        }
        path = self.output_dir / self.classified_json_filename
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return path

    def _partial_txt_path(self) -> Path:
        return Path(self.partial_txt_filename)

    def _partial_json_path(self) -> Path:
        return Path(self.partial_json_filename)

    def _write_partial(self, candidates: Iterable[Tuple[str, int]], active: Sequence[ActiveProxy]) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        candidate_entries = sorted({f"{host}:{port}" for host, port in candidates})
        try:
            txt_path = self._partial_txt_path()
            txt_path.parent.mkdir(parents=True, exist_ok=True)
            with txt_path.open("w", encoding="utf-8") as handle:
                for line in candidate_entries:
                    handle.write(f"{line}\n")
            json_path = self._partial_json_path()
            json_payload = {
                "generated_at": timestamp,
                "candidate_count": len(candidate_entries),
                "active_count": len(active),
                "candidates": candidate_entries,
                "active": self._serialize_active(active),
            }
            with json_path.open("w", encoding="utf-8") as handle:
                json.dump(json_payload, handle, ensure_ascii=False, indent=2)
        except OSError as exc:
            self.logger.warning("Unable to persist partial results: %s", exc)

    def write_partial_results(self, candidates: Iterable[Tuple[str, int]], active: Sequence[ActiveProxy]) -> None:
        """Persist interim results to the working directory."""
        self._write_partial(candidates, list(active))

    def publish(
        self,
        proxies: List[ActiveProxy],
        *,
        candidates: Optional[Iterable[Tuple[str, int]]] = None,
        validation_report: Optional["ValidationResult"] = None,
    ) -> None:
        self._ensure_output_dir()
        sorted_proxies = sorted(proxies, key=lambda proxy: proxy.latency)
        txt_path = self._write_txt(sorted_proxies)
        json_path = self._write_json(sorted_proxies)
        active_path = self._write_active_json(sorted_proxies)
        classified_path = self._write_classified_json(validation_report)
        artefacts: List[Path] = [txt_path, json_path, active_path]
        if classified_path is not None:
            artefacts.append(classified_path)
        zip_path = self._write_zip(artefacts)
        artefacts.append(zip_path)
        self.logger.info(
            "Published %d proxies to %s",
            len(sorted_proxies),
            zip_path.resolve(),
        )
        # Mirror final artefacts into the working directory root for workflow uploads
        try:
            for source in artefacts:
                destination = Path(source.name)
                if destination.resolve() == source.resolve():
                    continue
                destination.write_bytes(source.read_bytes())
        except OSError as exc:
            self.logger.warning("Unable to mirror published artefacts: %s", exc)
        # Update partial outputs one final time with the active proxies and full candidate set
        if candidates is not None:
            final_candidates = sorted({tuple(candidate) for candidate in candidates})
        else:
            final_candidates = [(proxy.host, proxy.port) for proxy in sorted_proxies]
        self._write_partial(final_candidates, sorted_proxies)
