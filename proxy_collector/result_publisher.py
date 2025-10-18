from __future__ import annotations

import json
import logging
import shutil
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
        latest_dir_name: str,
        retention: int,
        log_tail_bytes: int,
        txt_filename: str,
        json_filename: str,
        zip_filename: str,
        partial_txt_filename: str,
        partial_json_filename: str,
        active_json_filename: str,
        classified_json_filename: str,
    ) -> None:
        self.base_dir = Path(output_dir)
        self.latest_dir_name = latest_dir_name or "latest"
        self.retention = max(1, int(retention))
        self.log_tail_bytes = max(0, int(log_tail_bytes))
        self.txt_filename = txt_filename
        self.json_filename = json_filename
        self.zip_filename = zip_filename
        self.partial_txt_filename = partial_txt_filename
        self.partial_json_filename = partial_json_filename
        self.active_json_filename = active_json_filename
        self.classified_json_filename = classified_json_filename
        self.logger = logging.getLogger(__name__ + ".ResultPublisher")

    def _ensure_base_dir(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _timestamped_run_dir(self, timestamp: datetime) -> Path:
        base_label = timestamp.strftime("%Y%m%d-%H%M")
        run_dir = self.base_dir / base_label
        suffix = 1
        while run_dir.exists():
            suffix += 1
            run_dir = self.base_dir / f"{base_label}-{suffix}"
        return run_dir

    def _write_txt(self, proxies: Sequence[ActiveProxy], directory: Path) -> Path:
        path = directory / self.txt_filename
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

    def _write_json(
        self,
        proxies: Sequence[ActiveProxy],
        candidates: Sequence[str],
        directory: Path,
        generated_at: str,
    ) -> Path:
        path = directory / self.json_filename
        active_payload = self._serialize_active(proxies)
        payload = {
            "generated_at": generated_at,
            "total_candidates": len(candidates),
            "total_active": len(proxies),
            "preview": list(candidates[:3]),
            "active_preview": active_payload[:3],
            "active": active_payload,
            "candidates": list(candidates),
        }
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return path

    def _write_zip(self, directory: Path, files: Sequence[Path]) -> Path:
        path = directory / self.zip_filename
        with ZipFile(path, "w", ZIP_DEFLATED) as archive:
            for file_path in files:
                archive.write(file_path, arcname=file_path.name)
        return path

    def _write_active_json(
        self,
        proxies: Sequence[ActiveProxy],
        directory: Path,
        generated_at: str,
    ) -> Path:
        path = directory / self.active_json_filename
        payload = {
            "generated_at": generated_at,
            "count": len(proxies),
            "proxies": self._serialize_extended(proxies),
        }
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return path

    def _write_classified_json(
        self,
        report: Optional["ValidationResult"],
        directory: Path,
        generated_at: str,
    ) -> Optional[Path]:
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
            "generated_at": generated_at,
            "total_candidates": report.total_candidates,
            "success_count": len(report),
            "failures_by_reason": dict(report.failures_by_reason),
            "egress_ips": dict(report.egress_ips),
            "classifications": classifications,
        }
        path = directory / self.classified_json_filename
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

    def _maybe_copy_run_log(self, run_dir: Path) -> Optional[Path]:
        log_path = Path("logs") / "app.log"
        if not log_path.exists():
            return None
        try:
            data = log_path.read_bytes()
            if self.log_tail_bytes and len(data) > self.log_tail_bytes:
                data = data[-self.log_tail_bytes :]
            run_log = run_dir / "run.log"
            run_log.write_bytes(data)
            return run_log
        except OSError as exc:
            self.logger.warning("Unable to copy run log: %s", exc)
            return None

    def _mirror_to_root(self, artefacts: Sequence[Path]) -> None:
        for source in artefacts:
            try:
                destination = Path(source.name)
                if destination.resolve() == source.resolve():
                    continue
                destination.write_bytes(source.read_bytes())
            except OSError as exc:
                self.logger.warning("Unable to mirror artefact %s: %s", source, exc)

    def _copy_to_latest(self, run_dir: Path) -> None:
        latest_dir = self.base_dir / self.latest_dir_name
        if latest_dir.exists():
            shutil.rmtree(latest_dir, ignore_errors=True)
        shutil.copytree(run_dir, latest_dir)

    def _prune_history(self) -> None:
        entries = [
            path
            for path in self.base_dir.iterdir()
            if path.is_dir() and path.name != self.latest_dir_name
        ]
        if len(entries) <= self.retention:
            return
        entries.sort(key=lambda item: item.name)
        for obsolete in entries[:-self.retention]:
            shutil.rmtree(obsolete, ignore_errors=True)

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
        self._ensure_base_dir()
        sorted_proxies = sorted(proxies, key=lambda proxy: proxy.latency)
        timestamp = datetime.now(timezone.utc)
        generated_at = timestamp.isoformat()
        run_dir = self._timestamped_run_dir(timestamp)
        run_dir.mkdir(parents=True, exist_ok=True)

        if candidates is not None:
            candidate_pairs = sorted({(str(host), int(port)) for host, port in candidates})
        else:
            candidate_pairs = [(proxy.host, proxy.port) for proxy in sorted_proxies]
        candidate_entries = [f"{host}:{port}" for host, port in candidate_pairs]

        txt_path = self._write_txt(sorted_proxies, run_dir)
        json_path = self._write_json(sorted_proxies, candidate_entries, run_dir, generated_at)
        active_path = self._write_active_json(sorted_proxies, run_dir, generated_at)
        classified_path = self._write_classified_json(validation_report, run_dir, generated_at)
        log_path = self._maybe_copy_run_log(run_dir)

        artefacts: List[Path] = [txt_path, json_path, active_path]
        if classified_path is not None:
            artefacts.append(classified_path)
        if log_path is not None:
            artefacts.append(log_path)
        zip_path = self._write_zip(run_dir, artefacts)
        artefacts.append(zip_path)

        self.logger.info(
            "Published %d proxies to %s",
            len(sorted_proxies),
            zip_path.resolve(),
        )

        self._mirror_to_root(artefacts)
        try:
            self._copy_to_latest(run_dir)
        except OSError as exc:
            self.logger.warning("Unable to refresh latest results directory: %s", exc)
        self._prune_history()

        final_candidates = candidate_pairs
        self._write_partial(final_candidates, sorted_proxies)
