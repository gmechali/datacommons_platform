# Copyright 2026 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reads raw input datasets and processed graph artifacts from local filesystem or GCS buckets."""

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from google.cloud import storage

# Constants to eliminate magic strings
CONFIG_JSON_FILENAME = "config.json"
MCF_EXTENSION = ".mcf"
CSV_EXTENSION = ".csv"
JSONLD_EXTENSION = ".jsonld"
NODE_FILE_PREFIX = "node-"
OBSERVATION_FILE_PREFIX = "observation-"

MAX_JSONLD_OBSERVATION_BLOCKS = 1000
MAX_CSV_SAMPLE_ROWS = 30
CSV_BYTE_RANGE_BYTES = 10240
ROWS_PER_CSV_FILE = 10


@dataclass
class DatasetContent:
    """Container holding raw schema definitions, observations, and node metadata extracted from dataset files."""

    config_data: dict[str, Any] = field(default_factory=dict)
    mcf_contents: list[str] = field(default_factory=list)
    csv_rows: list[dict[str, str]] = field(default_factory=list)
    jsonld_blocks: list[dict[str, Any]] = field(default_factory=list)


class DatasetReader:
    """Handles reading dataset files directly from local filesystem paths or GCS buckets (gs://)."""

    def __init__(self, dataset_dirs: list[str]):
        self.dataset_dirs = dataset_dirs
        self.storage_client: storage.Client | None = None

    def _get_storage_client(self) -> storage.Client:
        if not self.storage_client:
            self.storage_client = storage.Client()
        return self.storage_client

    def read_gcs_directory(self, gcs_uri: str) -> DatasetContent:
        """Reads dataset files from GCS using byte-range HTTP streaming for ultra-fast CSV header sampling."""
        client = self._get_storage_client()
        clean_uri = gcs_uri.replace("gs://", "")
        parts = clean_uri.split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        if prefix and not prefix.endswith("/"):
            prefix += "/"

        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        jsonld_blobs = [b for b in blobs if b.name.endswith(JSONLD_EXTENSION)]
        node_schema_blobs = [
            b for b in jsonld_blobs if f"/{NODE_FILE_PREFIX}" in b.name or Path(b.name).name.startswith(NODE_FILE_PREFIX)
        ]
        observation_blobs = [b for b in jsonld_blobs if b not in node_schema_blobs]

        dataset_content = DatasetContent()
        for blob in blobs:
            relative_name = blob.name[len(prefix) :] if blob.name.startswith(prefix) else blob.name

            if relative_name == CONFIG_JSON_FILENAME:
                self._parse_gcs_config_json(blob, dataset_content)
                continue

            if relative_name.endswith(MCF_EXTENSION):
                self._parse_gcs_mcf_file(blob, dataset_content)
                continue

            if relative_name.endswith(CSV_EXTENSION) and len(dataset_content.csv_rows) < MAX_CSV_SAMPLE_ROWS:
                self._parse_gcs_csv_header(blob, dataset_content)
                continue

        # Parse node schema definitions first (StatisticalVariable, StatVarGroup, Provenance)
        for blob in node_schema_blobs:
            self._parse_gcs_jsonld_graph(blob, dataset_content)

        # Parse data observations (up to 30 observation blobs)
        for blob in observation_blobs[:30]:
            self._parse_gcs_jsonld_graph(blob, dataset_content)

        return dataset_content

    def read_local_directory(self, local_path: Path) -> DatasetContent:
        """Reads dataset files from a local directory."""
        dataset_content = DatasetContent()
        config_file = local_path / CONFIG_JSON_FILENAME
        if config_file.exists():
            try:
                with open(config_file, encoding="utf-8") as f:
                    dataset_content.config_data = json.load(f)
            except Exception:
                pass

        for mcf_file in local_path.glob(f"*{MCF_EXTENSION}"):
            try:
                with open(mcf_file, encoding="utf-8") as f:
                    dataset_content.mcf_contents.append(f.read())
            except Exception:
                pass

        # Read ALL node schema definitions first
        for jsonld_file in local_path.glob(f"{NODE_FILE_PREFIX}*{JSONLD_EXTENSION}"):
            self._parse_local_jsonld_file(jsonld_file, dataset_content)

        # Read observations (up to 30 observation files)
        local_obs_files = list(local_path.glob(f"{OBSERVATION_FILE_PREFIX}*{JSONLD_EXTENSION}"))
        for jsonld_file in local_obs_files[:30]:
            self._parse_local_jsonld_file(jsonld_file, dataset_content)

        for csv_file in local_path.glob(f"*{CSV_EXTENSION}"):
            if len(dataset_content.csv_rows) >= MAX_CSV_SAMPLE_ROWS:
                break
            try:
                with open(csv_file, encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for i, row in enumerate(reader):
                        if i >= ROWS_PER_CSV_FILE:
                            break
                        dataset_content.csv_rows.append(row)
            except Exception:
                pass

        return dataset_content

    @staticmethod
    def _parse_gcs_config_json(blob: storage.Blob, target_content: DatasetContent) -> None:
        try:
            target_content.config_data = json.loads(blob.download_as_text())
        except Exception:
            pass

    @staticmethod
    def _parse_gcs_mcf_file(blob: storage.Blob, target_content: DatasetContent) -> None:
        try:
            target_content.mcf_contents.append(blob.download_as_text())
        except Exception:
            pass

    @staticmethod
    def _parse_gcs_csv_header(blob: storage.Blob, target_content: DatasetContent) -> None:
        try:
            raw_bytes = blob.download_as_bytes(start=0, end=CSV_BYTE_RANGE_BYTES)
            chunk_text = raw_bytes.decode("utf-8", errors="ignore")
            lines = chunk_text.splitlines()
            if len(lines) > 1:
                lines = lines[:-1]
            csv_reader = csv.DictReader(lines)
            for i, row in enumerate(csv_reader):
                if i >= ROWS_PER_CSV_FILE:
                    break
                target_content.csv_rows.append(row)
        except Exception:
            pass

    @staticmethod
    def _parse_gcs_jsonld_graph(blob: storage.Blob, target_content: DatasetContent) -> None:
        try:
            graph_data = json.loads(blob.download_as_text())
            graph_nodes = graph_data.get("@graph", [])
            target_content.jsonld_blocks.extend(graph_nodes)
        except Exception:
            pass

    @staticmethod
    def _parse_local_jsonld_file(file_path: Path, target_content: DatasetContent) -> None:
        try:
            with open(file_path, encoding="utf-8") as f:
                graph_data = json.load(f)
                target_content.jsonld_blocks.extend(graph_data.get("@graph", []))
        except Exception:
            pass
