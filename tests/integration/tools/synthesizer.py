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

"""Developer tool that auto-generates test_spec.yaml from raw dataset CSV, MCF, and config.json files."""

import argparse
import csv
import json
import re
import sys
from pathlib import Path

# Ensure repository root is in Python sys.path
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import yaml

from tests.integration.core.config_schema import (
    ExpectedEdge,
    ExpectedNode,
    IndicatorResolutionSpec,
    IngestionManifestConfig,
    MCPAgentManifestConfig,
    MCPToolCallSpec,
    NodeQuerySpec,
    PointObservationSpec,
    PostprocessingManifestConfig,
    SDMXAvailabilityQuerySpec,
    SDMXDataQuerySpec,
    SDMXManifestConfig,
    SeriesObservationSpec,
    ServingAPIManifestConfig,
    SpecializationEdgeSpec,
    SpannerExpectations,
    SVGHierarchySpec,
    TestManifest,
)


class DatasetSynthesizer:
    """Inspects dataset directories (local or GCS gs://) and auto-generates a TestManifest."""

    def __init__(self, dataset_dirs: list[str | Path]):
        self.dataset_dirs = [str(d) for d in dataset_dirs]

    def _get_gcs_files(self, gs_url: str):
        """Helper to fetch config.json, *.mcf, and sample *.csv rows directly from GCS efficiently."""
        from google.cloud import storage

        clean_url = gs_url.replace("gs://", "")
        parts = clean_url.split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        config_data = {}
        mcf_contents = []
        csv_rows = []
        jsonld_blocks = []

        for b in blobs:
            rel_name = b.name[len(prefix):] if b.name.startswith(prefix) else b.name
            if rel_name == "config.json":
                try:
                    config_data = json.loads(b.download_as_text())
                except Exception:
                    pass
            elif rel_name.endswith(".mcf"):
                # Read MCF files for schema definition
                try:
                    mcf_contents.append(b.download_as_text())
                except Exception:
                    pass
            elif rel_name.endswith(".jsonld") and len(jsonld_blocks) < 500:
                try:
                    data = json.loads(b.download_as_text())
                    graph = data.get("@graph", [])
                    jsonld_blocks.extend(graph)
                except Exception:
                    pass
            elif rel_name.endswith(".csv") and len(csv_rows) < 30:
                # Efficient byte-range request: pull only first 10KB of CSV to sample header & rows
                try:
                    chunk = b.download_as_bytes(start=0, end=10240).decode("utf-8", errors="ignore")
                    lines = chunk.splitlines()
                    if len(lines) > 1:
                        # Drop last incomplete line
                        lines = lines[:-1]
                    reader = csv.DictReader(lines)
                    for i, row in enumerate(reader):
                        if i >= 10:
                            break
                        csv_rows.append(row)
                except Exception:
                    pass

        return config_data, mcf_contents, csv_rows, jsonld_blocks

    def synthesize(
        self,
        manifest_name: str | None = None,
        existing_manifest: TestManifest | None = None,
    ) -> TestManifest:
        expected_nodes: list[ExpectedNode] = []
        expected_edges: list[ExpectedEdge] = []
        specialization_edges: list[SpecializationEdgeSpec] = []
        indicator_resolutions: list[IndicatorResolutionSpec] = []
        point_observations: list[PointObservationSpec] = []
        series_observations: list[SeriesObservationSpec] = []
        node_queries: list[NodeQuerySpec] = []
        sdmx_data_queries: list[SDMXDataQuerySpec] = []
        sdmx_availability_queries: list[SDMXAvailabilityQuerySpec] = []
        mcp_tool_calls: list[MCPToolCallSpec] = []

        all_stat_vars: set[str] = set()
        all_svgs: set[str] = set()
        all_provenances: set[str] = set()
        all_places: set[str] = set()

        # Extract previously anchored StatVars from existing manifest if present
        existing_anchors: set[str] = set()
        if existing_manifest and existing_manifest.serving_api:
            for obs in existing_manifest.serving_api.point_observations:
                existing_anchors.update(obs.variables)

        for d_str in self.dataset_dirs:
            if d_str.startswith("gs://"):
                cfg_data, mcf_contents, csv_rows, jsonld_blocks = self._get_gcs_files(d_str)

                # Provenances from config.json
                for item in cfg_data.get("inputFiles", []):
                    prov = item.get("provenance", "").replace("dcid:", "")
                    if prov:
                        all_provenances.add(prov)

                # JSON-LD Graph parsing
                for item in jsonld_blocks:
                    itype = item.get("@type", "").replace("dcid:", "")
                    if itype == "StatVarObservation":
                        sv = item.get("dcid:variableMeasured", {}).get("@id", "").replace("dcid:", "")
                        place = item.get("dcid:observationAbout", {}).get("@id", "").replace("dcid:", "")
                        prov = item.get("dcid:provenance", {}).get("@id", "").replace("dcid:", "")
                        if sv:
                            all_stat_vars.add(sv)
                        if place:
                            all_places.add(place)
                        if prov:
                            all_provenances.add(prov)
                    elif itype in ("StatisticalVariable", "StatVarGroup", "Provenance"):
                        node_id = item.get("@id", "").replace("dcid:", "")
                        if node_id:
                            expected_nodes.append(ExpectedNode(subject_id=node_id, expected_types=[itype]))
                            if itype in ("StatisticalVariable", "StatVar"):
                                all_stat_vars.add(node_id)
                            elif itype == "StatVarGroup":
                                all_svgs.add(node_id)

                # MCF & JSON-LD parsing
                for content in mcf_contents:
                    for block in content.split("\n\n"):
                        node_match = re.search(r"Node:\s*dcid:([^\s]+)", block)
                        type_match = re.search(r"typeOf:\s*(?:dcid:|dcs:)?([^\s]+)", block)
                        name_match = re.search(r'name:\s*"([^"]+)"', block)
                        spec_match = re.search(r"specializationOf:\s*(?:dcid:|dcs:)?([^\s]+)", block)
                        member_match = re.search(r"memberOf:\s*(?:dcid:|dcs:)?([^\s]+)", block)

                        if node_match:
                            node_id = node_match.group(1)
                            raw_type = type_match.group(1) if type_match else "Node"
                            clean_type = raw_type.split(":")[-1]
                            expected_nodes.append(
                                ExpectedNode(
                                    subject_id=node_id, expected_types=[raw_type]
                                )
                            )

                            if spec_match:
                                parent_id = spec_match.group(1)
                                expected_edges.append(
                                    ExpectedEdge(
                                        subject_id=node_id,
                                        predicate="specializationOf",
                                        object_id=parent_id,
                                    )
                                )
                                specialization_edges.append(
                                    SpecializationEdgeSpec(
                                        subject_id=node_id, parent_svg=parent_id
                                    )
                                )

                            if member_match:
                                group_id = member_match.group(1)
                                expected_edges.append(
                                    ExpectedEdge(
                                        subject_id=node_id,
                                        predicate="memberOf",
                                        object_id=group_id,
                                    )
                                )

                            if clean_type in ("StatisticalVariable", "StatVar"):
                                all_stat_vars.add(node_id)
                                human_name = (
                                    name_match.group(1)
                                    if name_match
                                    else node_id.split("/")[-1].replace("_", " ")
                                )
                                indicator_resolutions.append(
                                    IndicatorResolutionSpec(
                                        query=human_name.lower(),
                                        expected_candidate_dcids=[node_id],
                                    )
                                )
                                mcp_tool_calls.append(
                                    MCPToolCallSpec(
                                        tool_name="search_indicators",
                                        arguments={"query": human_name.lower()},
                                        expected_match_dcids=[node_id],
                                    )
                                )
                            elif clean_type == "StatVarGroup":
                                all_svgs.add(node_id)
                                human_name = name_match.group(1) if name_match else node_id
                                node_queries.append(
                                    NodeQuerySpec(
                                        node_dcid=node_id,
                                        expression="->name",
                                        expected_values=[human_name] if human_name else [],
                                    )
                                )
                            elif clean_type in ("Provenance", "Source"):
                                all_provenances.add(node_id)

                # CSV Places sampling
                entity_col = "observationAbout"
                for item in cfg_data.get("inputFiles", []):
                    col_map = item.get("columnMappings", {})
                    if "dcid:observationAbout" in col_map:
                        entity_col = col_map["dcid:observationAbout"]
                        break

                for row in csv_rows:
                    entity = row.get(entity_col)
                    if entity:
                        clean_ent = entity.replace("dcid:", "")
                        all_places.add(clean_ent)

                continue

            d_path = Path(d_str).resolve()
            if not d_path.exists() or not d_path.is_dir():
                continue

            # 1. Parse config.json
            config_file = d_path / "config.json"
            cfg_data = {}
            if config_file.exists():
                try:
                    with open(config_file, encoding="utf-8") as f:
                        cfg_data = json.load(f)
                    for item in cfg_data.get("inputFiles", []):
                        prov = item.get("provenance", "").replace("dcid:", "")
                        if prov:
                            all_provenances.add(prov)
                except Exception:
                    pass

            # 2. Parse MCF files for Node DCIDs & types
            for mcf_file in d_path.glob("*.mcf"):
                try:
                    with open(mcf_file, encoding="utf-8") as f:
                        content = f.read()

                    for block in content.split("\n\n"):
                        node_match = re.search(r"Node:\s*dcid:([^\s]+)", block)
                        type_match = re.search(r"typeOf:\s*dcid:([^\s]+)", block)
                        name_match = re.search(r'name:\s*"([^"]+)"', block)

                        if node_match:
                            node_id = node_match.group(1)
                            node_type = type_match.group(1) if type_match else "Node"
                            expected_nodes.append(
                                ExpectedNode(
                                    subject_id=node_id, expected_types=[node_type]
                                )
                            )

                            if node_type in ("StatisticalVariable", "StatVar"):
                                all_stat_vars.add(node_id)
                                human_name = (
                                    name_match.group(1)
                                    if name_match
                                    else node_id.replace("_", " ")
                                )
                                indicator_resolutions.append(
                                    IndicatorResolutionSpec(
                                        query=human_name.lower(),
                                        expected_candidate_dcids=[node_id],
                                    )
                                )
                                mcp_tool_calls.append(
                                    MCPToolCallSpec(
                                        tool_name="search_indicators",
                                        arguments={"query": human_name.lower()},
                                        expected_match_dcids=[node_id],
                                    )
                                )
                            elif node_type in ("Provenance", "Source"):
                                all_provenances.add(node_id)
                except Exception:
                    pass

            # 3. Sample CSV files for observationAbout places using columnMappings from config.json
            entity_col = "observationAbout"
            if config_file.exists():
                try:
                    for item in cfg_data.get("inputFiles", []):
                        col_map = item.get("columnMappings", {})
                        if "dcid:observationAbout" in col_map:
                            entity_col = col_map["dcid:observationAbout"]
                            break
                except Exception:
                    pass

            for csv_file in d_path.glob("*.csv"):
                try:
                    with open(csv_file, encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        for i, row in enumerate(reader):
                            if i > 10:
                                break
                            entity = row.get(entity_col)
                            if entity:
                                clean_ent = entity.replace("dcid:", "")
                                all_places.add(clean_ent)
                except Exception:
                    pass

        # Stratified Sampling: anchor by (Provenance, Source Filename Stem) to ensure zero sample shift across runs
        topic_map: dict[str, list[str]] = {}
        for sv in sorted(all_stat_vars):
            # Extract (provenance, filename_stem) key (e.g. 'desagender/ABR_ADO_RATE' from 'undata/desagender/ABR_ADO_RATE.AGE--Y10T14__SEX--F')
            parts = sv.split(".")[0].split("/")
            prov_file_key = "/".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
            topic_map.setdefault(prov_file_key, []).append(sv)

        sampled_stat_vars: list[str] = []
        for prov_file_key, sv_list in topic_map.items():
            # Incremental Anchoring: If an existing anchor from a prior manifest exists for this category, preserve it!
            anchored_sv = next((sv for sv in sv_list if sv in existing_anchors), sv_list[0])
            sampled_stat_vars.append(anchored_sv)
            if len(sampled_stat_vars) >= 8:
                break

        if not sampled_stat_vars:
            sampled_stat_vars = list(all_stat_vars)[:5]

        sample_places = list(all_places)[:3] or ["country/USA"]
        for sv in sampled_stat_vars:
            point_observations.append(
                PointObservationSpec(
                    observation_about=sample_places,
                    variables=[sv],
                    date="LATEST",
                    expected_places_with_data=sample_places[:1],
                )
            )
            node_queries.append(
                NodeQuerySpec(
                    node_dcid=sv,
                    expression="->name",
                )
            )
            sdmx_data_queries.append(
                SDMXDataQuerySpec(
                    dataflow="DC/DF_OBS/1.0.0/*",
                    constraints={
                        "variableMeasured": sv,
                        "observationAbout": sample_places[0],
                    },
                    expected_csv_contains=[sv],
                )
            )

        name = manifest_name or (
            Path(self.dataset_dirs[0]).name if self.dataset_dirs else "custom_dataset"
        )
        rel_dirs = [str(d) for d in self.dataset_dirs]

        for sv in list(all_stat_vars)[:5]:
            series_observations.append(
                SeriesObservationSpec(
                    observation_about=sample_places,
                    variables=[sv],
                    min_series_length=1,
                )
            )
            sdmx_availability_queries.append(
                SDMXAvailabilityQuerySpec(
                    dataflow="DC/DF_OBS/1.0.0/*",
                    constraints={
                        "variableMeasured": sv,
                        "observationAbout": sample_places[0],
                    },
                )
            )

        return TestManifest(
            name=name,
            description=f"Auto-synthesized test manifest for {len(self.dataset_dirs)} dataset directories",
            ingestion=IngestionManifestConfig(
                dataset_dirs=rel_dirs,
                spanner_expectations=SpannerExpectations(
                    min_observation_count=1,
                    expected_nodes=expected_nodes[:15],
                    expected_edges=expected_edges[:10],
                ),
            ),
            postprocessing=PostprocessingManifestConfig(
                svg_hierarchy=SVGHierarchySpec(
                    expected_specialization_edges=specialization_edges[:5],
                ),
                indicator_resolutions=indicator_resolutions[:5],
            ),
            serving_api=ServingAPIManifestConfig(
                nodes=node_queries[:5],
                point_observations=point_observations,
                series_observations=series_observations,
                sdmx_3_0=SDMXManifestConfig(
                    data_queries=sdmx_data_queries[:3],
                    availability_queries=sdmx_availability_queries[:3],
                ),
            ),
            mcp_agent=MCPAgentManifestConfig(
                tool_calls=mcp_tool_calls[:3],
            ),
        )

    def save_yaml(self, output_file: str | Path):
        from dataclasses import asdict

        out_path = Path(output_file).resolve()
        existing_manifest = None
        if out_path.is_file():
            try:
                from tests.integration.core.config_schema import load_test_manifest

                existing_manifest = load_test_manifest(out_path)
            except Exception:
                pass

        manifest = self.synthesize(existing_manifest=existing_manifest)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(asdict(manifest), f, sort_keys=False)
        print(f"✔ Auto-synthesized test spec written to: {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Auto-synthesize test_spec.yaml from raw dataset directory (local or GCS gs://)"
    )
    parser.add_argument(
        "dataset_dir",
        type=str,
        nargs="+",
        help="Path(s) or GCS URI(s) to raw dataset directories (e.g. gs://bucket/path or /path/to/dataset)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path for test_spec.yaml (defaults to tests/integration/manifests/custom_dataset.yaml)",
    )

    args = parser.parse_args()
    resolved_dirs = []
    for d_str in args.dataset_dir:
        if not d_str.startswith("gs://"):
            d_path = Path(d_str).resolve()
            if not d_path.exists() or not d_path.is_dir():
                print(f"Warning: Local dataset directory '{d_str}' does not exist, skipping.")
                continue
            resolved_dirs.append(str(d_path))
        else:
            resolved_dirs.append(d_str)

    if not resolved_dirs:
        print("Error: No valid dataset directories provided.")
        sys.exit(1)

    if args.output:
        out_file = Path(args.output).resolve()
    else:
        folder_name = resolved_dirs[0].rstrip("/").split("/")[-1] or "custom_dataset"
        out_file = REPO_ROOT / "tests" / "integration" / "manifests" / f"{folder_name}.yaml"

    synthesizer = DatasetSynthesizer(resolved_dirs)
    synthesizer.save_yaml(out_file)


if __name__ == "__main__":
    main()
