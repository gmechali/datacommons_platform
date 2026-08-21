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

"""ManifestBuilder: Constructs declarative TestManifest dataclasses from parsed dataset content."""

import re
from dataclasses import asdict
from pathlib import Path
from typing import Any
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
    SpannerExpectations,
    SVGHierarchySpec,
    SpecializationEdgeSpec,
    TestManifest,
)
from tests.integration.tools.synthesizer.reader import DatasetContent, DatasetReader
from tests.integration.tools.synthesizer.sampler import (
    DEFAULT_MAX_PLACE_SAMPLES,
    DEFAULT_MAX_STAT_VAR_SAMPLES,
    DatasetSampler,
)

MAX_NODE_EXPECTATIONS = 15
MAX_EDGE_EXPECTATIONS = 10
MAX_SPECIALIZATION_EDGES = 5
MAX_INDICATOR_RESOLUTIONS = 5
MAX_NODE_QUERIES = 5
MAX_SDMX_QUERIES = 3
MAX_MCP_TOOL_CALLS = 3
DEFAULT_SDMX_DATAFLOW = "DC/DF_OBS/1.0.0/*"
DEFAULT_SDMX_FORMAT = "csv"


class DatasetSynthesizer:
    """Auto-synthesizes declarative integration test manifests directly from local directories or GCS buckets."""

    def __init__(self, dataset_dirs: list[str]):
        self.dataset_dirs = dataset_dirs
        self.reader = DatasetReader(dataset_dirs)

    def synthesize(
        self,
        manifest_name: str | None = None,
        existing_manifest: TestManifest | None = None,
        max_stat_vars: int = DEFAULT_MAX_STAT_VAR_SAMPLES,
        max_places: int = DEFAULT_MAX_PLACE_SAMPLES,
    ) -> TestManifest:
        """Parses dataset files and builds a comprehensive declarative TestManifest."""
        expected_nodes: list[ExpectedNode] = []
        expected_edges: list[ExpectedEdge] = []
        specialization_edges: list[SpecializationEdgeSpec] = []
        indicator_resolutions: list[IndicatorResolutionSpec] = []
        node_queries: list[NodeQuerySpec] = []
        mcp_tool_calls: list[MCPToolCallSpec] = []

        all_stat_vars: set[str] = set()
        all_svgs: set[str] = set()
        all_provenances: set[str] = set()
        all_places: set[str] = set()

        for directory_path in self.dataset_dirs:
            if directory_path.startswith("gs://"):
                dataset_content = self.reader.read_gcs_directory(directory_path)
            else:
                dataset_content = self.reader.read_local_directory(Path(directory_path).resolve())

            self._extract_provenances_from_config(dataset_content, all_provenances)
            self._extract_jsonld_content(
                dataset_content,
                expected_nodes,
                all_stat_vars,
                all_svgs,
                all_provenances,
                all_places,
                indicator_resolutions,
                mcp_tool_calls,
            )
            self._extract_mcf_content(
                dataset_content,
                expected_nodes,
                expected_edges,
                specialization_edges,
                node_queries,
                all_stat_vars,
                all_svgs,
                all_provenances,
                indicator_resolutions,
                mcp_tool_calls,
            )
            self._extract_csv_places(dataset_content, all_places)

        sampled_stat_vars = DatasetSampler.sample_stat_vars(
            all_stat_vars, existing_manifest=existing_manifest, max_samples=max_stat_vars
        )
        sample_places = DatasetSampler.sample_places(all_places, max_samples=max_places)

        point_observations, node_queries_from_svs, sdmx_data_queries = (
            self._build_stat_var_queries(sampled_stat_vars, sample_places)
        )
        node_queries.extend(node_queries_from_svs)

        series_observations, sdmx_availability_queries = (
            self._build_series_and_availability_queries(sorted(all_stat_vars)[:5], sample_places)
        )

        name = manifest_name or (
            Path(self.dataset_dirs[0]).name.rstrip("/") if self.dataset_dirs else "custom_dataset"
        )
        relative_dirs = [str(d) for d in self.dataset_dirs]

        return TestManifest(
            name=name,
            description=f"Auto-synthesized test manifest for {len(self.dataset_dirs)} dataset directories",
            ingestion=IngestionManifestConfig(
                dataset_dirs=relative_dirs,
                spanner_expectations=SpannerExpectations(
                    min_observation_count=1,
                    expected_nodes=expected_nodes[:MAX_NODE_EXPECTATIONS],
                    expected_edges=expected_edges[:MAX_EDGE_EXPECTATIONS],
                ),
            ),
            postprocessing=PostprocessingManifestConfig(
                svg_hierarchy=SVGHierarchySpec(
                    expected_specialization_edges=specialization_edges[:MAX_SPECIALIZATION_EDGES],
                ),
                indicator_resolutions=indicator_resolutions[:MAX_INDICATOR_RESOLUTIONS],
            ),
            serving_api=ServingAPIManifestConfig(
                nodes=node_queries[:MAX_NODE_QUERIES],
                point_observations=point_observations,
                series_observations=series_observations,
                sdmx_3_0=SDMXManifestConfig(
                    data_queries=sdmx_data_queries[:MAX_SDMX_QUERIES],
                    availability_queries=sdmx_availability_queries[:MAX_SDMX_QUERIES],
                ),
            ),
            mcp_agent=MCPAgentManifestConfig(
                tool_calls=mcp_tool_calls[:MAX_MCP_TOOL_CALLS],
            ),
        )

    def save_yaml(
        self,
        output_file: str | Path,
        manifest_name: str | None = None,
        max_stat_vars: int = DEFAULT_MAX_STAT_VAR_SAMPLES,
        max_places: int = DEFAULT_MAX_PLACE_SAMPLES,
    ) -> None:
        """Saves synthesized TestManifest dataclass to YAML file with an auto-generated header comment."""
        import datetime
        import logging

        logger = logging.getLogger("synthesizer")
        out_path = Path(output_file).resolve()
        existing_manifest = None
        if out_path.is_file():
            try:
                from tests.integration.core.config_schema import load_test_manifest

                existing_manifest = load_test_manifest(out_path)
                logger.info("Found existing manifest at %s — preserving prior sample anchors.", out_path)
            except Exception as e:
                logger.warning("Could not parse existing manifest at %s: %s", out_path, e)

        manifest = self.synthesize(
            manifest_name=manifest_name,
            existing_manifest=existing_manifest,
            max_stat_vars=max_stat_vars,
            max_places=max_places,
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)

        timestamp_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        sources_str = ", ".join(self.dataset_dirs)
        header_comment = (
            f"# ------------------------------------------------------------------------------\n"
            f"# AUTO-GENERATED BY DATASET SYNTHESIZER — DO NOT HAND-EDIT UNLESS INTENDED\n"
            f"# Source(s): {sources_str}\n"
            f"# Generated At: {timestamp_utc}\n"
            f"# ------------------------------------------------------------------------------\n\n"
        )

        yaml_content = yaml.safe_dump(asdict(manifest), sort_keys=False)

        # Inject clean section comments explaining each test stage and endpoint type
        section_comments = {
            "\ningestion:\n": (
                "\n# ==============================================================================\n"
                "# STAGE 1: INGESTION & SPANNER GRAPH EXPECTATIONS\n"
                "# Validates raw data loading, node counts, and Spanner SQL node/edge existence.\n"
                "# ==============================================================================\n"
                "ingestion:\n"
            ),
            "\npostprocessing:\n": (
                "\n# ==============================================================================\n"
                "# STAGE 2: POSTPROCESSING & VECTOR SEARCH EMBEDDINGS\n"
                "# Validates StatVarGroup hierarchy edges and Vertex AI natural language embeddings.\n"
                "# ==============================================================================\n"
                "postprocessing:\n"
            ),
            "\nserving_api:\n": (
                "\n# ==============================================================================\n"
                "# STAGE 3: SERVING API ENDPOINTS & SDMX 3.0 STANDARDS\n"
                "# Validates REST endpoints (/v2/node, /v2/observation Point & Series) and SDMX 3.0.\n"
                "# ==============================================================================\n"
                "serving_api:\n"
            ),
            "\nmcp_agent:\n": (
                "\n# ==============================================================================\n"
                "# STAGE 4: MCP AI AGENT TOOL INTEGRATION\n"
                "# Validates agentic AI tool calling (e.g. search_indicators).\n"
                "# ==============================================================================\n"
                "mcp_agent:\n"
            ),
            "  nodes:\n": "  # 1. Graph Property Queries (/v2/node)\n  nodes:\n",
            "  point_observations:\n": "  # 2. Point Observations (/v2/observation latest data)\n  point_observations:\n",
            "  series_observations:\n": "  # 3. Series Observations (/v2/observation time-series data)\n  series_observations:\n",
            "  sdmx_3_0:\n": "  # 4. SDMX 3.0 REST API (/sdmx/v3/data & /sdmx/v3/availability)\n  sdmx_3_0:\n",
        }

        for search_key, commented_val in section_comments.items():
            yaml_content = yaml_content.replace(search_key, commented_val)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header_comment + yaml_content)

        logger.info("✔ Auto-synthesized test spec written to: %s", out_path)

    @staticmethod
    def _extract_provenances_from_config(
        content: DatasetContent, target_provenances: set[str]
    ) -> None:
        for item in content.config_data.get("inputFiles", []):
            provenance_id = DatasetSampler.clean_dcid(item.get("provenance", ""))
            if provenance_id:
                target_provenances.add(provenance_id)

    @staticmethod
    def _extract_jsonld_id(val: Any) -> str:
        if not val:
            return ""
        if isinstance(val, dict):
            return DatasetSampler.clean_dcid(str(val.get("@id", "")))
        if isinstance(val, str):
            return DatasetSampler.clean_dcid(val)
        return ""

    @staticmethod
    def _extract_jsonld_content(
        content: DatasetContent,
        expected_nodes: list[ExpectedNode],
        all_stat_vars: set[str],
        all_svgs: set[str],
        all_provenances: set[str],
        all_places: set[str],
        indicator_resolutions: list[IndicatorResolutionSpec],
        mcp_tool_calls: list[MCPToolCallSpec],
    ) -> None:
        for item in content.jsonld_blocks:
            item_type = DatasetSampler.clean_dcid(item.get("@type", ""))
            if item_type == "StatVarObservation":
                stat_var_id = DatasetSynthesizer._extract_jsonld_id(
                    item.get("dcid:variableMeasured") or item.get("variableMeasured")
                )
                place_id = DatasetSynthesizer._extract_jsonld_id(
                    item.get("dcid:observationAbout") or item.get("observationAbout")
                )
                provenance_id = DatasetSynthesizer._extract_jsonld_id(
                    item.get("dcid:provenance") or item.get("provenance")
                )
                if stat_var_id:
                    all_stat_vars.add(stat_var_id)
                    human_name = stat_var_id.split("/")[-1].replace("_", " ")
                    indicator_resolutions.append(
                        IndicatorResolutionSpec(
                            query=human_name.lower(),
                            expected_candidate_dcids=[stat_var_id],
                        )
                    )
                    mcp_tool_calls.append(
                        MCPToolCallSpec(
                            tool_name="search_indicators",
                            arguments={"query": human_name.lower()},
                            expected_match_dcids=[stat_var_id],
                        )
                    )
                if place_id:
                    all_places.add(place_id)
                if provenance_id:
                    all_provenances.add(provenance_id)
            elif item_type in ("StatisticalVariable", "StatVarGroup", "Provenance"):
                node_id = DatasetSynthesizer._extract_jsonld_id(item.get("@id"))
                if node_id:
                    expected_nodes.append(
                        ExpectedNode(subject_id=node_id, expected_types=[item_type])
                    )
                    name_val = item.get("dcid:name") or item.get("name")
                    if item_type in ("StatisticalVariable", "StatVar"):
                        all_stat_vars.add(node_id)
                        human_name = name_val if name_val else node_id.split("/")[-1].replace("_", " ")
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
                    elif item_type == "StatVarGroup":
                        all_svgs.add(node_id)

    @staticmethod
    def _extract_mcf_content(
        content: DatasetContent,
        expected_nodes: list[ExpectedNode],
        expected_edges: list[ExpectedEdge],
        specialization_edges: list[SpecializationEdgeSpec],
        node_queries: list[NodeQuerySpec],
        all_stat_vars: set[str],
        all_svgs: set[str],
        all_provenances: set[str],
        indicator_resolutions: list[IndicatorResolutionSpec],
        mcp_tool_calls: list[MCPToolCallSpec],
    ) -> None:
        for mcf_text in content.mcf_contents:
            for block in mcf_text.split("\n\n"):
                node_match = re.search(r"Node:\s*(?:dcid:|dcs:)?([^\s]+)", block)
                if not node_match:
                    continue

                node_id = DatasetSampler.clean_dcid(node_match.group(1))
                type_match = re.search(r"typeOf:\s*(?:dcid:|dcs:)?([^\s]+)", block)
                name_match = re.search(r'name:\s*"([^"]+)"', block)
                spec_match = re.search(r"specializationOf:\s*(?:dcid:|dcs:)?([^\s]+)", block)
                member_match = re.search(r"memberOf:\s*(?:dcid:|dcs:)?([^\s]+)", block)

                raw_type = type_match.group(1) if type_match else "Node"
                clean_type = DatasetSampler.clean_dcid(raw_type)
                expected_nodes.append(
                    ExpectedNode(subject_id=node_id, expected_types=[raw_type])
                )

                if spec_match:
                    parent_id = DatasetSampler.clean_dcid(spec_match.group(1))
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
                    group_id = DatasetSampler.clean_dcid(member_match.group(1))
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

    @staticmethod
    def _extract_csv_places(content: DatasetContent, target_places: set[str]) -> None:
        entity_col = "observationAbout"
        for item in content.config_data.get("inputFiles", []):
            col_map = item.get("columnMappings", {})
            if "dcid:observationAbout" in col_map:
                entity_col = col_map["dcid:observationAbout"]
                break

        for row in content.csv_rows:
            entity = row.get(entity_col)
            if entity:
                clean_entity = DatasetSampler.clean_dcid(entity)
                target_places.add(clean_entity)

    @staticmethod
    def _build_stat_var_queries(
        sampled_stat_vars: list[str], sample_places: list[str]
    ) -> tuple[list[PointObservationSpec], list[NodeQuerySpec], list[SDMXDataQuerySpec]]:
        point_observations = []
        node_queries = []
        sdmx_data_queries = []

        for stat_var_id in sampled_stat_vars:
            point_observations.append(
                PointObservationSpec(
                    observation_about=sample_places,
                    variables=[stat_var_id],
                    date="LATEST",
                    expected_places_with_data=sample_places[:1],
                )
            )
            node_queries.append(
                NodeQuerySpec(
                    node_dcid=stat_var_id,
                    expression="->name",
                )
            )
            sdmx_data_queries.append(
                SDMXDataQuerySpec(
                    dataflow=DEFAULT_SDMX_DATAFLOW,
                    constraints={
                        "variableMeasured": stat_var_id,
                        "observationAbout": sample_places[0],
                    },
                    format=DEFAULT_SDMX_FORMAT,
                    expected_csv_contains=[stat_var_id],
                )
            )

        return point_observations, node_queries, sdmx_data_queries

    @staticmethod
    def _build_series_and_availability_queries(
        stat_var_ids: list[str], sample_places: list[str]
    ) -> tuple[list[SeriesObservationSpec], list[SDMXAvailabilityQuerySpec]]:
        series_observations = []
        sdmx_availability_queries = []

        for stat_var_id in stat_var_ids:
            series_observations.append(
                SeriesObservationSpec(
                    observation_about=sample_places,
                    variables=[stat_var_id],
                    min_series_length=1,
                )
            )
            sdmx_availability_queries.append(
                SDMXAvailabilityQuerySpec(
                    dataflow=DEFAULT_SDMX_DATAFLOW,
                    constraints={
                        "variableMeasured": stat_var_id,
                        "observationAbout": sample_places[0],
                    },
                )
            )

        return series_observations, sdmx_availability_queries


# Alias for domain clarity
TestSpecSynthesizer = DatasetSynthesizer
