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

"""Stratified Topic & Incremental Manifest Anchoring algorithms."""

from tests.integration.core.config_schema import TestManifest

DEFAULT_MAX_STAT_VAR_SAMPLES = 8
DEFAULT_MAX_PLACE_SAMPLES = 3
DEFAULT_FALLBACK_PLACE = "country/USA"


class DatasetSampler:
    """Handles stratified sampling of StatisticalVariables and Places with incremental manifest anchoring."""

    @staticmethod
    def clean_dcid(raw_dcid: str) -> str:
        """Robustly strips dcid: and dcs: prefixes without mutating inner string content."""
        if not raw_dcid:
            return ""
        cleaned = raw_dcid.strip()
        if cleaned.startswith("dcid:"):
            cleaned = cleaned[5:]
        elif cleaned.startswith("dcs:"):
            cleaned = cleaned[4:]
        return cleaned

    @classmethod
    def extract_provenance_file_key(cls, stat_var_dcid: str) -> str:
        """Robustly extracts the (Provenance, Topic Stem) key from a StatVar DCID.

        Example: 'undata/desagender/ABR_ADO_RATE.AGE--Y10T14__SEX--F' -> 'desagender/ABR_ADO_RATE'
        Example: 'dcid:Count_Person' -> 'Count_Person'
        """
        cleaned_dcid = cls.clean_dcid(stat_var_dcid)
        if not cleaned_dcid:
            return "default_topic"

        # Separate main topic stem from slice dimensions (split at first dot)
        topic_stem = cleaned_dcid.split(".")[0]
        segments = [s for s in topic_stem.split("/") if s]

        if len(segments) >= 2:
            return f"{segments[-2]}/{segments[-1]}"
        if segments:
            return segments[0]
        return "default_topic"

    @classmethod
    def sample_stat_vars(
        cls,
        all_stat_vars: set[str],
        existing_manifest: TestManifest | None = None,
        max_samples: int = DEFAULT_MAX_STAT_VAR_SAMPLES,
    ) -> list[str]:
        """Samples StatisticalVariables anchored by (Provenance, Source Filename Stem).

        Single deterministic algorithm:
        1. Group all available StatVars by (provenance, filename_stem) topic key.
        2. For each topic group, preserve existing anchor if available in prior manifest;
           otherwise pick the canonical first (alphabetical) StatVar.
        """
        if not all_stat_vars:
            return []

        existing_anchored_dcids: set[str] = set()
        if existing_manifest and existing_manifest.serving_api:
            for observation_spec in existing_manifest.serving_api.point_observations:
                for var in observation_spec.variables:
                    existing_anchored_dcids.add(cls.clean_dcid(var))

        topic_to_stat_vars_map: dict[str, list[str]] = {}
        for stat_var_dcid in sorted(all_stat_vars):
            clean_var = cls.clean_dcid(stat_var_dcid)
            if not clean_var:
                continue
            topic_key = cls.extract_provenance_file_key(clean_var)
            topic_to_stat_vars_map.setdefault(topic_key, []).append(clean_var)

        sampled_stat_vars: list[str] = []
        for topic_key, candidate_stat_vars in topic_to_stat_vars_map.items():
            anchored_dcid = next(
                (dcid for dcid in candidate_stat_vars if dcid in existing_anchored_dcids),
                candidate_stat_vars[0],
            )
            sampled_stat_vars.append(anchored_dcid)
            if len(sampled_stat_vars) >= max_samples:
                break

        return sampled_stat_vars

    @classmethod
    def sample_places(
        cls,
        all_places: set[str],
        max_samples: int = DEFAULT_MAX_PLACE_SAMPLES,
    ) -> list[str]:
        """Samples representative geographic entity DCIDs robustly."""
        cleaned_places = [cls.clean_dcid(p) for p in sorted(all_places) if cls.clean_dcid(p)]
        if not cleaned_places:
            return [DEFAULT_FALLBACK_PLACE]
        return cleaned_places[:max_samples]
