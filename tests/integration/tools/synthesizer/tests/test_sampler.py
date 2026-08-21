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

"""Unit tests for DatasetSampler logic (topic extraction, prefix cleaning, stratified sampling, and manifest anchoring)."""

from tests.integration.core.config_schema import (
    PointObservationSpec,
    ServingAPIManifestConfig,
    TestManifest,
)
from tests.integration.tools.synthesizer.sampler import DatasetSampler


def test_clean_dcid_prefix_removal():
    assert DatasetSampler.clean_dcid("dcid:country/USA") == "country/USA"
    assert DatasetSampler.clean_dcid("dcs:StatisticalVariable") == "StatisticalVariable"
    assert DatasetSampler.clean_dcid("country/USA") == "country/USA"
    assert DatasetSampler.clean_dcid("dcid:dcid_test") == "dcid_test"  # Safe prefix stripping!
    assert DatasetSampler.clean_dcid("") == ""


def test_extract_provenance_file_key_robustness():
    assert (
        DatasetSampler.extract_provenance_file_key("undata/desagender/ABR_ADO_RATE.AGE--Y10T14__SEX--F")
        == "desagender/ABR_ADO_RATE"
    )
    assert (
        DatasetSampler.extract_provenance_file_key("dcid:undata/desagender/ACC_ARV.SEX--F")
        == "desagender/ACC_ARV"
    )
    assert DatasetSampler.extract_provenance_file_key("dcid:Count_Person") == "Count_Person"
    assert DatasetSampler.extract_provenance_file_key("") == "default_topic"


def test_sample_stat_vars_stratified():
    all_stat_vars = {
        "undata/desagender/ABR_ADO_RATE.AGE--Y10T14__SEX--F",
        "undata/desagender/ABR_ADO_RATE.AGE--Y15T19__SEX--F",
        "undata/desagender/ACC_ARV.SEX--F",
        "undata/desagender/AGRI_OWN_RT",
    }
    sampled = DatasetSampler.sample_stat_vars(all_stat_vars, max_samples=5)

    assert len(sampled) == 3
    assert any("ABR_ADO_RATE" in sv for sv in sampled)
    assert any("ACC_ARV" in sv for sv in sampled)
    assert any("AGRI_OWN_RT" in sv for sv in sampled)


def test_sample_stat_vars_incremental_anchoring():
    all_stat_vars = {
        "undata/desagender/ABR_ADO_RATE.AGE--Y01T04__SEX--F",  # Comes earlier alphabetically!
        "undata/desagender/ABR_ADO_RATE.AGE--Y10T14__SEX--F",  # Existing anchor
        "undata/desagender/ACC_ARV.SEX--F",
    }

    existing_manifest = TestManifest(
        name="test",
        description="test",
        serving_api=ServingAPIManifestConfig(
            point_observations=[
                PointObservationSpec(
                    observation_about=["country/USA"],
                    variables=["undata/desagender/ABR_ADO_RATE.AGE--Y10T14__SEX--F"],
                )
            ]
        ),
    )

    sampled = DatasetSampler.sample_stat_vars(
        all_stat_vars, existing_manifest=existing_manifest
    )

    assert "undata/desagender/ABR_ADO_RATE.AGE--Y10T14__SEX--F" in sampled
    assert "undata/desagender/ABR_ADO_RATE.AGE--Y01T04__SEX--F" not in sampled


def test_sample_places_fallback():
    places = set()
    sampled = DatasetSampler.sample_places(places)
    assert sampled == ["country/USA"]
