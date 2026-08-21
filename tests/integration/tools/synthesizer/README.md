# 🛠️ Test Spec Synthesizer

`test-spec-synthesizer` is a modular, high-performance tool for auto-generating declarative integration test manifest specs (`.yaml`) for Google Data Commons platform deployments.

It parses raw dataset directories (`.mcf`, `.csv`, `config.json`) or processed Knowledge Graph artifacts (`node-*.jsonld`, `observation-*.jsonld`) directly from local filesystems or Google Cloud Storage (`gs://`) buckets.

---

## 🏗️ Package Architecture

```
tests/integration/tools/synthesizer/
├── __init__.py       # Package entrypoints & exports
├── reader.py         # DatasetReader: Handles GCS byte-range HTTP streaming & local file reading
├── sampler.py        # DatasetSampler: Stratified Topic Sampling & Incremental Manifest Anchoring
├── builder.py        # DatasetSynthesizer: Constructs declarative TestManifest dataclass objects
├── cli.py            # CLI argument parsing with structured logging
├── README.md         # Documentation
└── tests/            # Unit tests
    └── test_sampler.py
```

---

## 🌟 Key Features

1. **⚡ GCS Byte-Range HTTP Streaming**:
   Reads dataset headers and top observation rows in milliseconds directly from `gs://` buckets without downloading multi-gigabyte files into RAM.

2. **📊 Stratified Topic Sampling**:
   Groups StatisticalVariables by `(Provenance, Source Filename Stem)` to guarantee high-coverage test sampling across every indicator category (Health, Education, Poverty, Labor, Agriculture) instead of taking sequential rows from a single file.

3. **🛡️ Incremental Manifest Anchoring**:
   When re-running synthesis on an existing manifest (e.g. `DESA_GENDER.yaml`), Synthesizer preserves previously chosen StatVar anchors. Appending 10,000 new rows or earlier alphabetical variables causes **0 line diffs in `git status`**.

4. **📝 Auto-Generated Header Comments**:
   Prepends a version-controllable comment block to generated `.yaml` files including source directories and UTC timestamp.

5. **🔍 Robust Namespace & Prefix Handling**:
   Strips `dcid:` and `dcs:` prefixes safely (`removeprefix()`), preventing string manipulation bugs or accidental token mutation.

---

## 🚀 Quickstart & Usage

### 1. Synthesize a Manifest from a Raw GCS Input Directory

```bash
uv run python3 -m tests.integration.tools.synthesizer.cli \
  "gs://gmechali-staging-dc-artifacts-datcom-website-dev/ingestion/input/DESA_GENDER" \
  -o "tests/integration/manifests/DESA_GENDER.yaml"
```

### 2. Synthesize a Manifest directly from Processed JSON-LD Artifacts

```bash
uv run python3 -m tests.integration.tools.synthesizer.cli \
  "gs://gmechali-staging-dc-artifacts-datcom-website-dev/ingestion/internal/datacommons/jsonld/DESA_GENDER_20260821_144027_412729/DESA_GENDER" \
  -o "tests/integration/manifests/DESA_GENDER.yaml" \
  --name "DESA_GENDER"
```

### 3. CLI Flags & Options

```bash
uv run python3 -m tests.integration.tools.synthesizer.cli --help

Options:
  -o FILE, --output FILE
                        Destination path for the output YAML manifest (required)
  -n NAME, --name NAME  Explicit manifest name (defaults to directory basename)
  --max-stat-vars INT   Maximum StatisticalVariables to sample across topics (default: 8)
  --max-places INT      Maximum geographic entity places to sample (default: 3)
  -v, --verbose         Enable detailed debug logging output
  -q, --quiet           Suppress informational logs (show errors only)
```

---

## 🧪 Running Pytest Integration Tests with Synthesized Specs

Once a manifest is generated in `tests/integration/manifests/<name>.yaml`, execute the integration audit suite:

```bash
# Run using manifest short-name (DESA_GENDER):
uv run pytest tests/integration/ \
  -v -s \
  --workspace=/Users/gmechali/Desktop/datacommons/playground/gmechali-staging \
  --project=datcom-website-dev \
  --test-config=DESA_GENDER \
  --reuse-data
```

---

## 🔁 Batch Synthesize Specs Across All Agency Directories

```bash
for agency in DESA_GENDER ECLAC ILO IOM_DTM SDG UNAIDS UNDP_HDRO UNESCO UNICEF UNIDO WHO; do
  echo "🛠️ Synthesizing $agency..."
  uv run python3 -m tests.integration.tools.synthesizer.cli \
    "gs://gmechali-staging-dc-artifacts-datcom-website-dev/ingestion/input/$agency" \
    -o "tests/integration/manifests/$agency.yaml"
done
```

---

## 🔬 Running Unit Tests

Run unit tests for the synthesizer package:

```bash
uv run python3 -c "
import tests.integration.tools.synthesizer.tests.test_sampler as t
t.test_clean_dcid_prefix_removal()
t.test_extract_provenance_file_key_robustness()
t.test_sample_stat_vars_stratified()
t.test_sample_stat_vars_incremental_anchoring()
t.test_sample_places_fallback()
print('🎉 ALL UNIT TESTS PASSED!')
"
```
