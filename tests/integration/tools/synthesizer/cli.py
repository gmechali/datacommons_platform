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

"""CLI Entry point for the Dataset Synthesizer tool."""

import argparse
import logging
import sys
from pathlib import Path

from tests.integration.tools.synthesizer.builder import DatasetSynthesizer

logger = logging.getLogger("synthesizer")


def build_arg_parser() -> argparse.ArgumentParser:
    """Builds a clean, robust command-line argument parser."""
    parser = argparse.ArgumentParser(
        prog="test-spec-synthesizer",
        description="Synthesize declarative integration test manifest specs (.yaml) directly from dataset directories or GCS buckets (gs://).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "dataset_dirs",
        metavar="DATASET_DIR",
        nargs="+",
        help="Path(s) to dataset directory (e.g. /path/to/dataset or gs://bucket/path/dataset)",
    )

    parser.add_argument(
        "-o",
        "--output",
        required=True,
        metavar="FILE",
        help="Destination path to write output YAML manifest (e.g. tests/integration/manifests/my_dataset.yaml)",
    )

    parser.add_argument(
        "-n",
        "--name",
        default=None,
        metavar="NAME",
        help="Explicit manifest name (defaults to basename of first dataset directory)",
    )

    parser.add_argument(
        "--max-stat-vars",
        type=int,
        default=8,
        metavar="INT",
        help="Maximum StatisticalVariables to sample across topic categories",
    )

    parser.add_argument(
        "--max-places",
        type=int,
        default=3,
        metavar="INT",
        help="Maximum geographic entity places to sample",
    )

    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable detailed debug logging output",
    )
    verbosity_group.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress informational output, showing warnings and errors only",
    )

    return parser


def main(args_list: list[str] | None = None) -> int:
    """Command-line entry point with robust argument parsing, logging, and error handling."""
    parser = build_arg_parser()
    args = parser.parse_args(args_list)

    if args.verbose:
        log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.WARNING
    else:
        log_level = logging.INFO

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.max_stat_vars <= 0:
        parser.error("--max-stat-vars must be greater than 0")

    if args.max_places <= 0:
        parser.error("--max-places must be greater than 0")

    out_path = Path(args.output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Initializing DatasetSynthesizer for %d directories...", len(args.dataset_dirs))
        synthesizer = DatasetSynthesizer(args.dataset_dirs)
        synthesizer.save_yaml(
            output_file=out_path,
            manifest_name=args.name,
            max_stat_vars=args.max_stat_vars,
            max_places=args.max_places,
        )
        return 0
    except Exception as e:
        logger.error("Failed during dataset synthesis: %e", e, exc_info=args.verbose)
        return 1


if __name__ == "__main__":
    sys.exit(main())
