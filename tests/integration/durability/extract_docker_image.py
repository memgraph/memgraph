#!/usr/bin/env python3
import argparse
import json
import logging
import os
import secrets  # For secure random string generation
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

# Setup logger
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)  # Change to DEBUG for more detail


def safe_extract_tar(tar_file: Path, output_dir: Path):
    """Extract all members of a tar archive with current user ownership using external tar."""
    extract_cmd = ["tar", "--no-same-owner", "-xf", str(tar_file), "-C", str(output_dir)]
    result = subprocess.run(extract_cmd, check=True)

    if result.returncode == 0:
        logger.info(f"Successfully extracted {tar_file} to {output_dir}")


def extract_docker_image(image: str, output_dir: Path, force: bool):
    # If output directory already exists, handle according to the force flag
    if output_dir.exists():
        if force:
            logger.warning(f"Destination directory {output_dir} already exists. Deleting it as --force is set.")
            shutil.rmtree(output_dir)  # Delete the existing directory
        else:
            logger.warning(f"Destination directory {output_dir} already exists. Use --force to overwrite.")
            return  # Exit early if not forcing

    output_dir.mkdir(parents=True, exist_ok=False)

    # Check if the image exists locally
    try:
        subprocess.run(
            ["docker", "inspect", "--type=image", image],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(f"Docker image {image} found locally.")
    except subprocess.CalledProcessError:
        logger.warning(f"Docker image {image} not found locally. Attempting to pull...")
        pull_result = subprocess.run(["docker", "pull", image])
        if pull_result.returncode != 0:
            raise RuntimeError(f"Failed to pull Docker image: {image}")

    with tempfile.TemporaryDirectory(prefix="docker_image_tmp_", dir="/tmp") as tmp_path:
        tmp_dir = Path(tmp_path)
        logger.info(f"Using temporary extraction dir: {tmp_dir}")

        # Save Docker image and extract to tmp_dir
        logger.info(f"Saving Docker image: {image}")
        save_cmd = ["docker", "save", image]
        with subprocess.Popen(save_cmd, stdout=subprocess.PIPE) as proc:
            with tarfile.open(fileobj=proc.stdout, mode="r|") as tar:
                tar.extractall(path=tmp_dir)

        # Read manifest
        manifest_path = tmp_dir / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError("manifest.json not found in the extracted image.")

        with open(manifest_path) as f:
            manifest = json.load(f)

        layers = manifest[0]["Layers"]

        # Extract each layer into output_dir
        logger.info(f"Extracting layers to: {output_dir}")
        for layer in layers:
            layer_path = tmp_dir / layer
            logger.debug(f"Extracting layer: {layer_path}")
            safe_extract_tar(layer_path, output_dir)

    logger.info(f"Image extracted successfully to: {output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Extract a Docker image into a local folder.")
    parser.add_argument("image", help="Docker image name (e.g. memgraph/memgraph:2.19.0)")
    parser.add_argument("--output", help="Destination directory (default: /tmp/<image_tag>)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "-f", "--force", action="store_true", help="Force extraction by deleting existing output directory"
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Derive a clean folder name from image tag
    random_string = "".join(secrets.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(6))
    image_name = args.image.split(":")[0].replace("/", "-")  # Replace slashes with dashes for folder name
    tag = args.image.split(":")[-1] if ":" in args.image else "latest"
    default_output = Path(f"/tmp/{random_string}/{image_name}/{tag}")
    output_dir = Path(args.output) if args.output else default_output

    extract_docker_image(args.image, output_dir, args.force)


if __name__ == "__main__":
    main()
