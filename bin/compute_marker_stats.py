"""
compute_marker_stats.py
-----------------------
Compute per-marker mean and std (in 0-1 range) from COMET OME-TIFF images
for use in KRONOS marker_metadata.csv.

Normalization strategy (per KRONOS GitHub issue):
  1. Divide raw pixel values by the dtype maximum (e.g. 65535.0 for uint16)
     to scale to [0, 1]. This value should match "marker_max_values" in your
     KRONOS config.
  2. Pool all normalized pixels across ALL images and samples per marker.
  3. Compute global mean and std on the pooled normalized pixels.

At inference, KRONOS applies: (img / marker_max_value - mean) / std

Usage:
    python compute_marker_stats.py \
        --image_dir /path/to/ome_tiffs \
        --output marker_stats.csv \
        --pattern "*.ome.tiff" \
        --dtype_max 65535.0

    # Merge with existing KRONOS metadata to find new markers:
    python compute_marker_stats.py \
        --image_dir /path/to/ome_tiffs \
        --output marker_stats.csv \
        --existing_metadata marker_metadata.csv
"""

import argparse
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import pandas as pd
import tifffile
from tqdm import tqdm


# ---------------------------------------------------------------------------
# OME-TIFF helpers
# ---------------------------------------------------------------------------

DTYPE_MAX = {
    "uint8":   255.0,
    "uint16":  65535.0,
    "uint32":  4294967295.0,
    "float32": 1.0,
    "float64": 1.0,
}


def parse_marker_names_from_ome(tif: tifffile.TiffFile) -> list:
    """
    Extract ordered channel names from the OME-XML metadata embedded in a
    COMET OME-TIFF. Returns a list aligned with the channel axis of the array.
    Falls back to an empty list if parsing fails.
    """
    try:
        ome_xml = tif.ome_metadata
        root = ET.fromstring(ome_xml)

        # OME namespace varies by version — find it dynamically
        ns = root.tag.split("}")[0].lstrip("{") if "}" in root.tag else ""
        ns_prefix = f"{{{ns}}}" if ns else ""

        image    = root.find(f".//{ns_prefix}Image")
        pixels   = image.find(f"{ns_prefix}Pixels")
        channels = pixels.findall(f"{ns_prefix}Channel")

        names = []
        for ch in channels:
            # COMET typically stores marker name in "Name" or "Fluor"
            name = ch.get("Name") or ch.get("Fluor") or ch.get("ID", "")
            names.append(name.upper().strip())
        return names

    except Exception as e:
        print(f"  [warn] Could not parse OME-XML channel names: {e}")
        return []


def read_ome_tiff(path: Path):
    """
    Read a COMET OME-TIFF.
    Returns:
        image     : array of shape (C, H, W), original dtype preserved
        markers   : list of marker/channel names, length C
        dtype_max : maximum value for this dtype (e.g. 65535.0 for uint16)
    """
    with tifffile.TiffFile(path) as tif:
        marker_names = parse_marker_names_from_ome(tif)
        image = tif.asarray()
        raw_dtype = str(image.dtype)

    # Determine dtype max before any casting
    max_val = DTYPE_MAX.get(raw_dtype)
    if max_val is None:
        print(f"  [warn] Unrecognised dtype '{raw_dtype}', defaulting to 65535.0")
        max_val = 65535.0

    # Normalise dimension ordering to (C, H, W)
    if image.ndim == 2:
        image = image[np.newaxis, ...]
    elif image.ndim == 3:
        pass
    elif image.ndim == 4:
        image = image[0]        # (Z or T, C, H, W) — take first slice
    elif image.ndim == 5:
        image = image[0, 0]     # (T, Z, C, H, W)

    n_channels = image.shape[0]

    if len(marker_names) != n_channels:
        print(f"  [warn] Channel name count ({len(marker_names)}) != "
              f"image channels ({n_channels}). Using generic names.")
        marker_names = [f"CH_{i:03d}" for i in range(n_channels)]

    return image, marker_names, max_val


# ---------------------------------------------------------------------------
# Stats accumulation
# ---------------------------------------------------------------------------

def accumulate_pixels(image_paths: list, dtype_max_override=None):
    """
    Iterate over all OME-TIFFs, normalize each channel by dtype max,
    and accumulate all pixel values per marker name.

    Returns:
        pixel_store : { marker_name -> list of 1-D float32 pixel arrays }
        dtype_max   : the dtype max value used (for reporting / KRONOS config)
    """
    pixel_store = {}
    detected_max = None

    for path in tqdm(image_paths, desc="Reading images"):
        try:
            image, marker_names, file_max = read_ome_tiff(path)
        except Exception as e:
            print(f"  [error] Skipping {path.name}: {e}")
            continue

        max_val = dtype_max_override if dtype_max_override is not None else file_max

        if detected_max is None:
            detected_max = max_val
        elif detected_max != max_val and dtype_max_override is None:
            print(f"  [warn] {path.name} dtype max {max_val} differs from "
                  f"first image ({detected_max}). Use --dtype_max to fix.")

        # Key normalization step: divide by dtype max → [0, 1]
        image_norm = image.astype(np.float32) / max_val

        for ch_idx, marker in enumerate(marker_names):
            flat = image_norm[ch_idx].flatten()
            if marker not in pixel_store:
                pixel_store[marker] = []
            pixel_store[marker].append(flat)

        del image, image_norm

    final_max = dtype_max_override if dtype_max_override is not None else (detected_max or 65535.0)
    return pixel_store, final_max


def compute_stats(pixel_store: dict) -> pd.DataFrame:
    """
    Concatenate pooled pixels per marker and compute mean + std.
    """
    rows = []
    for marker in sorted(pixel_store.keys()):
        pixels = np.concatenate(pixel_store[marker])
        rows.append({
            "marker_name": marker,
            "marker_mean": round(float(np.mean(pixels)), 6),
            "marker_std":  round(float(np.std(pixels)),  6),
            "n_pixels":    int(len(pixels)),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Compute KRONOS marker_mean / marker_std from COMET OME-TIFFs"
    )
    parser.add_argument(
        "--image_dir", type=str, required=True,
        help="Directory containing OME-TIFF files (searched recursively)"
    )
    parser.add_argument(
        "--output", type=str, default="marker_stats.csv",
        help="Output CSV path (default: marker_stats.csv)"
    )
    parser.add_argument(
        "--pattern", type=str, default="*.ome.tiff",
        help="Glob pattern for images (default: *.ome.tiff). "
             "Also try '*.ome.tif' depending on your naming."
    )
    parser.add_argument(
        "--dtype_max", type=float, default=None,
        help="Override dtype max for normalization. Auto-detected from image "
             "dtype if not set (uint16=65535, uint8=255). This value must match "
             "'marker_max_values' in your KRONOS inference config."
    )
    parser.add_argument(
        "--existing_metadata", type=str, default=None,
        help="Optional: path to existing marker_metadata.csv to compare against. "
             "Reports which of your markers are already covered vs. new."
    )
    args = parser.parse_args()

    image_dir = Path(args.image_dir)
    image_paths = sorted(image_dir.rglob(args.pattern))

    if not image_paths:
        print(f"No files found matching '{args.pattern}' under {image_dir}")
        return

    print(f"Found {len(image_paths)} OME-TIFF file(s):")
    for p in image_paths:
        print(f"  {p}")

    # Accumulate pixels and compute stats
    pixel_store, dtype_max = accumulate_pixels(image_paths, args.dtype_max)
    stats_df = compute_stats(pixel_store)

    print(f"\nNormalization: raw pixel / {dtype_max}")
    print(f"=> Set 'marker_max_values': {dtype_max} in your KRONOS inference config.\n")
    print("Computed stats:")
    print(stats_df[["marker_name", "marker_mean", "marker_std",
                     "n_pixels"]].to_string(index=False))

    # Compare against existing KRONOS metadata if provided
    if args.existing_metadata:
        existing = pd.read_csv(args.existing_metadata)
        existing_names = set(existing["marker_name"].str.upper())
        your_names     = set(stats_df["marker_name"])

        matched  = your_names & existing_names
        new_only = your_names - existing_names

        print(f"\n--- Comparison with {args.existing_metadata} ---")
        print(f"  Matched to KRONOS pretraining set : {len(matched)}")
        print(f"  New markers (not in pretraining)  : {len(new_only)}")

        if new_only:
            print("\n  New markers — use computed stats and assign an unused marker ID:")
            new_df = stats_df[stats_df["marker_name"].isin(new_only)][
                ["marker_name", "marker_mean", "marker_std"]
            ]
            print(new_df.to_string(index=False))

    # Save (drop diagnostic n_pixels column for clean KRONOS-ready output)
    out_path = Path(args.output)
    stats_df[["marker_name", "marker_mean", "marker_std"]].to_csv(out_path, index=False)
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()