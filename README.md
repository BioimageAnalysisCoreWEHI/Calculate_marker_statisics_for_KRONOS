# Calculate_marker_stats_for_KRONOS

Nextflow pipeline for computing per-marker mean and standard deviation from COMET OME-TIFF images for KRONOS marker metadata.

## What it does

- Recursively finds OME-TIFF files in `--image_dir` using `--pattern`.
- Applies per-image normalization by dividing by dtype max (auto-detected or `--dtype_max`).
- Pools normalized pixels by marker across all files.
- Computes `marker_mean` and `marker_std` and writes a CSV.

## Container

The pipeline runs with:

`community.wave.seqera.io/library/python_tifffile_scikit-image_scikit-learn_pruned:593e00ba324c12b3`

## Required parameter

- `--image_dir` Directory containing OME-TIFF files.

## Optional parameters

- `--output` Output CSV filename (default: `marker_stats.csv`).
- `--pattern` Recursive glob pattern (default: `*.ome.tiff`).
- `--dtype_max` Optional dtype max override for normalization (e.g. `65535`).
- `--existing_metadata` Optional existing `marker_metadata.csv` for marker comparison.
- `--script` Path to Python compute script (default: `bin/compute_marker_stats.py`).
- `--outdir` Published output directory (default: `results`).
- `--publish_dir_mode` Nextflow publish mode (default: `copy`).

## Usage

### Local (Docker)

```bash
nextflow run main.nf \
	-profile docker \
	--image_dir /path/to/ome_tiffs \
	--pattern '*.ome.tiff' \
	--dtype_max 65535 \
	--output marker_stats.csv \
	--outdir results
```

### Local (Conda)

```bash
nextflow run main.nf \
	-profile conda \
	--image_dir /path/to/ome_tiffs \
	--pattern '*.ome.tiff' \
	--dtype_max 65535 \
	--output marker_stats.csv \
	--outdir results
```

### HPC (Slurm + Apptainer)

```bash
nextflow run main.nf \
	-profile apptainer,large \
	--image_dir /path/to/ome_tiffs \
	--pattern '*.ome.tiff' \
	--dtype_max 65535 \
	--output marker_stats.csv \
	--outdir /path/to/output
```

### With existing metadata comparison

```bash
nextflow run main.nf \
	-profile apptainer,large \
	--image_dir /path/to/ome_tiffs \
	--existing_metadata /path/to/marker_metadata.csv \
	--output marker_stats.csv \
	--outdir /path/to/output
```

## Outputs

- `marker_stats.csv` (or filename passed via `--output`)
- `compute_marker_stats.log`
