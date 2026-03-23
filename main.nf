nextflow.enable.dsl = 2

params.image_dir = null
params.output = 'marker_stats.csv'
params.pattern = '*.ome.tiff'
params.dtype_max = null
params.existing_metadata = null
params.script = "${projectDir}/bin/compute_marker_stats.py"
params.outdir = 'results'
params.publish_dir_mode = 'copy'
params.validate_params = true

process COMPUTE_MARKER_STATS {
    tag "marker_stats"
    label 'process_heavy'

    conda "${projectDir}/bin/environment.yml"
    container 'community.wave.seqera.io/library/python_tifffile_scikit-image_scikit-learn_pruned:593e00ba324c12b3'

    publishDir "${params.outdir}", mode: params.publish_dir_mode

    input:
    tuple val(image_dir), val(output_name), val(pattern), val(dtype_max), val(existing_metadata), path(script_file)

    output:
    path output_name
    path 'compute_marker_stats.log'

    script:
    def existingArg = existing_metadata ? "--existing_metadata '${existing_metadata}'" : ''
    def dtypeArg = dtype_max != null ? "--dtype_max ${dtype_max}" : ''
    """
    set -euo pipefail

        python "${script_file}" \
      --image_dir "${image_dir}" \
      --output "${output_name}" \
      --pattern "${pattern}" \
            ${dtypeArg} \
      ${existingArg} \
      2>&1 | tee compute_marker_stats.log
    """
}

workflow {
    if (!params.image_dir) {
        error "Missing required parameter: --image_dir"
    }

    def imageDir = file(params.image_dir)
    if (!imageDir.exists() || !imageDir.isDirectory()) {
        error "image_dir does not exist or is not a directory: ${params.image_dir}"
    }

    def scriptParam = params.script.toString()
    def scriptCandidates = [
        file(scriptParam),
        file("${projectDir}/${scriptParam}")
    ]
    def scriptFile = scriptCandidates.find { candidate -> candidate.exists() }
    if (!scriptFile) {
        error "Python script does not exist: ${params.script} (tried: ${scriptCandidates*.toString().join(', ')})"
    }

    def outputParam = params.get('output', 'marker_stats.csv').toString()
    if (!outputParam?.trim()) {
        error "output cannot be empty"
    }

    def patternParam = params.get('pattern', '*.ome.tiff').toString()
    if (!patternParam?.trim()) {
        error "pattern cannot be empty"
    }

    def dtypeMaxParam = params.dtype_max != null ? (params.dtype_max as double) : null
    if (dtypeMaxParam != null && dtypeMaxParam <= 0) {
        error "dtype_max must be > 0 when provided"
    }

    def existingMetadataParam = params.existing_metadata ? file(params.existing_metadata.toString()).toString() : null
    if (existingMetadataParam) {
        def metadataFile = file(existingMetadataParam)
        if (!metadataFile.exists()) {
            error "existing_metadata file does not exist: ${params.existing_metadata}"
        }
    }

    channel
        .of(tuple(
            imageDir.toString(),
            outputParam,
            patternParam,
            dtypeMaxParam,
            existingMetadataParam,
            scriptFile
        ))
        | COMPUTE_MARKER_STATS
}
