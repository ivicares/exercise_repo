#!/usr/bin/env python3

#  ivana code
# do the raw alignment, so no primer clipping, recalibration and other funny business
import os
import subprocess
from sys import argv
from utils import run_subprocess, piped_subprocess

# TODO find and download an exome grom GIAB
# TODO read https://peps.python.org/pep-0008/
# TODO decorate all methods with type hints
# TODO write unit tests using pytest
# TODO turn into flows using Prefect
# TODO dockerize

def size_on_disk(fastq_path, fq_pair):
    return sum([os.path.getsize(f"{fastq_path}/{f}") for f in fq_pair])


def is_nonzero(fnm):
    return os.path.exists(fnm) and os.path.getsize(fnm) > 0


def exists(fpath, fn):
    fullpath = f"{fpath}/{fn}"
    return is_nonzero(fullpath)


def get_fastqs(fastq_path: str) -> list[str]:
    extension = ".fastq.gz"
    fastqs = []
    # find all files with this extension
    return fastqs


def find_pairs(fastqs):
    fq_pairs = []
    # find all _R1_, _R2_ pairs
    return fq_pairs


def create_sam(bwa, ref_genome, fastq_path, fq_pair, output_dir):

    output = f"{output_dir}/{fq_pair[0].replace('_R1', '').replace('fastq.gz','sam')}"
    if is_nonzero(output):
        print(f"{output} found", end="\n")
    else:
        # TODO bwa command to create sam file
        cmd = ""
        print(f"running {cmd}")  # TODO replace comments with oython logging
        # run_subprocess(cmd, stdoutfnm=output)
        print(f"done")
    return output


def extract_base_name(fq_pair):

    if fq_pair[0].replace('_R1', '') != fq_pair[1].replace('_R2', ''):
        print(f"name mismatch {fq_pair}")
        exit(1)

    return fq_pair[0].replace('_R1', '').replace('.fastq.gz', '')


def extract_fastqs(samtools, sam, contig,  outdir,  extracted_contig_fastq):

    cmds = [f"awk '$1~/^@/ || $3==\"{contig}\"' {sam}",
            f"{samtools} fastq -1 {outdir}/{extracted_contig_fastq[0]} "
            f"-2 {outdir}/{extracted_contig_fastq[1]} -0 /dev/null -s /dev/null -n"]
    print(f"extracting fastq from  {sam}")
    piped_subprocess(cmds, stdoutfnm=subprocess.DEVNULL, errorfnm=subprocess.DEVNULL)
    print(f"done")


def concat_and_compress_fastqs(fastq_dir, outname_base):

    for read in ["R1", "R2"]:
        extension = ".fastq"
        fq_files = [f"{fastq_dir}/{f}" for f in os.listdir(fastq_dir) if f[-len(extension):]]
        # TODO use fastcat here https://github.com/epi2me-labs/fastcat
        cmd = f"cat {' '.join(fq_files)}"
        outname = f"{outname_base}.{read}.fastq"
        run_subprocess(cmd, stdoutfnm=outname)

        print(f"compressing {read}")
        cmd = f"gzip {outname}"
        run_subprocess(cmd)
        print(f"{read} compressed")


def main():

    if len(argv)<4:
        print(f"Usage: {argv[0]} <path to fastq dir> <path to ref genome> <output dir>")
        exit()
    [fastq_path, ref_genome, output_dir] = argv[1:4]
    bwa = "/devel/software/sentieon-genomics-202112.01/bin/bwa"
    samtools = "/devel/software/samtools-1.11/samtools"

    # TODO check with pydantic
    # die_if_not_dir(fastq_path, check_absolute=False)
    # die_if_not_dir(output_dir, check_absolute=True)
    # die_if_not_nonzero_file(ref_genome)
    # for dep in [bwa, samtools]:
    #     die_if_not_runnable(dep)

    # TODO turn this into Prefect 2 flow https://docs.prefect.io/tutorials/first-steps/
    contig_dir = f"{output_dir}/contig"
    os.makedirs(contig_dir, exist_ok=True)

    fastqs = get_fastqs(fastq_path)

    fq_pairs = find_pairs(fastqs)
    fq_pairs.sort(key=lambda fp: size_on_disk(fastq_path, fp))
    for fq_pair in fq_pairs:
        base_name = extract_base_name(fq_pair)
        print()
        print(fq_pair, size_on_disk(fastq_path, fq_pair), base_name)
        extracted_contig_fastq = [f"{base_name}.{read}.fastq" for read in ["R1", "R2"]]
        if all([exists(contig_dir, f) for f in extracted_contig_fastq]):
            print(f"{extracted_contig_fastq} found in {contig_dir}")
        else:
            sam = create_sam(bwa, ref_genome, fastq_path, fq_pair, output_dir)
            extract_fastqs(samtools, sam, contig_dir, extracted_contig_fastq)
            if all([exists(contig_dir, f) for f in extracted_contig_fastq]):
                # remove sam
                os.remove(sam)
            else:
                print(f"error extracting fastq from {sam}")
                exit()
    print()
    concat_and_compress_fastqs(contig_dir, "contig_all")

    print(f"output written to {contig_dir}")


##############################################
if __name__ == "__main__":
    main()
