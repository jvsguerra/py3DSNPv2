import os
import pathlib
import pandas
from typing import Union, List
from py3DSNPv2 import Client, Job, process_3dgenes, process_3dsnps


def run(snps: List[str], type: str, filename: Union[str, pathlib.Path]) -> None:
    # Create client
    client = Client()

    # Iteratively create jobs for each SNP
    results = pandas.DataFrame()
    for snp in snps:
        # Create job
        job = Job(snp, type=type, format="json")

        # Run job with 3DSNP v2.0 api
        job = client.run(job)

        # Process job depending on type of data
        if type == "3dgene":
            tmp = process_3dgenes(job)
        elif type == "3dsnp":
            tmp = process_3dsnps(job)

        # Append non-None data to results
        if tmp is not None:
            results = pandas.concat([results, tmp])

    # List to pandas.DataFrame
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    results.to_csv(filename)


if __name__ == "__main__":
    # Read RS of SNPs
    snps = pandas.read_csv("data/snps_information.csv")["RS"].values

    # Run 3Dgenes in 3DSNP v2.0
    run(snps, "3dgene", "results/3dgenes.csv")

    # Run 3DSNPs in 3DSNP v2.0
    run(snps, "3dsnp", "results/3dsnps.csv")
