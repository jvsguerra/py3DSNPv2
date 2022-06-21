from typing import Optional, List, Any
import requests

__all__ = ["Job", "Client", "process_3dgenes", "process_3dsnps"]


class Job:
    def __init__(self, rs: str, type: str, format: str = "json"):
        self.rs: str = rs
        self.type: str = type
        self.format: str = format
        self.request: requests.models.Response = None
        self.output: Optional[List[List[Any]]] = None


class Client:
    def __init__(self, server="https://omic.tech/3dsnpv2/api.do"):
        self.server = server

    def run(self, job: Job) -> List[Any]:
        job.request = requests.post(
            f"{self.server}?id={job.rs}&type={job.type}&format={job.format}"
        )
        if job.request.ok:
            job.output = job.request.json()
        return job


def process_3dgenes(job: Job) -> Optional[List[Any]]:
    if type(job.output) is list:
        results = []
        for i in range(len(job.output[0]["data_loop_gene"])):
            results.append(
                [job.rs]
                + [
                    job.output[0]["data_loop_gene"][i][key]
                    for key in ["gene", "start", "end", "distance", "cell", "type_loop"]
                ]
            )
        return results
    else:
        return None


def process_3dsnps(job: Job) -> Optional[List[Any]]:
    if type(job.output) is list:
        results = []
        for i in range(len(job.output[0]["data_loop_snp"])):
            results.append(
                [job.rs]
                + [
                    job.output[0]["data_loop_gene"][i][key]
                    for key in [
                        "SNP_B",
                        "r2",
                        "dp",
                        "start",
                        "end",
                        "distance",
                        "cell",
                        "type_loop",
                        "type",
                    ]
                ]
            )
        return results
    else:
        return None
