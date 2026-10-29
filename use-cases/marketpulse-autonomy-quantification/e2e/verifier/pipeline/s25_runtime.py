import json
import os

from common.services import ProductServices


def verify(context):
    manifest = context.report_root / "runtime" / "processes.json"
    if not manifest.exists():
        # Fixed case ports are reused across reports. Retire only processes whose
        # executable and kernel start time still match an earlier owned manifest.
        for index, previous in enumerate((context.root / "reports").glob("**/runtime/processes.json")):
            if previous == manifest:
                continue
            records = json.loads(previous.read_text())
            if any(ProductServices.is_running(record) for record in records):
                context.evidence(f"retired-runtime-{index}.json", previous.read_text())
                ProductServices.stop(previous)
    if manifest.exists():
        records = json.loads(manifest.read_text())
        if any(record["run_id"] != context.run_id for record in records):
            context.evidence(f"previous-runtime-round-{context.round_number}.json", manifest.read_text())
            ProductServices.stop(manifest)
    os.environ[context.config["pipeline"]["run_id_env"]] = context.run_id
    return ProductServices(context).start()
