import os, sys, time, json, argparse
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

def es_client():
    load_dotenv()
    return Elasticsearch(
        hosts=[os.getenv("ELASTICSEARCH_HOSTS")],
        basic_auth=(os.getenv("ELASTICSEARCH_USER"), os.getenv("ELASTICSEARCH_PASSWORD")),
        verify_certs=False,
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", required=True, help="Task id from _reindex")
    ap.add_argument("--interval", type=float, default=2.0, help="Polling interval seconds")
    args = ap.parse_args()

    es = es_client()
    while True:
        resp = es.tasks.get(task_id=args.task).body
        completed = resp.get("completed", False)
        status = resp.get("response") or resp.get("task", {}).get("status") or {}
        total = status.get("total", 0)
        created = status.get("created", 0)
        updated = status.get("updated", 0)
        version_conflicts = status.get("version_conflicts", 0)
        pct = (created + updated) / total * 100 if total else 0.0
        print(json.dumps(
            {"completed": completed, "total": total, "created": created,
             "updated": updated, "version_conflicts": version_conflicts,
             "progress_pct": round(pct, 2)}, indent=2))
        if completed:
            break
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
