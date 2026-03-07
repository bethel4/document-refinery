from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.agents.query_agent import QueryAgent


def main() -> int:
    parser = argparse.ArgumentParser(description="Ask a document question and print provenance chain.")
    parser.add_argument("--document-id", required=True)
    parser.add_argument("--document-name", required=True)
    parser.add_argument("--question", required=True)
    parser.add_argument("--pageindex", required=True, help="Path to pageindex tree json")
    parser.add_argument("--open-first", action="store_true", help="Open first cited page using xdg-open")
    args = parser.parse_args()

    pageindex_path = Path(args.pageindex)
    if not pageindex_path.exists():
        raise SystemExit(f"PageIndex file not found: {pageindex_path}")

    tree = json.loads(pageindex_path.read_text(encoding="utf-8"))
    agent = QueryAgent()

    result = agent.answer_query(
        question=args.question,
        document_id=args.document_id,
        document_name=args.document_name,
        pageindex_tree=tree,
    )

    print("Answer:")
    print(result.get("answer", ""))
    print("\nProvenanceChain:")
    for i, c in enumerate(result.get("provenance_chain", []), start=1):
        print(
            f"{i}. doc={c.get('document_name')} page={c.get('page_number')} "
            f"bbox={c.get('bbox')} hash={str(c.get('content_hash'))[:16]} "
            f"status={c.get('verification_status')}"
        )
        print(f"   open: {c.get('open_command')}")

    if args.open_first and result.get("provenance_chain"):
        import subprocess

        open_cmd = result["provenance_chain"][0].get("open_command")
        if open_cmd:
            subprocess.run(open_cmd, shell=True, check=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
