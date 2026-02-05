#!/usr/bin/env python3
"""Simple extractor: read questions.json and output question_id, title, body.

Usage:
  python scripts/extract_questions.py -i questions.json -o extracted.json
  python scripts/extract_questions.py --input questions.json
"""
import json
from typing import List, Dict


def extract_questions(path: str) -> List[Dict]:
    """Return list of dicts with keys: question_id, title, body."""
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    items = data.get("items") or []
    out: List[Dict] = []
    for it in items:
        qid = it.get("question_id")
        title = it.get("title")
        body = it.get("body")
        out.append({"question_id": qid, "title": title, "body": body})
    return out


def main() -> None:
    import argparse, sys

    parser = argparse.ArgumentParser(description="Extract questions from questions.json")
    parser.add_argument("-i", "--input", default="questions.json", help="Path to questions.json")
    parser.add_argument("-o", "--output", help="Write output JSON to this file (default: stdout)")
    args = parser.parse_args()

    try:
        res = extract_questions(args.input)
    except Exception as e:
        print(f"Error reading {args.input}: {e}", file=sys.stderr)
        raise

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump(res, fh, ensure_ascii=False, indent=2)
    else:
        json.dump(res, sys.stdout, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
