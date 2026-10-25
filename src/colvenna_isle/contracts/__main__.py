"""Generate/check artifacts or validate a JSON/YAML document without AWS calls."""

import argparse
import json
from pathlib import Path

from .model import artifacts
from .validation import ContractError, load_document, validate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate")
    generate.add_argument("--check", action="store_true")
    check = commands.add_parser("validate")
    check.add_argument("contract")
    check.add_argument("document", type=Path)
    check.add_argument("--context", type=Path)
    check.add_argument("--definition", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "generate":
            folder = Path(__file__).parent / "v2"
            folder.mkdir(exist_ok=True)
            for name, value in artifacts().items():
                content = json.dumps(value, sort_keys=True, indent=2) + "\n"
                path = folder / name
                if args.check:
                    if not path.exists() or path.read_text() != content:
                        raise ContractError(f"Generated contract differs: {name}")
                else:
                    path.write_text(content)
            print(json.dumps({"valid": True, "contract_version": 2, "generated_artifacts": 4}))
        else:
            validate(args.contract, load_document(args.document),
                     context=load_document(args.context) if args.context else None,
                     definition=load_document(args.definition) if args.definition else None)
            print(json.dumps({
                "valid": True, "contract_version": 2, "validation_scope": "OFFLINE_CONTRACT_ONLY",
                "not_validated": ["authentication/authorization", "SQL sandbox", "Parquet content",
                                  "cloud identity/availability", "runtime publication"],
            }))
        return 0
    except (ContractError, json.JSONDecodeError) as error:
        print(json.dumps({"valid": False, "error": str(error)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
