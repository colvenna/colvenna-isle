# Repository guidance

Read the [API contract](docs/design/api-contract.md) and the design document of the area you change before changing APIs, configuration, schemas, storage formats, deployment or version handling. Keep the contract sources (`src/colvenna_isle/contracts/`), the generated schemas, the design documents, the [user guide](docs/user-guide.md), the [installation guide](docs/installation.md) and the [release notes](docs/release-notes.md) aligned.

Never commit AWS account IDs, role names, credentials, API Keys or deployment identifiers. Code reads the account from `COLVENNA_ACCOUNT` / `AWS::AccountId`; tests use the AWS documentation example account `123456789012`.

For submissions and validation, follow [CONTRIBUTING.md](CONTRIBUTING.md).
