# Contributing

## Local Checks

```sh
pnpm install
go test ./...
moon run docs:build
```

For DynamoDB integration:

```sh
docker run -d --name evt-moto -p 4566:5000 motoserver/moto:5.1.22
terraform -chdir=infra/local init
terraform -chdir=infra/local apply -auto-approve
AWS_ENDPOINT_URL=http://localhost:4566 moon run evt:integration
```

## Pull Requests

Keep framework behavior compatible unless the change is explicitly released as a
breaking change. Add tests for storage, serialization, or retry behavior changes.

Use neutral example names and local emulator values in docs and tests.
