# rgb-service

BiHelix RGB service workspace.

This repository contains the RGB contract and asset libraries extracted from
`btc-local-wallet`. It intentionally excludes Lightning Network node code.

## Crates

- `rgb-aluvm`
- `rgb-api`
- `rgb-coloring`
- `rgb-consensus`
- `rgb-ops`
- `rgb-service-api`
- `rgb-service-local`
- `rgb-schemas`

Internal RGB workspace crates are kept where required:

- `rgb-api/cli`
- `rgb-api/psbt`
- `rgb-ops/invoice`

## Service Boundary

`rgb-service-api` defines the public service boundary for:

- asset and contract management
- balance and allocation queries
- RGB invoice creation
- transfer prepare/commit/cancel
- consignment validate/import
- pending operation and recovery workflows

HTTP support is available behind the `axum` feature. Mutating endpoints accept
signed requests. Operations that move or lock RGB value require an additional
asset spend authorization, intended to be signed by the user wallet or signer.
