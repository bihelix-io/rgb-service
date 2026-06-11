# RGB Consensus

[![Build](https://github.com/rgb-protocol/rgb-consensus/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/rgb-protocol/rgb-consensus/actions/workflows/build.yml)
[![Tests](https://github.com/rgb-protocol/rgb-consensus/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/rgb-protocol/rgb-consensus/actions/workflows/test.yml)
[![Lints](https://github.com/rgb-protocol/rgb-consensus/actions/workflows/lint.yml/badge.svg?branch=master)](https://github.com/rgb-protocol/rgb-consensus/actions/workflows/lint.yml)
[![codecov](https://codecov.io/gh/rgb-protocol/rgb-consensus/branch/master/graph/badge.svg)](https://codecov.io/gh/rgb-protocol/rgb-consensus)

[![crates.io](https://img.shields.io/crates/v/rgb-consensus)](https://crates.io/crates/rgb-consensus)
[![Docs](https://docs.rs/rgb-consensus/badge.svg)](https://docs.rs/rgb-consensus)
[![unsafe forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/rust-secure-code/safety-dance/)
[![Apache-2 licensed](https://img.shields.io/crates/l/rgb-consensus)](./LICENSE)

RGB is confidential & scalable client-validated smart contracts for Bitcoin &
Lightning. To learn more about RGB please check [RGB website][Site].

RGB Consensus library provides consensus-critical and validation code for RGB.

The consensus-critical code library is shared with the following libraries:
1. [AluVM virtual machine][AluVM] used by RGB for Turing-complete smart contract
   functionality.
2. [Strict types][StrictTypes], defining memory layout and serialization of
   structured data types used in RGB smart contracts.

## License

See [LICENSE](LICENSE) file.


[Site]: https://rgb.info
[AluVM]: https://github.com/rgb-protocol/rgb-aluvm
[StrictTypes]: https://github.com/rgb-protocol/rgb-strict-types
