# rgb-service-api

Shared API layer for BiHelix RGB services.

This crate defines the stable boundary for RGB asset, balance, invoice,
transfer, and pending/recovery operations. It also defines the
authorization model used by HTTP services and SDK callers.

Mutating asset operations require signed requests. Operations that move or lock
RGB value additionally require an explicit asset spend authorization from the
user wallet or signer.
