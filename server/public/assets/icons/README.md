# CK Wallet PWA icons

The CK Wallet install surfaces generate their icon PNGs on-demand at runtime so we do not need to store binary assets in the repository. Requests to `/assets/icons/*.png` are routed through `server/iconFactory.js`, which paints the icons with the CK Wallet colour palette and serves them with long-lived cache headers.
