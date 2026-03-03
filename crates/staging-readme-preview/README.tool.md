# Staging README preview

This crate has **no dependencies**. It exists only to publish to [staging.crates.io](https://staging.crates.io) and preview how the fluss-rs README will look on a crate page. It reuses workspace metadata (edition, license, version, etc.) so package info stays in sync with fluss-rs.

## Usage

1. **Update README** (when you change the real one):
   ```bash
   cp crates/fluss/README.md crates/staging-readme-preview/README.md
   ```

2. **Login to staging** (once; get token from https://staging.crates.io/settings/tokens):
   ```bash
   cargo login --registry staging
   ```

3. **Publish to staging** (from repo root):
   ```bash
   cargo publish -p fluss-rs-readme-preview --registry staging
   ```

4. Open https://staging.crates.io/crates/fluss-rs-readme-preview to view the README.

Bump `version` in the root `[workspace.package]` (or override in this crate's Cargo.toml) if you publish again; staging requires version bumps.
