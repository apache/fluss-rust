---
sidebar_position: 4
---
# Release

This document describes how to create a release of the Fluss clients (fluss-rust, fluss-python, fluss-cpp) from the [fluss-rust](https://github.com/apache/fluss-rust) repository. It follows the [Apache Fluss release guide](https://fluss.apache.org/community/how-to-release/creating-a-fluss-release/) and the [Apache OpenDAL release guide](https://nightlies.apache.org/opendal/opendal-docs-stable/community/release/).

Publishing software has legal consequences. This guide complements the [Product Release Policy](https://www.apache.org/legal/release-policy.html) and [Release Distribution Policy](https://infra.apache.org/release-distribution.html).

## Overview

1. [Decide to release](#decide-to-release)
2. [Prepare for the release](#prepare-for-the-release)
3. [Build a release candidate](#build-a-release-candidate)
4. [Vote on the release candidate](#vote-on-the-release-candidate)
5. [Fix any issues](#fix-any-issues) (if needed, go back to step 3)
6. [Finalize the release](#finalize-the-release)
7. [Promote the release](#promote-the-release)

## Decide to Release

Deciding to release and selecting a Release Manager is a consensus-based decision of the community. Anybody can propose a release on the dev mailing list.

## Prepare for the Release

### One-Time Setup

See [Release Manager Preparation](https://fluss.apache.org/community/how-to-release/release-manager-preparation/) for GPG key setup. For fluss-rust you do **not** need Nexus/Maven.

### Install Rust

The release script uses `git archive` and `gpg`. Building or verifying the project requires Rust (match [rust-toolchain.toml](https://github.com/apache/fluss-rust/blob/main/rust-toolchain.toml)). The dependency list script requires Python 3.11+.

```bash
rustc --version
cargo --version
```

To use `just release`, install [just](https://github.com/casey/just). Otherwise run `./scripts/release.sh $RELEASE_VERSION`.

### Set Environment Variables

```bash
export RELEASE_VERSION="0.1.0"
export RELEASE_TAG="v${RELEASE_VERSION}"
export SVN_RELEASE_DIR="fluss-rust-${RELEASE_VERSION}"
export LAST_VERSION="0.0.9"   # omit for the first release
export NEXT_VERSION="0.2.0"
```

### Generate Dependencies List

Required by [ASF release policy](https://www.apache.org/legal/release-policy.html). Do this on `main` before creating the release branch.

```bash
git checkout main && git pull
python3 scripts/dependencies.py generate
git add **/DEPENDENCIES*.tsv
git commit -m "chore: update dependency list for release ${RELEASE_VERSION}"
git push origin main
```

### Create a Release Branch

```bash
git checkout main && git pull
git checkout -b release-${RELEASE_VERSION}
git push origin release-${RELEASE_VERSION}
```

### Bump Version on Main

```bash
git checkout main && git pull
./scripts/bump-version.sh $RELEASE_VERSION $NEXT_VERSION
git add Cargo.toml
git commit -m "Bump version to ${NEXT_VERSION}"
git push origin main
```

## Build a Release Candidate

### Set RC Variables

```bash
export RC_NUM="1"
export RC_TAG="v${RELEASE_VERSION}-rc${RC_NUM}"
export SVN_RC_DIR="fluss-rust-${RELEASE_VERSION}-rc${RC_NUM}"
```

### Tag and Push

```bash
git checkout release-${RELEASE_VERSION} && git pull
git tag -s $RC_TAG -m "${RC_TAG}"
git push origin $RC_TAG
```

Pushing the tag triggers CI (GitHub Actions: Release Rust, Release Python).

### Create Source Artifacts

```bash
just release $RELEASE_VERSION
# Or: ./scripts/release.sh $RELEASE_VERSION
```

This creates under `dist/`:
- `fluss-rust-${RELEASE_VERSION}-incubating.tgz`
- `fluss-rust-${RELEASE_VERSION}-incubating.tgz.sha512`
- `fluss-rust-${RELEASE_VERSION}-incubating.tgz.asc`

Verify: `gpg --verify dist/fluss-rust-${RELEASE_VERSION}-incubating.tgz.asc dist/fluss-rust-${RELEASE_VERSION}-incubating.tgz`

### Stage to SVN

```bash
svn checkout https://dist.apache.org/repos/dist/dev/incubator/fluss fluss-dist-dev --depth=immediates
cd fluss-dist-dev
mkdir $SVN_RC_DIR
cp ../dist/fluss-rust-${RELEASE_VERSION}-incubating.* $SVN_RC_DIR/
svn add $SVN_RC_DIR
svn commit -m "Add fluss-rust ${RELEASE_VERSION} RC${RC_NUM}"
```

## Vote on the Release Candidate

Start a vote on the dev@ mailing list with subject: `[VOTE] Release Apache Fluss clients ${RELEASE_VERSION} (RC${RC_NUM})`

The vote is open for at least 72 hours. It requires at least 3 PPMC affirmative votes. If the project is in incubation, a second vote on general@incubator.apache.org is required.

## Fix Any Issues

If the vote fails:

1. Fix issues on `main` or the release branch via PRs.
2. Optionally remove the old RC from dist.apache.org dev.
3. Increment `RC_NUM`, recreate tag and artifacts, and repeat.

## Finalize the Release

### Push the Release Tag

```bash
git checkout $RC_TAG
git tag -s $RELEASE_TAG -m "Release fluss-rust, fluss-python, fluss-cpp ${RELEASE_VERSION}"
git push origin $RELEASE_TAG
```

### Deploy Source Artifacts

```bash
svn mv -m "Release fluss-rust ${RELEASE_VERSION}" \
  https://dist.apache.org/repos/dist/dev/incubator/fluss/$SVN_RC_DIR \
  https://dist.apache.org/repos/dist/release/incubator/fluss/$SVN_RELEASE_DIR
```

### Verify Published Packages

- **Rust:** [crates.io/crates/fluss-rs](https://crates.io/crates/fluss-rs)
- **Python:** [PyPI pyfluss](https://pypi.org/project/pyfluss/)
- **C++:** Distributed via the source archive

### Create GitHub Release

1. Go to [Releases > New release](https://github.com/apache/fluss-rust/releases/new).
2. Choose tag `$RELEASE_TAG`, target `release-${RELEASE_VERSION}`.
3. Generate release notes, add notable/breaking changes and download links.
4. Publish.

### Update CHANGELOG.md

Add an entry for `$RELEASE_VERSION` on `main`.

## Promote the Release

- Merge website PRs (release blog, download page).
- Wait 24 hours, then announce on dev@ and announce@apache.org.

## See Also

- [Release Manager Preparation](https://fluss.apache.org/community/how-to-release/release-manager-preparation/)
- [How to Verify a Release Candidate](https://github.com/apache/fluss-rust/blob/main/docs/verifying-a-release-candidate.md)
- [ASF Release Policy](https://www.apache.org/legal/release-policy.html)
