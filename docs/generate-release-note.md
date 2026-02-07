<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Generate Release Note

Use GitHub's **Generate release notes** to produce a draft from merged PRs between tags. Categories (Added, Fixed, Docs, etc.) are configured in [.github/release.yml](../.github/release.yml).

1. Go to [Create a new release](https://github.com/apache/fluss-rust/releases/new).
2. In **Choose a tag**, pick the release tag (e.g. `v0.1.0`) or type `draft` if the tag does not exist yet.
3. Click **Generate release notes**.
4. Copy the generated content for **CHANGELOG.md** or the GitHub Release description. When publishing the release, add the official download link, checksums/verification, and install instructions (see [creating-a-release.md](creating-a-release.md)).

See [creating-a-fluss-rust-release.md](creating-a-fluss-rust-release.md) and [GitHub: Automatically generated release notes](https://docs.github.com/en/repositories/releasing-projects-on-github/automatically-generated-release-notes).
