#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate bindings/python/GENERATED_README.md from the website docs.

Usage:
    python generate_readme.py          # writes GENERATED_README.md
    python generate_readme.py --check  # exits non-zero if GENERATED_README.md is stale
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DOCS_DIR = SCRIPT_DIR / "../../website/docs/user-guide/python"

LICENSE_HEADER = """\
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
"""

# Files in the order they should appear in the README.
SECTIONS: list[str] = [
    "installation.md",
    "example/index.md",
    "example/configuration.md",
    "example/admin-operations.md",
    "example/log-tables.md",
    "example/primary-key-tables.md",
    "example/partitioned-tables.md",
    "error-handling.md",
    "data-types.md",
    "api-reference.md",
]

FRONTMATTER_RE = re.compile(r"^---\n.*?^---\n", re.MULTILINE | re.DOTALL)


def strip_frontmatter(text: str) -> str:
    return FRONTMATTER_RE.sub("", text, count=1)


def build_readme() -> str:
    parts = [LICENSE_HEADER, "# Fluss Python Client\n"]

    for section in SECTIONS:
        path = DOCS_DIR / section
        if not path.exists():
            print(f"warning: {path} not found, skipping", file=sys.stderr)
            continue
        content = strip_frontmatter(path.read_text()).strip()
        parts.append(content)

    return "\n\n".join(parts) + "\n"


def main() -> None:
    readme = build_readme()
    dest = SCRIPT_DIR / "GENERATED_README.md"

    if "--check" in sys.argv:
        if not dest.exists() or dest.read_text() != readme:
            print("GENERATED_README.md is out of date. Run: python generate_readme.py")
            sys.exit(1)
        print("GENERATED_README.md is up to date.")
        return

    dest.write_text(readme)
    print(f"Wrote {dest}")


if __name__ == "__main__":
    main()
