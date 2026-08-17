# Publishing

Registry: **pub.dev**. Canonical version: **0.1.1**.

The release workflow is tag-bound, verifies conformance and version alignment, and uses a protected GitHub environment. Publishing is considered successful only after the official registry resolves the exact version; a GitHub tag or release is not sufficient.

External one-time setup: The first version must be published by an authorized uploader. Then enable pub.dev automated publishing for customyco/customy-data-flutter, tag pattern v{{version}} and environment pub.dev.

After the registry-side setup is complete, dispatch `.github/workflows/publish.yml` against the immutable `v0.1.1` tag when the registry supports manual dispatch. pub.dev requires a new matching tag push. Never replace a published version or bypass a failed conformance job.
