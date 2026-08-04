# Threat Model

## Governed frontier sharing

The protected assets are repository isolation, source confidentiality,
project-local generated state, deterministic retrieval, and the read-only
source boundary. Attackers may control repository names and contents, remotes,
directory names, tool requests, Git metadata inside a repository, and supplied
lineage-like text. Operators control the server configuration, allowed roots,
state directory, and lineage manifest.

Cross-worktree sharing is disabled unless an operator-owned manifest lists
each exact allowed root. Manifest membership is necessary but insufficient:
the service also requires the same canonical Git common directory, a clean
worktree, the same commit, and exact source and deterministic index signatures.
The manifest id is hashed together with its full root set, then bound to the
Git common directory. Consequently, identical names, remotes, files, commits
in separately initialized repositories, or user-supplied lineage values do not
cross the boundary. A missing Git executable, malformed or symlinked manifest,
unsupported repository state, or failed proof disables sharing.

Only positive immutable frontier candidates cross the boundary. Candidate and
dependency ids are opaque SHA-256 addresses. Prompts, paths, project ids,
source text, negative results, references, snapshots, continuation state,
memory, metrics, redaction state, and generations stay in project stores. The
requesting engine validates the shared record, resolves every candidate in its
own index, reranks locally, and rechecks its Git proof, generation, and exact
signatures before use. Failures fall back to normal local search.

Git is executed without a shell and with `GIT_OPTIONAL_LOCKS=0`; commands are
read-only and the canonical project root is passed as a distinct argument.
The shared LMDB lives only under generated server state, has a fixed 256 MiB
budget, reloads with schema filtering, and is subject to age pruning. Corrupt
or unknown records are ignored. These controls do not authorize writes to a
source repository and do not broaden the configured allowed-root boundary.
