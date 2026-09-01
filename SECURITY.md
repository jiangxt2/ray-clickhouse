# Security Policy

## Supported versions

Security fixes target the latest supported release and the latest commit on `master`. Version-specific support windows and any end-of-support notice are stated in versioned release notes.

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public GitHub Issue, pull request, discussion, log, benchmark artifact, or test fixture.

Use GitHub's private vulnerability reporting entry for this repository when it is available. If that entry is unavailable, contact the repository owner through their GitHub profile and request a private reporting channel without including vulnerability details in the initial public message.

Include the affected version or commit, impact, minimal reproduction, preconditions, and any known mitigations only in the agreed private channel. Do not include credentials, access tokens, production endpoints, or unrelated user data.

## Security boundaries

- Prefer `password_env` to literal passwords; resolved credentials must not enter logs, repr output, artifacts, or serialized public configuration.
- Treat a timed-out or disconnected INSERT as potentially committed and reconcile `AmbiguousWriteError` outcomes before retrying.
- Treat table-management failures after destructive steps as potentially partial.
- Treat `filter` as a trusted scalar predicate and bind values through `query_parameters`.
- Do not describe the connector as providing transactions, snapshot isolation, exactly-once writes, or connector-side Distributed shard routing.
