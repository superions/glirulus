# Contributing to Glirulus

Welcome! Contributions from all experience levels are appreciated. Documentation
improvements, clearer examples, bug reports and small focused pull requests are
valuable ways to help.

You can open a pull request directly; a prior issue is optional. For a larger
change, sharing the problem and proposed direction first can help avoid work
that does not fit the project. Explain what changes and why, and keep unrelated
changes separate.

## Working locally

Use a current Rust toolchain supporting edition 2024. The workspace is standalone;
it does not need a Myoxus checkout. The existing Linux checker requires Bash,
Cargo, GNU `timeout`, `setsid` and `flock`. PostgreSQL integration also needs Docker.

Choose checks relevant to your change:

| Command | What it checks |
| --- | --- |
| `scripts/check.sh build` | Locked workspace library build |
| `scripts/check.sh example` | Complete JSON example, from input `41` to output `42` |
| `scripts/check.sh tests` | Library tests, doctests and the public macro consumer |
| `scripts/check.sh macros` | Public macro consumer alone |
| `scripts/check.sh features` | Default, no-default and all-feature compilation, plus isolated codec profiles |
| `scripts/check.sh postgres` | PostgreSQL integration using disposable Docker containers |

A documentation-only change usually needs a careful read and link review, with
no tests needed. A changed executable example merits running that example.
For code, describe the relevant checks you ran and any limits; every pull request
does not need the full qualification campaign. The checker bounds each Cargo
command, uses this checkout's `target/`, preserves `CARGO_BUILD_JOBS` and rejects
concurrent checks in the same checkout. `--timeout-seconds N` sets the per-command
timeout.

## Reports and pull requests

For bugs, include a minimal reproduction and the expected and actual behavior.
The crate version and Rust/platform details help when relevant. Remove credentials,
private workflow data and sensitive payloads from examples and logs.

For ideas, explain the problem and who would benefit; a proposed solution or
alternatives are welcome but optional. In a pull request, briefly describe what
changed, why and how you checked it. An issue link is optional.

## Licensing

Contribute code under the project's [MIT license](LICENSE). Preserve original
Sayiir grants, Glirulus attribution and package `NOTICE` files when modifying
derived code. The project illustration has its own
[AGPL-3.0-or-later license](artwork/LICENSE) and [attribution](artwork/NOTICE);
it is separate from the MIT-licensed crates and documentation.
