# Integration test ledger

> The comparison harness and its associated dry-run/formal suites were retired
> after review. Rows beginning with `Official comparison` are historical
> diagnostics only and are not release or contribution gates. Current validation
> follows the repository's normal unit, contract, integration, and Ray-cluster
> suites.

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| `tests/integration` with ClickHouse Docker | Real ClickHouse 24.8, Ray Data local runtime, filtered single read, logical Date/Decimal/DateTime values, empty table, View/aggregate View, Distributed policy, partition/range split, sync/async append, create/overwrite, schema rejection, permission failure | Enhanced implementation after DDL preflight, explicit nullability, precision-preserving type mapping, ambiguous table-management errors, and metadata tracing | Final compatibility regression after review fixes | Passed: 11 tests, 1 expected multi-node skip, 2026-08-22 |
| `tests/integration` in Ray Docker cluster | ClickHouse 25.3 fixed digest, Ray 2.55.1 head plus two workers, Ray Jobs driver, same read/write and semantic policy suite | Same enhanced implementation; runner uses the built worktree package | Combined multi-node Ray and ClickHouse 25.3 compatibility validation after review fixes | Passed: 12 tests, 2026-08-22 |
| `tests/integration` with ClickHouse Docker | ClickHouse 26.8.1.2041 at multi-architecture digest `sha256:9f8fa0d51f86611fda226c4085cdafdddce35312a60c934de2e4450d61fcb99a`, Python 3.12 and Ray 2.58 local runtime; full read/write/schema/DDL/error suite including MaterializedView rejection | `public-safety-current-it` implementation candidate after public API, receipt, engine policy, current-image, CI and Docker lifecycle changes | Required current-LTS real-protocol evidence before the final distributed suite | Passed: 12 tests, 1 expected cluster-only skip, 2026-09-01; pytest/Compose/image evidence saved under `.artifacts/it`, exact `ray-clickhouse-it` Compose resources removed |
| Ray 2.58 cluster runtime build preflight | Ray 2.58.0/Python 3.12 base at multi-architecture digest `sha256:c3c9573c5c6bfe4127885f79622d6a32064d34cafc7d156ec728aab8657be250`; dependency installation and `pip check` before Compose startup | First `public-safety-current-it` cluster candidate installed the complete project `dev` extra in the ephemeral runtime | Required dependency-consistent image before the final distributed suite | Failed before Compose and pytest: Twine/Keyring upgraded `urllib3` to 2.7, conflicting with base-image `botocore 1.32.7` requirement `<2.1`; no Compose resources or temporary runtime image remained; 2026-09-01 |
| `tests/integration` in Ray Docker cluster | ClickHouse 26.8.1.2041 at digest `sha256:9f8fa0d51f86611fda226c4085cdafdddce35312a60c934de2e4450d61fcb99a`; Ray 2.58.0/Python 3.12 base at digest `sha256:c3c9573c5c6bfe4127885f79622d6a32064d34cafc7d156ec728aab8657be250`; Ray head plus two workers and Ray Jobs driver | Corrected `public-safety-current-it` candidate with runtime-only project dependencies plus pytest; runtime image built once and Compose started with `--no-build` | Final current-version distributed execution and image-lifecycle evidence after the failed dependency preflight | Passed: 13 tests, 2026-09-01; runtime image `sha256:01887ed79fc74d03c1edc755e21dbcecd536311ff85422400c17aa0ac3f9bc78` and exact Compose resources removed; dangling-image before/after records identical; evidence saved under `.artifacts/it/ray-cluster` |
| Official comparison local infrastructure smoke | Minimal deterministic ClickHouse fixture; official/external normal read; one external post-commit/pre-ack response loss; one official post-commit Ray worker loss; comparison Docker context allowlist; exact Compose/image cleanup | Final Unit A harness on branch `official-comparison`, base HEAD `1b1995155dd8bd68c6e537daa8b9348f80ce7c83`; scenario digest `f64cb0de1cdbe043f24968508a9ae475de4dd0e111ae4df3027942178b4a7237`; controller lock `158001d03d24c7816a5f50acf0f069653b2ebcf8e23fa622a0387762d4774bef`; result schema `43953ee6afea0bd9839e5de0a9ec1a6065fddeca46caa9d626bf372d4c7ef63c` | Required one-time real-infrastructure validation before Unit A can be considered implementation-complete; expected duration 15–30 minutes; no overlap with the four earlier pre-scenario preflights, which produced no comparison result | Failed before image build, 2026-09-01: BuildKit frontend digest resolution returned registry 401; no Compose resources or comparison scenario ran; explicit domestic-mirror frontend pull succeeded afterward, and a rerun requires renewed approval |
| Official comparison Dockerfile frontend preflight | Pinned Dockerfile frontend `docker.m.daocloud.io/docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e` | Final Unit A smoke candidate after the pre-build 401; direct `docker pull` with proxy variables cleared | Verify the exact frontend digest is available locally before the next and only renewed smoke attempt | Passed: digest returned exactly as declared; no Compose resources or comparison scenario ran; 2026-09-01 |
| Official comparison runtime image preflight | Immutable external and harness wheels, Ray 2.58.0/Python 3.12 dual environments, hashed Linux requirements, and deny-all Docker context; no Compose services | First complete Unit A smoke candidate on branch `official-comparison`; base HEAD `1b1995155dd8bd68c6e537daa8b9348f80ce7c83` | Required image build before the first local smoke | Failed before runtime image completion and before Compose startup, 2026-09-01: content-addressed wheel contexts renamed files to non-standard `ray_clickhouse.whl`/`comparison.whl`, so pip rejected the external wheel filename; no comparison container, network, or volume was created, the target runtime image was not produced, and build cache was retained; fix preserves standard wheel basenames |
| Official comparison runtime image hash preflight | Same immutable wheels, hashed Linux requirements, and deny-all context after preserving standard wheel basenames; no Compose services | Second Unit A smoke candidate after the first image-build fix | Verify dependency and local-wheel supply-chain enforcement before Compose | Failed before runtime image completion and before Compose startup, 2026-09-01: pip correctly enabled `--require-hashes` for the compiled requirements and rejected the local external wheel when it was appended to the same install command without an inline requirement hash; no comparison container, network, or volume was created and no target runtime image was produced; fix installs each hashed lock first, then installs separately SHA-256-recorded local wheels with `--no-deps` |
| Official comparison first Compose preflight | Successfully built one runtime image with official/external Ray 2.58.0 environments and separately hashed wheels; started the first official normal-read Compose project | Third Unit A smoke candidate after wheel hash installation fixes | Validate the first real Ray/ClickHouse comparison cluster before scenario execution | Failed before Ray readiness and before any comparison scenario, 2026-09-01: the isolated `ray[data]` environment correctly lacked optional dashboard dependencies while Compose requested `--include-dashboard=true`; exact Compose containers/network and the target runtime image were removed, no comparison result was produced, and build layers were retained; fix disables the dashboard and runs the comparison process directly as an external Ray driver instead of using Ray Jobs |
| Official comparison lightweight controller preflight | Successfully built the dual runtime image, started healthy Ray head/workers/runner and ClickHouse, and launched the comparison proxy service | Fourth Unit A smoke candidate after disabling Dashboard/Ray Jobs and separating the lightweight controller | Validate that the controller-only proxy starts without contaminating official/external environments | Failed before any comparison scenario, 2026-09-01: the controller correctly omitted Ray/PyArrow, but package `__init__` eagerly imported the PyArrow-dependent fixture module, so the proxy process exited; exact Compose resources and target image were removed; fix makes package initialization import-free so each scenario module owns its runtime dependencies |
| Official comparison harness wheel identity preflight | Current source had import-free package initialization, but the built harness wheel embedded the earlier eager-import initializer; healthy Ray/ClickHouse containers were torn down after the proxy loaded that stale wheel | Fourth Unit A smoke candidate forensic follow-up | Verify the wheel installed in every runtime is byte-derived from the current harness source | Failed before any comparison scenario, 2026-09-01: `uv build --project comparison/official` was used as if `--project` selected the source tree and reused a same-version cached artifact; exact Compose resources and target image were removed; fix passes `comparison/official` as the positional build source, forces PEP 517 with `--no-cache`, then extracts and byte-compares the wheel initializer with current source before Docker build |
| Unit A `master` push ClickHouse CI | Existing required ClickHouse 26.8 integration job on the final Unit A commit | Unit A retirement commit `3f38d72` after cumulative review | Normal exact-SHA regression for the final CI/package state; comparison smoke is retired and not repeated | Passed in CI run `33728841222`; all jobs green and release candidate record generated |
| Official comparison GitHub-hosted dry-run | GitHub-hosted `ubuntu-24.04` x86_64 identity, immutable official/external wheels, paired public-API smoke, sanitization, and artifact flow | Candidate `b48b8f7e0581316ffa572683fee44ce7f25216eb`; scenario digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f` | Validate remote profile and artifact lifecycle before formal comparison; run `33636851403` | Passed: 2/2 behavior results valid; main artifact `9849353296` (`sha256:62945b518e13d6ee05c86d5c329a0174fb88188d1a44173bcd46eddb6f96eb9b`), provenance artifact `9849354303` (`sha256:becce09c51d5a01e9ff8c8c380b4f63e0b82e943444a6d3a42ee88d3d89131c0`); paired resource metrics intentionally empty for bounded dry-run; 2026-09-02 |
| Official comparison formal reference run | All predeclared correctness, planning, resource, type, and one-shot fault scenarios in one GitHub-hosted paired job | Retired comparison experiment | Formal comparison and Unit B evidence import were cancelled after review; no upstream or release conclusion depends on them | Retired; no run will be scheduled |
| Official comparison local infrastructure smoke rerun | Same four approved smoke cases and exact resource/context/teardown checks after explicit frontend pre-pull and pip retry fixes | Branch `official-comparison`, base HEAD `1b1995155dd8bd68c6e537daa8b9348f80ce7c83`; runner `6466d710eb82b94c3095fab2f19d92941fb174d8f01395af958d7ed8ec255e82`; Dockerfile `6e6fddbe8afe3483856de33c581043ddb7072ebf4f1f69476a5d1cbd25d3c24f`; scenario `4fb4af7c19ce1c6af34c7a4a2fcaa52a1245491b71159e776099de6f742a37bc`; controller lock `158001d03d24c7816a5f50acf0f069653b2ebcf8e23fa622a0387762d4774bef` | Renewed final smoke approval after the prior pre-build and dependency-install failures; no overlap with prior no-scenario attempts; expected duration 15–30 minutes | Interrupted before scenario, 2026-09-01: Ray readiness probe had no per-attempt timeout and was manually interrupted after exact resource cleanup; proxy upstream healthcheck was also too shallow; no comparison result produced |
| Official comparison dependency-install preflight | Same hashed dual Linux requirements and pip cache, with fixed `PIP_DEFAULT_TIMEOUT=60` and `PIP_RETRIES=8` | Dockerfile after the external-install timeout failure; no Compose services | Bound the retry/timeout mitigation before this renewed smoke; expected duration under 10 minutes | Passed as part of current build; no Compose resources or comparison scenario ran |
| Official comparison readiness preflight | Proxy `/ping` healthcheck reaches ClickHouse; Ray attachment probe has a 10-second timeout and exact child termination | Compose `74b5516ffae4ef837f1324dad003326edb8e3ec13eb1f88f2ddf4b2ee558ba2c`; runner `0a5507d8d06a6a45809521b60f83974fc4f7d078c7a11b98b3faee07436b1c30`; Dockerfile `6e6fddbe8afe3483856de33c581043ddb7072ebf4f1f69476a5d1cbd25d3c24f` | Renewed smoke approval after the interrupted readiness attempt; expected duration 15–30 minutes; no overlap with prior no-scenario attempts | Failed before runner readiness, 2026-09-01: proxy healthcheck assumed `Ok` was present in the first 128-byte read and marked the healthy ClickHouse upstream unhealthy; Compose exact resources were removed and no comparison scenario ran |
| Official comparison proxy-health preflight | Proxy healthcheck performs a complete standard-library HTTP request to ClickHouse `/ping` through the proxy and validates status 200 | Compose `0f46535f2391e0729c3aeab30591662087bff108b5647ff99bd9b4e9f63821a1`; runner `6e38141d91c08937380cca9b9483b3f31b560f5d9c5976fdeeb13aa1de39a380`; Dockerfile `6e6fddbe8afe3483856de33c581043ddb7072ebf4f1f69476a5d1cbd25d3c24f`; scenario `4fb4af7c19ce1c6af34c7a4a2fcaa52a1245491b71159e776099de6f742a37bc`; controller lock `158001d03d24c7816a5f50acf0f069653b2ebcf8e23fa622a0387762d4774bef` | Renewed final smoke approval after the healthcheck false-negative; expected duration 15–30 minutes; no overlap with prior no-scenario attempts | Failed before scenario, 2026-09-02: Compose reached healthy ClickHouse/proxy/Ray services, but the driver in an independent runner container could not complete Ray's local node-discovery path; no comparison result produced and exact resources were removed |
| Official comparison Ray driver-placement preflight | Driver, preparation, warmup, and readiness run inside `ray-head` with `ray.init(address="auto")`; runner remains an idle lifecycle peer | Compose `4e499df7ff7e54d57f9af7c528fe163e8fa120adc8606e4ab4d50f11c11adb67`; runner `7e9bfce1db25265eef011870d264d4a5cf533af222828bf69164cbd39363e998`; scenario `4fb4af7c19ce1c6af34c7a4a2fcaa52a1245491b71159e776099de6f742a37bc`; controller lock `158001d03d24c7816a5f50acf0f069653b2ebcf8e23fa622a0387762d4774bef` | Renewed final smoke approval after the Ray local-node discovery diagnosis; expected duration 15–30 minutes; no overlap with prior no-scenario attempts | Failed before runner readiness, 2026-09-02: explicit ray-head environment mapping dropped `RAY_COMPARISON_RUNTIME`, expanding the Ray executable path to `/opt/ray-clickhouse//bin/ray`; exact Compose resources were removed and no comparison scenario ran |
| Official comparison ray-head environment preflight | ray-head explicitly carries runtime selector, usage flag, head/worker Object Store variables, ClickHouse endpoints, and `RAY_COMPARISON_RAY_ADDRESS=auto` | Compose `36c2f7bd1d1d2831660a4029b9359bf8488fa125f8cb91a045afecc5e79d8102`; runner `7e9bfce1db25265eef011870d264d4a5cf533af222828bf69164cbd39363e998`; scenario `4fb4af7c19ce1c6af34c7a4a2fcaa52a1245491b71159e776099de6f742a37bc`; controller lock `158001d03d24c7816a5f50acf0f069653b2ebcf8e23fa622a0387762d4774bef` | One bounded final validation after the complete readiness/environment diagnosis; expected duration 15–30 minutes; no overlap with prior no-scenario attempts | Failed before runner readiness, 2026-09-02: ray-head environment mapping dropped `RAY_COMPARISON_RUNTIME`, expanding the executable path to `/opt/ray-clickhouse//bin/ray`; exact Compose resources were removed and no comparison scenario ran |
| Official comparison fault-boundary preflight | Response-loss accepts driver/head source; worker-loss sets `num_cpus=1` on both public write APIs; task evidence waits for terminal attempts | Runner `cda50e278cca7f6ad71de370ee77321ba8ca7be33aad6527c714c4224459fe1b`; script `7e9bfce1db25265eef011870d264d4a5cf533af222828bf69164cbd39363e998`; Compose `36c2f7bd1d1d2831660a4029b9359bf8488fa125f8cb91a045afecc5e79d8102`; scenario `4fb4af7c19ce1c6af34c7a4a2fcaa52a1245491b71159e776099de6f742a37bc` | One bounded final smoke after actual case evidence exposed the fault-source and task-state defects; no overlap with prior three successful/invalid case executions | Failed: run `smoke-20260902t020424z-56282`, official/external normal reads passed, but external post-commit response loss was retried by clickhouse-connect and did not produce `AmbiguousWriteError`; non-resource empty-array expansion was not reached because result validation stopped first; exact Compose resources and runtime image were removed; evidence under `.artifacts/comparison/smoke-20260902t020424z-56282`; 2026-09-02 |
| Official comparison final local infrastructure smoke | Four approved smoke cases: official/external normal read, external post-commit/pre-ack response loss, official post-commit worker loss; exact context, image, Compose, query/task, resource, checksum and teardown evidence | Runner `cda50e278cca7f6ad71de370ee77321ba8ca7be33aad6527c714c4224459fe1b`; script `59506f0e84cdc718fc7f3a9e7ba55dc66de348b2bc13dd350cb7c53b8f26ae49`; fault proxy `0f55602fbd842dd2bfd87776cf8fabb01afe031d708a0efbbe1192a9d637117d`; Compose `36c2f7bd1d1d2831660a4029b9359bf8488fa125f8cb91a045afecc5e79d8102`; scenario `4fb4af7c19ce1c6af34c7a4a2fcaa52a1245491b71159e776099de6f742a37bc`; controller lock `158001d03d24c7816a5f50acf0f069653b2ebcf8e23fa622a0387762d4774bef` | Final bounded real-infrastructure validation after correcting client-safe-close handling, SQL INSERT detection, and non-resource summary invocation; no overlap with earlier failed/no-scenario runs | Passed: run `smoke-20260902t021822z-76058`, `summary.json` reports 4/4 valid; external response-loss records `AmbiguousWriteError`, one fault event, 2 inserted rows and 2 task attempts; official worker-loss records one fault event, 4 inserted rows and 4 task attempts; all measured read resources have complete telemetry; dangling-image baseline unchanged and exact Compose resources removed; evidence under `.artifacts/comparison/smoke-20260902t021822z-76058`; 2026-09-02 |
| Official comparison corrected smoke preflight | Four approved smoke cases after streaming block, Write-task, driver-RSS, provenance, allowlist, base-image, artifact-directory, cleanup, and column-order fixes | Runner `48220de38ac1ac10c6522d216f2f38dd9d8b831c7a1f5a11d619c93e8e305c98`; metrics `d19a627a19cf084992e79377f25e1d9963acfa18206182aaa6ace454af9b2c04`; evidence `5f203d3bde1a75f2c22dfe29ecd9b54d5c5c748ba579cbe265e429c2ae289f44`; config `9b1f15d4a2fb04bbde48fd8931c86b546798e30d6121748305ca1df222ec9478`; fixtures `be71aec2c82c50d166ba39d6a11b788cff1e3188601a4e7dd7bc6fa68ae0302a`; manifest CLI `8ff5861f0903ef67ac9a8d49bc78637e374e6bab517b0beed3823b91bd67fc48`; script `d533272e7e97624594dffa6beff9de9f495f4f5db576828a41514ef0c5a8720a`; Dockerfile `754e87a7be3e7ed0677c458c64cae81dff28f44867ce290e13b0618820c848b2`; workflow `2be66c7437c60311a8e1bfd7693d621d5ac356e17d226bb379ccc10386a53067`; scenario manifest digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f` (scenarios.toml file SHA `e6acdf5c2e52adb492710ca4936650c71be171805ff679e871edc5813e7d02be`); controller lock `158001d03d24c7816a5f50acf0f069653b2ebcf8e23fa622a0387762d4774bef` | One final real-infrastructure run after the full corrected harness gate | Failed before result validation: baseline resource sampling consumed the short normal read window, leaving only sample 0; exact Compose resources and runtime image were removed; evidence under `.artifacts/comparison/smoke-20260902t035159z-16806`; 2026-09-02 |
| Official comparison baseline-gated final smoke | Four approved smoke cases after adding a baseline-ready handshake between the resource sampler and measured driver | Runner `576cd6ab27525333a7b29355819a538dcd99509db5a2dad9ff583f88c029330c`; metrics `d19a627a19cf084992e79377f25e1d9963acfa18206182aaa6ace454af9b2c04`; evidence `be02993dc4ff6297993c3586b7b192756f450128f7df9c476ff45417396585a3`; config `9b1f15d4a2fb04bbde48fd8931c86b546798e30d6121748305ca1df222ec9478`; fixtures `be71aec2c82c50d166ba39d6a11b788cff1e3188601a4e7dd7bc6fa68ae0302a`; manifest CLI `8ff5861f0903ef67ac9a8d49bc78637e374e6bab517b0beed3823b91bd67fc48`; script `cb818cdb2008cfc568adae5b72dee820790a4983462a31316e93956bee0c33ca`; Dockerfile `754e87a7be3e7ed0677c458c64cae81dff28f44867ce290e13b0618820c848b2`; workflow `2be66c7437c60311a8e1bfd7693d621d5ac356e17d226bb379ccc10386a53067`; manifest schema `3328c87c69d41b22c6492756c893c4f50fdc2bba3157282712eb1a3adebf74b3`; scenario manifest digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f` (scenarios.toml file SHA `e6acdf5c2e52adb492710ca4936650c71be171805ff679e871edc5813e7d02be`); controller lock `158001d03d24c7816a5f50acf0f069653b2ebcf8e23fa622a0387762d4774bef` | One final real-infrastructure smoke after diagnosing and fixing the baseline sampling race; no overlap with the failed preflight because the measurement synchronization changed | Passed: run `smoke-20260902t040053z-28888`, 4/4 results valid; streaming results use whole underlying blocks with `driver_process_sample_count=2` and complete telemetry for both resource read cases; external response-loss records one failed Write attempt, one insert attempt, and 2 final rows; official worker-loss records `Write` states `FAILED,FINISHED`, two Write attempts, four insert attempts, and 4 final rows; provenance, strict raw-tree sanitizer, exact cleanup, and unchanged dangling-image baseline verified; evidence under `.artifacts/comparison/smoke-20260902t040053z-28888`; 2026-09-02 |
| Official comparison remote-gate correction smoke | Existing four smoke cases after complete-evidence validation, diagnostics separation, strict Object Store location/service checks, and fixed Python 3.12 classifier | Metrics `1756d6e1395d05ec23dbc4330cea230bb91eb4dff4a63c1c655b65fa4b4eb6fc`; evidence `5b393c74fc7ebf04b0616ee35750216150869ad617cdf77de25c6cffc0a28cfc`; manifest CLI `ebc52831ef12db3f0a0ea088f4b92cfdcfd627979f4e87444c217edcecd90ad6`; workflow `bb3ca561cffa33d160cfbfd3e3199e96a7461b186d0131016b66a2d9a747caee`; CI `4840d61b439860b8927d6ecddd715f17a3f2b8966aba2538b6a1fe061e11b037`; scenario digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f` | One necessary real-infrastructure run because resource telemetry validation changed runtime acceptance; no overlap with prior smoke due changed measurement validity contract | Failed at official worker-loss sampler cleanup: killed worker made optional non-resource `compose exec` return nonzero under `set -e`; exact Compose resources and runtime image were removed; evidence under `.artifacts/comparison/smoke-20260902t064333z-38714`; 2026-09-02 |
| Official comparison worker-loss cleanup smoke | Same four approved smoke cases after making optional telemetry failures non-fatal for non-resource worker-loss scenarios | Script `82a6a22dc128cc9ba8fffc7907ed5edba397e0b4df7b958e5fd96940d1c5428a`; metrics `1756d6e1395d05ec23dbc4330cea230bb91eb4dff4a63c1c655b65fa4b4eb6fc`; scenario digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f` | One necessary real-infrastructure run after diagnosing the optional-sampler failure; no overlap with the failed run because only the cleanup error propagation changed | Failed before resource summary: strict Object Store validation found sparse idle baseline locations, so the parser required an unavailable baseline series; exact Compose resources and runtime image were removed; evidence under `.artifacts/comparison/smoke-20260902t065050z-51180`; 2026-09-02 |
| Official comparison sparse-baseline final smoke | Same four approved smoke cases after allowing explicit sparse idle baseline locations while requiring complete measured samples | Metrics `7b96a11322181593e09fe644bc14e908b469a357c0e51bcaf17dd1b3c3ea4d63`; evidence `5b393c74fc7ebf04b0616ee35750216150869ad617cdf77de25c6cffc0a28cfc`; script `82a6a22dc128cc9ba8fffc7907ed5edba397e0b4df7b958e5fd96940d1c5428a`; workflow `bb3ca561cffa33d160cfbfd3e3199e96a7461b186d0131016b66a2d9a747caee`; CI `4840d61b439860b8927d6ecddd715f17a3f2b8966aba2538b6a1fe061e11b037`; scenario digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f` | One final real-infrastructure run after diagnosing sparse Ray Object Store baseline semantics; no overlap with the failed run because telemetry acceptance changed | Passed: run `smoke-20260902t070026z-68884`, 4/4 results valid; measured samples contain all declared services and locations, sparse idle baseline locations are explicitly counted; external response-loss records one failed Write attempt, one insert attempt, and 2 final rows; official worker-loss records `FAILED,FINISHED`, two Write attempts, four insert attempts, and 4 final rows; exact Compose cleanup and unchanged dangling-image baseline verified; 2026-09-02 |
| Official comparison final telemetry-gated smoke | Same four approved smoke cases after resource-summary now marks sparse Object Store baselines incomplete and rejects them for resource-required cases | `__main__.py` telemetry gate; metrics `7b96a11322181593e09fe644bc14e908b469a357c0e51bcaf17dd1b3c3ea4d63`; scenario manifest digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f`; scenarios.toml file SHA `e6acdf5c2e52adb492710ca4936650c71be171805ff679e871edc5813e7d02be` | One final smoke on the final code state; no overlap with the prior run because the acceptance contract changed | Passed: run `smoke-20260902t072328z-4833`, 4/4 results valid; all three read/transport cases had complete telemetry and zero missing baseline locations; official worker-loss remained valid with telemetry explicitly incomplete after the killed worker; external response-loss and official replay assertions passed; exact Compose cleanup and unchanged dangling-image baseline verified; 2026-09-02 |
| Official comparison strict service/location smoke | Four approved smoke cases after changing Object Store validation from sample-global to per-service/location | Current uncommitted metrics strictness; scenario manifest digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f` | One diagnostic run to validate the review-required cross-product check; no reuse of prior smoke because measured telemetry acceptance changed | Failed before result validation: minimal fixture had a missing measured `ray-worker-2/WORKER_HEAP` series; the failure proved strict resource mode is fail-closed, and exact Compose/runtime cleanup completed; evidence under `.artifacts/comparison/smoke-20260902t081514z-78497`; 2026-09-02 |
| Official comparison evidence-integrity smoke | Four approved smoke cases after complete log/telemetry file validation, per-service Object Store checks, recursive JSON allowlist, provenance checksum separation, and temporary-build cleanup | Current uncommitted Unit A harness/workflow changes on branch `official-comparison`; scenario manifest digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f` | One necessary final infrastructure validation because resource telemetry acceptance and evidence publication contracts changed; expected duration 15–30 minutes; no overlap with prior smoke evidence | Passed: run `smoke-20260902t082640z-95267`, 4/4 behavior results valid; all required operational files were produced, per-service Object Store missing locations are recorded and telemetry is explicitly incomplete for the minimal fixture, strict resource mode would reject these samples; response-loss/replay assertions, exact Compose cleanup, and unchanged dangling-image baseline verified; 2026-09-02 |
| Official comparison summary/sanitizer gate | Recompute summary and complete sanitizer from `smoke-20260902t082640z-95267` using telemetry-gated pairing, recursive JSON/JSONL allowlists, and symlink/binary policy | Current `summary.py`, `evidence.py`, and CLI changes; no Docker or Ray execution | Short post-change evidence integrity validation; no overlap with infrastructure smoke; raw `summary.json` remains immutable and is not overwritten; derived output is `.artifacts/comparison/derived/smoke-20260902t082640z-95267/summary.json` | Passed: all incomplete smoke rows have zero paired resource metrics and no resource/timing fields, including `query_duration_ms`, in their derived numeric summaries; complete sanitizer accepted the artifact; symlink and complete-binary rejection tests passed; 2026-09-02 |
| Official comparison ray-doris lifecycle smoke | Four approved smoke cases after switching to one shared cluster per runtime side, clean resource-side boundaries, side-specific Ray environments, and explicit worker-loss markers | Runner `eb0e599c4a12c8d796f10760bfc9c749d38fb574`; Compose `dcd78561de1a4913c688fbb6aa634416673464e2`; runner implementation `4059be597ab1456d790aa097f0b99802b3130338`; evidence `3ea52e679d041d49adc91bfd5ae0e55a54f74ae0`; scenario digest `0c67ad12e32f9d67115dd73af409d975c49fae34023b31a8ba6c98e4cebdca2f` | One necessary final real-infrastructure smoke after lifecycle and runtime-isolation changes; no overlap with prior per-case or mixed-runtime failures | Passed: run `smoke-20260903t042606z-93874`; 4/4 results valid, external response-loss and official worker-loss boundaries proven, worker-loss recorded two Write attempts and four inserted rows, complete sanitizer replay passed, each side's Compose project and runtime image removed, dangling-image baseline unchanged; 2026-09-03 |

## Read diagnostics

### Initial full suites

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| clickhouse final (ray-clickhouse-read-error-causes) | Complete existing and new integration cases | Content SHA256 b6018e51bb87d1305e8c6f694ce7836279be1d0b60393664081bf79ece856180; Python 3.12 / Ray 2.58 / ClickHouse 26.8 | Authorized final run; distinct branch and infrastructure baseline | Passed (exit 0); evidence `.artifacts/implementation/clickhouse-final-b6018e51bb87` |
| ray-cluster final (ray-clickhouse-read-error-causes) | Complete existing and new integration cases | Content SHA256 b6018e51bb87d1305e8c6f694ce7836279be1d0b60393664081bf79ece856180; Python 3.12 / Ray 2.58 / ClickHouse 26.8 | Authorized final run; distinct branch and infrastructure baseline | Passed (exit 0); evidence `.artifacts/implementation/ray-cluster-final-b6018e51bb87` |

### Review correction validation

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| Complete ClickHouse IT after diagnostic corrections | All 29 collected integration cases, including the new public worker-token failure | Runtime fingerprint `a1835f330a8c92e6f90125867e45485b32c255472df1fb83d6bf0465193b8fc2`; detailed files in `.artifacts/review-fixes/summary.json` | Prior 27 passed plus one cluster-only skip cover the previous implementation; redaction and worker-token IT changed | Pending explicit rerun approval; not executed |
| Complete Ray-cluster IT after diagnostic corrections | All 29 cases on the existing three-node Ray 2.58 baseline; worker-resolved token and serialized errors | Same runtime fingerprint | Prior 28 passed cover the previous implementation; new distributed credential-failure evidence is needed | Pending explicit rerun approval; not executed |
| Approved review rerun: clickhouse | Complete suite, 29 collected cases | Runtime fingerprint `a1835f330a8c92e6f90125867e45485b32c255472df1fb83d6bf0465193b8fc2` | User explicitly approved the planned final rerun; prior runtime evidence covers the previous implementation | failed; exit=1; counts={"total": 29, "passed": 27, "failed_or_error": 1, "skipped": 1}; `.artifacts/review-fixes/clickhouse-a1835f330a8c` |
| Approved review rerun: ray-cluster | Complete suite, 29 collected cases | Runtime fingerprint `3d47da43cbd029939d1155cdf358ff4fd9a537121bf3424a790b6ee03ee1c70b` | User explicitly approved the planned final rerun; prior runtime evidence covers the previous implementation | passed; exit=0; counts={"total": 29, "passed": 29, "failed_or_error": 0, "skipped": 0}; `.artifacts/review-fixes/ray-cluster-3d47da43cbd0` |

### Fixture correction and current evidence

- The failed ClickHouse run was caused by the test combining token_provider with the connector-managed username/password. The SDK factory rejects this combination; the prior claimed public route is withdrawn.
- The fixture now exercises a supported additional header through the real SDK/read transport in a Ray worker. Production/authentication behavior was not changed.
- The first authorized complete Ray-cluster run passed all 29 cases at runtime fingerprint `3d47da43cbd029939d1155cdf358ff4fd9a537121bf3424a790b6ee03ee1c70b`. The extra complete ClickHouse rerun is pending separate approval.

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| Approved review rerun: clickhouse | Complete suite, 29 collected cases | Runtime fingerprint `3d47da43cbd029939d1155cdf358ff4fd9a537121bf3424a790b6ee03ee1c70b` | User explicitly approved the planned final rerun; prior runtime evidence covers the previous implementation | passed; exit=0; counts={"total": 29, "passed": 28, "failed_or_error": 0, "skipped": 1}; `.artifacts/review-fixes/clickhouse-3d47da43cbd0` |

### Final review validation

- The separately approved complete ClickHouse rerun passed 28 cases with one expected cluster-only skip. The corrected Ray-cluster suite passed all 29 cases with no skips.
- Both successful runs use runtime fingerprint `3d47da43cbd029939d1155cdf358ff4fd9a537121bf3424a790b6ee03ee1c70b`. Exact project resources were cleaned; normalized dangling image identities are unchanged.
- Prior failed evidence is retained. The unsupported public token_provider route claim is withdrawn; production authentication behavior was not expanded.

## Typed read parameters

### Initial full suites

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| clickhouse final (ray-clickhouse-typed-read-parameters) | Complete existing and new integration cases | Content SHA256 d350485fa80424dddf006dc4601176d184805c6b501bab42aa64843d49873f20; Python 3.12 / Ray 2.58 / ClickHouse 26.8 | Authorized final run; distinct branch and infrastructure baseline | Passed (exit 0); evidence `.artifacts/implementation/clickhouse-final-d350485fa804` |
| ray-cluster final (ray-clickhouse-typed-read-parameters) | Complete existing and new integration cases | Content SHA256 d350485fa80424dddf006dc4601176d184805c6b501bab42aa64843d49873f20; Python 3.12 / Ray 2.58 / ClickHouse 26.8 | Authorized final run; distinct branch and infrastructure baseline | Passed (exit 0); evidence `.artifacts/implementation/ray-cluster-final-d350485fa804` |

### Review correction validation

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| Complete ClickHouse IT after comment-binding corrections | All 67 collected cases, including single/partition/auto comment bindings | Runtime fingerprint `d2dbf8cc4cad38c14219b40268847b812e46bc1824d9891edf367859cf472067`; detailed files in `.artifacts/review-fixes/summary.json` | Prior 63 passed plus one cluster-only skip cover the previous implementation; comment validation and IT cases changed | Pending explicit rerun approval; not executed |
| Approved review rerun: clickhouse | Complete suite, 67 collected cases | Runtime fingerprint `d2dbf8cc4cad38c14219b40268847b812e46bc1824d9891edf367859cf472067` | User explicitly approved the planned final rerun; prior runtime evidence covers the previous implementation | passed; exit=0; counts={"total": 67, "passed": 66, "failed_or_error": 0, "skipped": 1}; `.artifacts/review-fixes/clickhouse-d2dbf8cc4cad` |

### Review rerun completion

- The approved complete ClickHouse suite passed 66 cases with one expected cluster-only skip. The runtime fingerprint is unchanged, and exact project resources were cleaned. Normalized dangling image identities match the pre-run baseline.

### Master synchronization validation

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| Local static and unit/contract checks | Ruff format/lint, mypy, compatibility, documentation, release checks, and the complete unit/contract suite | Feature combined with master `e6d2f399587580d139a43654af254e981ea8c17e`; runtime fingerprint `8d02f0d4e81943aab650955a6690126ed5a5ee427ad121d75fe19fb365eca407` | User-approved resolution of the shared ledger conflict; production delta against master matches the original feature | Passed: 335 unit/contract cases, no skips; all static checks passed; evidence `.artifacts/conflict-resolution/master-e6d2f3995875-20261003T152113Z/validation.json` |
| CI ClickHouse and Ray multi-node integration | Combined feature and read-diagnostic cases | Same combined runtime state, pending commit and push | The prior separate-branch IT results remain historical evidence; CI will validate this new combination | Pending authorized commit/push; not executed for this combined state |

## Trusted query results

### Initial full suites

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| clickhouse diagnostic (ray-clickhouse-read-query-results) | Targeted new-implementation checkpoint: tests/integration/test_query_metadata.py | Content SHA256 2ab30278cf9bfda7a198f3eaafcc594ba37a7eb694ad442886421502a195a1a0; Python 3.12 / Ray 2.58 / ClickHouse 26.8 | Authorized diagnostic run; distinct branch and infrastructure baseline | Passed (exit 0); evidence `.artifacts/implementation/clickhouse-diagnostic-2ab30278cf9b` |
| clickhouse final (ray-clickhouse-read-query-results) | Complete existing and new integration cases | Content SHA256 c50f06a1544ee3cfa054663748f748333d4056d2fdedb2f52b84843e23a3f7fe; Python 3.12 / Ray 2.58 / ClickHouse 26.8 | Authorized final run; distinct branch and infrastructure baseline | Failed (exit 1); evidence `.artifacts/implementation/clickhouse-final-c50f06a1544e` |
| ray-cluster final (ray-clickhouse-read-query-results) | Complete existing and new integration cases | Content SHA256 ff001326207b123db31a1cb47d6a5f49c618bc3ad5c4ea4be1278a41d6aa0b5e; Python 3.12 / Ray 2.58 / ClickHouse 26.8 | Authorized final run; distinct branch and infrastructure baseline | Passed (exit 0); evidence `.artifacts/implementation/ray-cluster-final-ff001326207b` |
| clickhouse final (ray-clickhouse-read-query-results) | Complete existing and new integration cases | Content SHA256 ff001326207b123db31a1cb47d6a5f49c618bc3ad5c4ea4be1278a41d6aa0b5e; Python 3.12 / Ray 2.58 / ClickHouse 26.8 | User explicitly approved rerun after incorrect zero-scan assertion; test now verifies zero output and records scan work; previous failed full result cannot be reused | Passed (exit 0); evidence `.artifacts/implementation/clickhouse-final-ff001326207b` |

### Master synchronization validation

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| Local static and unit/contract checks | Ruff format/lint, mypy, compatibility, documentation, release checks, and the complete unit/contract suite | Feature combined with master `e6d2f399587580d139a43654af254e981ea8c17e`; runtime fingerprint `70e56dbfa7d9a1e516481feb63fd0a34c3938d72f22f76a836446b5821395024` | User-approved resolution of the shared ledger conflict; production delta against master matches the original feature | Passed: 316 unit/contract cases, no skips; all static checks passed; evidence `.artifacts/conflict-resolution/master-e6d2f3995875-20261003T152113Z/validation.json` |
| CI ClickHouse and Ray multi-node integration | Combined feature and read-diagnostic cases | Same combined runtime state, pending commit and push | The prior separate-branch IT results remain historical evidence; CI will validate this new combination | Pending authorized commit/push; not executed for this combined state |

### Typed parameter integration validation

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| Focused binding and query contract checks | tests/unit/test_query.py, tests/unit/test_typed_parameters.py, tests/contract/test_public_api.py; Ruff, mypy and documentation checks | Query feature combined with master `2525e9134a95811b85f1666707d242d6843f6eaa`; runtime fingerprint `59affb8efc22623319a973cd7fa84e6b2bf4d237427a44bf5995182e4100c352` | Query mode now calls the merged typed-parameter validator; run only the directly affected checks | Passed: 106 cases, no skips; all static checks passed; evidence `.artifacts/conflict-resolution/master-2525e9134a95-20261003T155426Z/validation.json` |
| Prior CI integration evidence | Read diagnostics plus typed parameters, and read diagnostics plus query reads | [Typed-parameter CI](https://github.com/jiangxt2/ray-clickhouse/actions/runs/37133808308) and [Query CI](https://github.com/jiangxt2/ray-clickhouse/actions/runs/37133854786) | Preserve successful separate-combination evidence; no local full suites or infrastructure tests rerun | Historical: typed combination ClickHouse 69 passed plus one cluster-only skip, Ray cluster 70 passed; query combination ClickHouse 44 passed plus one cluster-only skip, Ray cluster 45 passed. These runs do not claim complete three-feature integration coverage |

### Ray 2.59.0 compatibility validation

| Suite | Scope | Code/config state | Reason | Result |
| --- | --- | --- | --- | --- |
| Ray cluster integration | Full table/query/type/partition/range/read/write suite, including sync and async post-commit acknowledgement-loss faults | Uncommitted `ray-259-compatibility` on base `86eb325981f216ba554c0b2ad032cacf1fab47bc`; Ray 2.59.0, PyArrow 19, ClickHouse 26.8.1.2041; later edits only clarify compatibility documentation | Admit the new Ray minor while preserving zero replay and empty-write contracts; avoid a duplicate single-node run | Passed: 88 tests, 0 skipped, 12.02s. Fault cases prove one write attempt, one committed row, client closure, no confirmed receipt, and `AmbiguousWriteError`. Logs `/tmp/ray-259-validation-20261013/clickhouse-cluster/`; cleanup exit 0. Local uncommitted validation, not release-SHA evidence. |
| Approved additional Python baselines | Full unit/contract on Python 3.10, 3.11 and 3.13, including real local-Ray read/write contracts | Python 3.10.18, 3.11.15 and 3.13.7 / Ray 2.59.0 / PyArrow 19.0.1; source unchanged; state and environment facts in `followup/python-final-results.json` | Close the explicitly approved Python/Ray validation gaps without repeating database IT | Each baseline passed all 405 cases, zero skipped, in 17.09s, 13.73s and 13.66s. Initial attempts had 399 passes and six Ray-setup errors because the local Unix socket path exceeded 103 bytes; shorter isolated paths resolved this environment issue. Original and final logs remain under `/tmp/ray-259-validation-20261013/followup/`, with final logs `ray-clickhouse-py310-unit-contract-retry.log`, `ray-clickhouse-py311-unit-contract-retry.log` and `ray-clickhouse-py313-unit-contract-retry.log`. No ClickHouse IT rerun. |

## Master development version validation

User-approved development identity `1.1.dev0` on 2026-10-13; source-state SHA-256 excluding this ledger: `423f956212fdd4b677a75c2e1e6825480088faa1cd083d7e7d9ff5eaa5772302`. Primary runtime: Python 3.12.12 / Ray 2.59.0. Full unit suite: 410 passed, zero skipped; Ruff format/lint, mypy, workflow/version policies and strict documentation/spelling passed. Wheel/sdist build, Twine checks and isolated non-editable installs passed, with exact development metadata, public imports, dependency checks and production source-byte comparisons. Logs, artifact hashes and XML: `/tmp/ray-259-development-versions-20261013/`.

This follow-up changes version metadata, release/CI validation and direct regressions, with no additional data-path or test-infrastructure change. Retain the recorded functional contracts and real database/cluster IT; no such IT was rerun. It does not certify an exact published release SHA or a remote Actions run. No commit, push or publication performed.

## Filtered Distributed local reads

Scope: Issue #19, `distributed-local-read`, base `8713697`. Primary runtime:
Python 3.12.12, Ray 2.59.0, PyArrow 19.0.1, clickhouse-connect 1.5.0, ARM64.
The user authorized development, real infrastructure tests and multiple cached
ClickHouse images on 2026-10-20. Tests use two shards with two replicas each,
Keeper and three Ray nodes, with per-run generated HTTPS certificates.

| Suite | Scope and result | Evidence |
| --- | --- | --- |
| Unit/contract without Ray runtime | 475 passed; 6 Ray-marked cases deliberately assigned to their separate runtime suite | `.artifacts/unit-final.xml` |
| Public Ray runtime contracts | 6 passed; exact existing read/write/empty-Dataset contracts; first sandbox attempt could not enumerate processes, then only these six cases ran with host access | `.artifacts/ray-contract.xml` |
| ClickHouse 26.8.1.2041 protocol | 85 passed; 3 cluster cases selected by the disjoint cluster suite, no skipped tests | `.artifacts/it/single-26.8/pytest.xml` |
| ClickHouse 26.8.1.2041 distributed/cluster | 35 passed (32 new distributed cases and 3 existing cluster cases); 85 protocol cases deselected to avoid duplicate evidence | `.artifacts/it/distributed-26.8-complete/pytest.xml` |
| ClickHouse 25.12.2.54 distributed | 32 passed; same local-read contracts without repeating protocol regression | `.artifacts/it/distributed-25.12-final/pytest.xml` |
| ClickHouse 25.8.14.17 distributed | 33 passed; the 32 shared cases plus the new structured-privilege capability smoke | `.artifacts/it/distributed-25.8-final/pytest.xml` |
| ClickHouse 24.8.14.39 capability fallback | 1 passed; real CHECK GRANT syntax absence, complete logical-source row multiset and exactly one logical task | `.artifacts/it/distributed-24.8-capability/pytest.xml` |
| Additional 26.8 edge cases | 2 passed: actual missing selected target and the new positive privilege-capability smoke; no full-suite repeat | `.artifacts/it/distributed-26.8-final-edge/pytest.xml` |

The shared distributed cases verify all four split modes, task budgets of one
and four, one fixed replica per shard, complete row multisets with duplicates
and NULLs, empty shards, source projection order, cache regrouping and task row
limits. Query-log assertions check initial local data queries on selected hosts
and their absence on unselected replicas. Other cases cover real row-policy
fallback, denied policy visibility, schema drift, partial-output transport
failure, early generator closure, HTTPS on workers, hostname mismatch, missing
worker password environment, metadata-origin fallthrough and optional split or
weight capability loss. Capability differences are injected per node while
queries, declarations and Arrow probes still use the real servers. This does
not claim arbitrary mixed-version cluster or Cloud compatibility.

### Capability differences and bounded measurements

No server version string participates in planning. Real 24.8 rejects the fixed
CHECK GRANT statement with code 62 and has no isGranted SQL function. The
connector recognizes that exact missing capability and falls back as a whole;
permission denials, malformed metadata and transport failures remain errors.
25.8, 25.12 and 26.8 execute the local path. Effective parallel-replica controls
are selected by available fields, including the legacy control when the newer
field is absent; max_parallel_replicas alone is not an enablement proof.

The bounded 22-row comparison is recorded as JUnit properties, including actual
server version, planning and scan durations. On 26.8 it measured approximately
23ms logical planning / 11ms reading and 115ms local planning / 16ms reading.
This small fixture proves path/cost observability, not a throughput advantage.
There is no snapshot or count-equality correctness claim.

### Diagnostic attempts and evidence reuse

Diagnostic failures are not counted as passed coverage. Separate artifacts
retain the initial invalid Compose name, CHECK GRANT command-result handling,
empty Native result headers, a test-script edit during execution, default
settings representation, query-log suffix matching, test collection indentation,
and the duplicate Ray fixture initialization. The last of those ran 30 new
cases successfully but failed the first original cluster setup. The final
35-case cluster run includes the fixture fix and relevant metadata/unknown-stat
changes. No successful full suite was repeated on unchanged applicable state.

The subsequent production change only recognizes code 62 for connector-owned
CHECK GRANT syntax; it leaves every successful probe path unchanged. Preserve
the 26.8/25.12 positive-path results and separately prove the new missing-capability
branch on 24.8, its positive branch on 25.8, and the new edge cases on 26.8.
Later CI wiring, additional tests and documentation do not trigger protocol
repeats. Source-state fingerprints, runtime image identity, pytest and Compose
logs and Docker before/after snapshots are stored beside each suite's XML.
All test-created project resources and runtime images are cleaned by their
owning script; cached base images and unrelated infrastructure are retained.
Static checks, package build/Twine and strict docs/spelling pass locally; no
commit, push, PR, Issue update or remote CI execution is claimed.

Final evidence index: `.artifacts/it/distributed-local-read-results.json` includes
all successful XML reports and the final production-source digest. The final
wheel contains byte-identical copies of all 15 Python production modules;
wheel/sdist Twine checks pass. Final resource inspection found no test project
containers or temporary runtime image tags, and no newly dangling image IDs.
Canonical `master` remains clean. The local worktree is ready for user review;
no Git commit or external mutation has been performed.

## Structured metadata compatibility investigation

User authorization: pull a fixed ClickHouse 23.3 server image and verify the
compatibility approach, prioritizing structured system tables over SQL parsing.
Scope is isolated research probes and evidence; no connector implementation
change is included in this investigation.

| Suite | Environment and code state | Purpose and non-overlap | Status |
| --- | --- | --- | --- |
| Metadata/security capability probes | Fixed 23.3.22.3 ARM64 image; two fresh servers with select_from_system_db_requires_grant true/false; probe source under `.artifacts/metadata-compat/` | Verify actual metadata, grants via active/default roles, column hiding, SQL/XML policy inventory, policy metadata filtering and query permission enforcement; no repeated connector or Ray suites | Planned |
| Same structured-contract probes | Cached 24.8.14.39 ARM64 image; true/false settings in separate fresh servers | Verify the candidate contract is shared across old baselines; previous 24.8 test only proved CHECK GRANT fallback | Planned |

Before running, record probe/config hashes, image identity, container and dangling
image baselines. Retain per-case JSON and server logs. Cleanup targets only the
containers and volumes created by the probe runner. Downloaded base images are
retained. No global Docker cleanup or external Git operation is authorized.

First structured-probe run: 23.3 executed 86 checks, 84 passed; two XML checks
incorrectly assumed the diagnostic storage label was `users_xml`, while the
real server returned `users.xml`. All authorization, filtering, inventory and
four-split prototype checks passed in both configuration modes. 24.8 passed all
86 original checks. The XML assertion is being replaced by the stronger
structured applicability/scope and actual filtered-row contract; storage labels
remain diagnostics. Run only the new focused XML checks on both baselines to
verify this change; retain the already-passed full-probe evidence. Initial 23.3
image inspection failed before container creation after the pulled digest was
no longer in the local inventory; recovery pulled the same fixed version,
verified its ARM64 identity and retained explicit version tags.

### Verified compatibility findings

Evidence index: `.artifacts/metadata-compat/findings.json`. Actual servers report
23.3.22.3 and 24.8.14.39. Each baseline ran both explicit true/false system-table
grant configurations. Both reject CHECK GRANT with code 62. The probes proved
native role inheritance and disabled-role/revocation enforcement, table-level
DESCRIBE denial versus partially visible system.columns rows, and complete
SQL/XML policy representation through the same structured system table.

A successful system.row_policies SELECT can be filtered to zero rows even with
SHOW ROW POLICIES permission. Plain SHOW ROW POLICIES shares that filtering.
In both baseline/configuration pairs, direct SHOW CREATE ROW POLICIES ON *.*
still returned the complete policy inventory while the protected SELECT
returned zero. Direct SHOW CREATE enumerates AccessControl independently. Its
row count can be compared with the unfiltered structured policy inventory and
unique UUID count without parsing returned CREATE SQL. Under static policy/grant
configuration and verified neutral result settings, both enumerate the same
underlying set and row-policy filtering can only remove structured rows.
Counts here establish metadata-list completeness; they do not establish equality
between Distributed and local data. Permission failures or inventory disagreement
remain errors. Result-affecting settings, especially additional_result_filter
and result truncation, must be checked before trusting metadata completeness.

For the four-split feasibility probe, only this research process substituted
CHECK GRANT with real DESCRIBE/SHOW CREATE metadata operations and the independent
policy inventory check. The existing datasource then completed single,
partition, range and auto local reads on both baselines and both configuration
modes, preserving the duplicate row. These were one-shard, one-server probes;
they do not claim formal implementation or full multi-shard/Ray compatibility.
No production connector source changed. Existing engine-target SQL parsing is
still used; no new policy/grant SQL parser was introduced.

The two old XML diagnostic assertions were superseded by focused probes proving
the configured user's actual filtered result, structured apply_to_list/scope,
and independent inventory completeness. Each baseline passed eight focused
assertions. No successful full-probe suite was repeated. Preserve the original
23.3 checks and their incorrect-label failures as diagnostic evidence.

## Formal native metadata conversion

Authorized 2026-10-20: replace CHECK GRANT with native operations, update the
design, prove old baselines through real multi-shard/replica and Ray execution,
and regress existing baselines. Production changes enforce whole-table DESCRIBE,
cross-check system.columns, validate effective settings before policy inventory,
and compare structured UUID inventory against direct native enumeration. No
policy/grant SQL parser or new public option is introduced. The static metadata,
ACL and policy assumption applies throughout planning.

| Suite | State / scope | Reason and overlap | Result |
| --- | --- | --- | --- |
| Unit/contract and quality | Final local Python 3.12; production/type/docs/package checks | Includes new native operation, completeness and ordering boundaries; no container overlap | 490 non-Ray cases plus six separate local-Ray contracts passed; quality checks passed |
| Protocol | 26.8.1.2041; 85 non-cluster integration cases | Final primary invocation selected integration, so these ran once in the combined primary report; no separate protocol rerun | All 85 passed within the 135-case primary report |
| Primary distributed/Ray | 26.8.1.2041; 47 distributed plus three legacy cluster and 85 protocol cases | Two shards, two replicas each, Keeper, HTTPS, three Ray nodes; both system-table grant modes on different nodes | 135 passed, zero skipped, 83.51s |
| Legacy distributed/Ray | 23.3.22.3 and 24.8.14.39 | Reuse unaffected successes and add only changed/new context, cache and security cases; case-level mapping records all final coverage | Each covers all 47 final distributed cases, zero unresolved failures or skipped coverage |
| Existing baseline regression | 25.8.14.17 and 25.12.2.54; same distributed suite | Each final suite once after relevant fixes; no repeated protocol suites | Each 47 passed, zero skipped |

Each runner records exact source/test/config fingerprint, image IDs, XML, pytest
and Compose logs and Docker before/after snapshots. Container runs are serial
because the existing Ray fixture uses a fixed subnet. Failed tests retain their
reports; only root-cause fixes justify affected reruns. Test-owned runtime images
and project resources are removed by trap; cached base images are retained.

23.3 attempt `native-23.3-v1` stopped after fixture errors: a YAML anchor override
replaced the source node's volume list, losing the common Keeper/topology config.
Server logs show NO_ZOOKEEPER (225). No conversion success is claimed. Correct
the fixture by retaining common config/users mounts, add a structural unit
regression, and rerun the failed 23.3 suite with fail-fast diagnostics. This is
an infrastructure change and failure recovery, not a repeat of a passed suite.
The original source fingerprint and Compose/pytest logs are retained.

23.3 `native-23.3-v2`: 17 passed, then the cluster-name capability injection
assertion failed. Real 23.3 exposes cluster without the newer name alias; after
removing cluster, whole-source fallback was correct. Update the test to assert
the actual field-dependent contract and full filtered multiset. Production is
unchanged. Retain the 17 passes and run only the failed and unexecuted cases;
do not repeat the successful split/origin cases. The first unit-suite invocation
used the pytest entrypoint and failed collection of tools imports; corrected
python -m pytest passed 484 non-Ray cases, with six Ray cases reserved separately.

23.3 `native-23.3-v3`: 24 additional cases passed, including both Ray workers,
HTTPS, both system grant modes, inherited/disabled/revoked roles, filtered policy
inventory, partial column visibility and XML policy behavior. One fixture tried
GRANT ALL, which the old default admin cannot delegate in full. Replace it with
the exact SELECT / SHOW COLUMNS / SHOW ROW POLICIES privileges needed by that
test. Production remains unchanged. Only that failed test and the unexecuted
target-missing test remain; preserve the 41 distinct successful cases.

23.3 `native-23.3-v4`: both remaining cases passed, zero skipped. The combined
v2/v3/v4 reports cover all 43 distinct distributed tests with no remaining
failure. Test-only fixture corrections did not change production code or the
previously passed cases. Both true/false system-table grant modes and the
three-node Ray / two-shard / two-replica topology are now formally verified.
Proceed with one final 43-case run on each remaining baseline; the 26.8 primary
also selects the three disjoint legacy cluster tests. No protocol suite repeats
on additional versions.

24.8 `native-24.8`: all 43 distributed tests passed, zero skipped (13.06s),
including local conversion, both grant modes and real worker/shard placement.
The six separate public local-Ray contracts also passed (7.66s).

Static self-review found a pre-existing initial-implementation cache edge: target
resolution was stored before strict split preparation finished. A later planning
attempt could retain it after a NotApplicable result. Publish targets and child
datasources together only after successful preparation. Add a unit case and
extend the strict optional-split IT to prove failed state is empty, repeat errors
stay errors, and a new ineligible context selects the logical source. This
production change affects failure-cache lifecycle, not native metadata success
paths. Run only the changed IT case once on 23.3 and 24.8; retain all unchanged
43-case metadata evidence. Remaining baseline suites run the final cache fix.

The changed failure-cache IT passed once on each old baseline (`native-23.3-cache`
and `native-24.8-cache`), zero skipped. Final review also extends policy scope to
the discovery system tables themselves: complete policy inventory must not permit
filtered clusters/parts to silently omit work. Add real filtered-cluster and
filtered-parts cases, and require throwing read/timeout overflow modes before
inventory checks. Strengthen the profile-setting test with direct context failure
so cluster identity mismatch cannot hide a missed setting check. Each old
baseline receives only these changed/new security cases plus one native local
positive smoke. The remaining three baselines run all 47 final distributed
cases; the primary also runs the three existing cluster cases. This is affected
production/context coverage, not a repeated unchanged full suite.

Both final old-baseline security selections passed six cases each, zero skipped.
Their final coverage index (`.artifacts/it/native-execution-ledger.json`) maps
all 47 final distributed cases to passed reports, reusing unchanged cases and
replacing affected assertions with final targeted evidence.

25.8 final attempt stopped at the first smoke: a new unrelated system.tables
field parameterized_view_parameters is formatted as compact Array(Tuple(...))
in system.columns and multiline text in DESCRIBE. An isolated fixed-image
metadata probe proved identical 39 field names and all other types. Respect the
approved extra-field contract: compare complete field-name sets; validate type
and native declaration agreement for consumed fields only. Do not add a type SQL
normalizer for unused fields. Unit tests prove both unused multiline tolerance
and consumed declaration mismatch rejection. Preserve the failed report; rerun
the failed 25.8 suite on the corrected metadata state. Old baselines require no
repeat: they already satisfied the stronger all-field literal equality; all
consumed field checks and native operations remain equivalent there.

### Final results and review

`.artifacts/it/native-execution-ledger.json` records each completed suite and
preserves the failed 25.8 attempt. `.artifacts/it/native-local-read-results.json`
indexes reports, final production fingerprint and package/resource evidence.
The primary CLI had both a target-side -m cluster and the runner's final default
-m integration; pytest selected the latter. All 47 distributed, three cluster
and 85 protocol cases passed in one 135-case report. Do not run the standalone
protocol suite again merely for a different wrapper: its cases are already
fully covered. CI uses the explicit marker environment and continues to select
the intended disjoint jobs.

Final source changes after the old-baseline positive tests only stop comparing
unused complex field type text and retain strict complete-name/consumed-type
checks. Old nodes had already satisfied the stronger literal comparison; the
native operations, required types and outcomes are unchanged there. New unit
cases and final 25.8/25.12/26.8 suites cover this precise compatibility fix.

Static self-review covered the cumulative diff, credential-safe exceptions,
source and physical schema, native role enforcement, full policy inventory,
discovery-table filtering, effective partial-result controls, fixed endpoints,
atomic failed-plan state, task budgets, streaming cancellation, TLS and resource
ownership. No remaining blocking finding. All assertions run against real
ClickHouse/Ray; no permanent skip, grant parser or policy-expression parser was
introduced. CI matrix covers fixed 23.3/24.8/26.8 baselines but has not run remotely.
No commit, push, PR or Issue mutation was performed.

## Review correction validation

User approved fixing the three reviewed findings. Scope: effective aggregate/sort
overflow safety, display-independent business type comparison using the existing
declaration scanner, and native target security/visibility before engine catalog
interpretation. Additions do not change the public API or parse versions, grants
or policy SQL. Existing default read/write functions are unchanged.

| Suite | Coverage / state | Reason and non-overlap | Result |
| --- | --- | --- | --- |
| Targeted units, then final unit/contract | Type token equivalence and strict differences, effective overflow modes, native visibility, ordering | Fast development diagnostics; no container cases | 105 targeted and 515 final non-Ray unit/contract cases passed; six unchanged local-Ray contracts retain prior evidence |
| Primary final ClickHouse/Ray integration | 26.8.1.2041; all distributed and integration cases, including new review regressions | One final combined run covers protocol and cluster; no separate duplicate protocol run | 147 passed, zero skipped, 98.25s (59 distributed + three legacy cluster + 85 protocol) |
| Affected compatibility cases | 23.3.22.3, 24.8.14.39, 25.8.14.17, 25.12.2.54 | New review regression file plus existing actual-missing-target test, once per version; retain unrelated previous full-suite evidence | Each passed all 13 cases, zero failures/errors/skips |

New infrastructure assertions use SQL settings profiles assigned to the same
default identity on either the entrance or selected target node. This avoids an
identity mismatch producing a false-positive fallback. Partial-key behavior is
exercised with separate input blocks. Named Tuple positives run on real Ray
workers with full and narrow projections, while different fields/types must
still fail. Metadata row policy and denied visibility are tested separately
from real missing targets. Record final source/test/config fingerprints and logs
under `.artifacts/it/review-fix-*`; failures permit only root-cause recovery.

First final compatibility selection: ClickHouse 25.8.14.17 passed all 13 cases
(12 new review regressions plus actual missing target), zero skipped, 6.56s.
The profile tests actually observed partial aggregate keys from separate blocks,
then verified complete logical-source fallback on entrance and target nodes.
Both named Tuple projections ran on Ray workers; semantic type/name differences,
target catalog row filtering, native denied visibility and true absence were
distinguished. Reuse this passed suite; do not repeat it on unchanged code.

All remaining selections passed on their first run. Source fingerprints, exact
image IDs and full pytest/Compose reports are indexed by
`.artifacts/it/review-fix-execution.json`. No successful unchanged infrastructure
suite was repeated. The primary explicitly selects the integration marker and
includes the complete protocol suite, so no separate wrapper run is needed.

Static self-review confirms that type keys change only comparison: quoted
literals, tuple identifiers, widths, precision, timezone and enum values remain
significant. Existing declarations and data SQL are retained. All three column
comparison sites use the same key. Required system field declarations remain
strict. Effective overflow checks precede sorted business-column metadata;
target context and native DESCRIBE precede engine lookup. An empty catalog with
a successful native description now raises DiscoveryError, rather than claiming
absence. Denied visibility and actual missing targets retain native semantic
errors. The all-or-nothing plan cache and no-runtime-fallback boundary remain
unchanged and pass the complete primary suite. No remaining review finding.

Ruff, strict mypy, docs/compatibility/release checks, strict Sphinx and spelling
passed. Wheel/sdist and byte-level module verification are recorded in the fix
evidence index. No commit, push, PR or Issue mutation was performed.

## Numeric comment correction

Approved scope: distinguish native DESCRIBE comments from explicitly selected
system.columns positions, preserve schema drift detection, and document planning
overhead, static configuration and independent query snapshots in the public
read docstring. PR #20 at 2cff914 passed its remote CI and Greptile review; those
results do not substitute for verification of this subsequent parser correction.

| Suite | Coverage / state | Reason and overlap | Result |
| --- | --- | --- | --- |
| Unit/contract and static checks | Final parser/source integration, resolver numeric comments, write positions and true drift | Full fast local suite; no database cases | 532 non-Ray cases passed; six unchanged local-Ray contracts retain prior evidence; Ruff, strict mypy, docs/compatibility/release checks, strict Sphinx and spelling passed |
| New numeric-comment IT | ClickHouse 26.8.1.2041, 23.3.22.3 and 24.8.14.39; two cases per baseline | Single/multiple columns, duplicates, native descriptions, logical/local Ray reads, query reads and append writes; run only this new file once per baseline | All three baselines passed both cases on their first run, zero failures/errors/skips |

The shared parser changed, so the new real-server cases exercise both DESCRIBE
callers and the system.columns write caller. Use cached fixed images, the existing
two-shard/two-replica/three-Ray-node runner, unique Compose projects and its cleanup
trap. Save source fingerprints, XML and logs under .artifacts/it/numeric-comments-*.
Retain the earlier unrelated protocol, security, type and lifecycle evidence;
do not repeat successful unchanged infrastructure suites.

Final execution index: .artifacts/it/numeric-comments-execution.json. Each baseline
ran the identical source/test/config state. The single-column regression checks
the actual DESCRIBE comment '2026', verifies position 1, and completes conversion;
the multi-column case also checks comment '0' and output order. Confirmed append
writes and query reads exercise the other callers of the shared parser. Unit
coverage retains errors for real source-position changes and invalid explicit
write positions. No resolver check was removed or changed into fallback.

The public docstring links to the existing architecture section and states
per-node planning overhead, static topology/security and no shared snapshot.
The PR-body draft records the completed remote matrix at 2cff914 and separately
identifies this correction's local evidence. Local validation completed before
the correction's commit and PR publication; remote outcomes are tracked by PR checks.

## Distributed simple predicate conversion

Approved on 2026-10-21 for branch `distributed-simple-predicates`, based on
`69346ebd35d2eafeca5fa50cea67cbe38428b171`. Extend only the local-conversion
recognizer for constant-bound BETWEEN, fixed-pattern LIKE and scalar-list IN,
including NOT and Boolean composition. SQL and bindings remain unchanged.
Subqueries and GLOBAL IN remain ineligible; the public SELECT-clause rejection
is retained. No unrelated pushdown worktree is included.

The following table records the initial implementation, before review corrections.

| Suite | Coverage / code state | Reason and overlap | Result |
| --- | --- | --- | --- |
| Focused parser unit tests | New grammar and rejection cases plus existing distributed units; Python 3.12 / Ray 2.59 | Development feedback before infrastructure; no database overlap | 147 passed |
| Final static and unit/contract | Complete unit/contract suite, Ruff, strict mypy and documentation checks | Final short baseline; metadata/logs under `.artifacts/simple-predicates/short-final` | 603 passed, zero skipped; Ruff, strict mypy, documentation, compatibility and release checks passed |
| Primary combined IT | ClickHouse 26.8.1.2041 / Ray 2.59.0 / Python 3.12.14; complete distributed and integration suites in two-shard/two-replica/three-node topology | One final combined run covers protocol and cluster behavior without separate overlapping runners; fingerprints/logs under `.artifacts/simple-predicates/primary` | 168 passed, zero failures/errors/skips; 175.62 seconds; owned Compose resources and runtime image cleaned |
| Older baseline 23.3 | ClickHouse 23.3.22.3 / Ray 2.59; new simple-predicate file only | One minimal compatibility run; no repeat of the primary protocol suite; logs under `.artifacts/simple-predicates/ch23.3` | 19 passed, zero failures/errors/skips; 22.74 seconds; owned Compose resources and runtime image cleaned |
| Older baseline 24.8 | ClickHouse 24.8.14.39 / Ray 2.59; new simple-predicate file only | One minimal compatibility run; no repeat of the primary protocol suite; logs under `.artifacts/simple-predicates/ch24.8` | 19 passed, zero failures/errors/skips; 23.91 seconds; owned Compose resources and runtime image cleaned |

Infrastructure starts only after implementation and static checks stabilize.
Record source/test/config fingerprints and actual runtime versions before each
run. Compare complete row multisets with logical reads, including duplicates,
NULLs, empty results, native casts, escaping and both binding protocols. Query
logs prove the selected local targets, retained operators and Ray-worker origins.
Use fixed mirror images, unique Compose projects, existing cleanup traps and
proxy-free Docker operations. Passed long suites are not repeated on unchanged
code/configuration/environment. Failed runs retain logs and receive only the
necessary diagnostic rerun before a final complete suite.

All three initial infrastructure runs passed on their first attempt. Their
runtime source/test/config fingerprint was identical for that implementation:
`1284765cd19a8b9748ed59761ab8b1a4c10b358ea6c3d6207bcf8722e921aa3e`.
The fingerprint excludes Markdown evidence updates. Execution records, actual
image identities, version records, XML, runner/pytest/Compose logs and cleanup
evidence remain in the per-baseline artifact directories. The execution index is
`.artifacts/simple-predicates/summary.json`. Normalized dangling-image inventories
are unchanged for all three runs; only the owned runtime tags/images were removed.

Reproducible commands from the repository root:

```bash
.venv/bin/python -m pytest tests/unit tests/contract
RAY_CLICKHOUSE_CLUSTER_PYTEST_MARKER=integration ./scripts/run_distributed_it.sh tests/distributed tests/integration
RAY_CLICKHOUSE_IT_IMAGE=docker.m.daocloud.io/clickhouse/clickhouse-server:23.3.22.3 RAY_CLICKHOUSE_IT_BASELINE=23.3.22.3 ./scripts/run_distributed_it.sh tests/distributed/test_simple_predicates.py
RAY_CLICKHOUSE_IT_IMAGE=docker.m.daocloud.io/clickhouse/clickhouse-server:24.8.14.39 RAY_CLICKHOUSE_IT_BASELINE=24.8.14.39 ./scripts/run_distributed_it.sh tests/distributed/test_simple_predicates.py
```

Ruff format/lint, strict mypy, documentation/compatibility/release checks, strict
Sphinx HTML and spelling passed. Static self-review covered the full cumulative
change, operand categories, complete Boolean recognition, BETWEEN AND boundaries,
NOT/NULL behavior, quoting and parameter preservation, unsupported forms,
unchanged topology/security/error handling, meaningful query-log/multiset
assertions and test resource cleanup. A subsequent full review identified two
missed defects: percent literals conflicted with internal client bindings, and
the global ALIAS guard masked negative recognizer tests. The above results are
retained as initial evidence, not as verification of the corrections below.

## Distributed simple predicate review corrections

The explicit repair request authorizes internal split binding selection in
`_sql.py`, real-driver regressions in `test_sql.py`, direct parser rejection
tests and clean-schema resolver controls, and expanded percent-literal IT.
No dependencies, public signatures or topology/security contracts change.

| Suite | Coverage / state | Reason and overlap | Result |
| --- | --- | --- | --- |
| Red regression proof | Real SQL generator and clickhouse-connect binder; internal partition/range constraints plus percent literals | New cases prove the reported failure before the fix; not counted as passing coverage | 36 regressions failed, 10 controls passed; `.artifacts/simple-predicates/review-fix/percent-red.log` |
| Corrected short suite | Complete unit/contract tests, parser-negative mutation sensitivity, Ruff, strict mypy and docs | Binding helper and tests changed; new complete short baseline | 648 passed, zero skipped; all 37 accept-all mutations detected; Ruff, strict mypy, docs/compatibility/release checks, strict Sphinx and spelling passed |
| Corrected primary combined IT | ClickHouse 26.8.1.2041 / Ray 2.59, complete distributed and integration suites | Previous 168 passing cases omitted literal-percent/split combinations; one run for changed binding code | 170 passed, 7 failed, zero skipped in 197.39 seconds: three SQL-text log assertions, four transport failures during metadata/client initialization; evidence under `review-fix/primary`, not a passing suite |
| Corrected older baseline 23.3 | ClickHouse 23.3.22.3 / Ray 2.59, updated simple-predicate file only | Prior 19 cases did not cover the correction; first updated compatibility run, under `review-fix/ch23.3` | 28 passed, zero failures/errors/skips in 39.02 seconds; structured query-log function assertions verified; cleanup completed |
| Corrected older baseline 24.8 | ClickHouse 24.8.14.39 / Ray 2.59, updated simple-predicate file only | Prior 19 cases did not cover the correction; first updated compatibility run, under `review-fix/ch24.8` | 23 passed, 1 failed, 4 fixture errors, zero skipped in 54.34 seconds; fixture INSERTs explicitly failed with code 241: about 952 MiB required against a 768 MiB server limit; not a passing suite; cleanup completed |

New evidence belongs under `.artifacts/simple-predicates/review-fix/`. Freeze
runtime code/tests/config before infrastructure; record fingerprints, actual
versions, commands, XML, logs and exact lifecycle evidence. Reuse dependency and
Docker caches, unique owned Compose projects and existing cleanup traps. No
duplicate protocol runner or extra Ray baseline is planned. Complete the final
cumulative static audit only after the correction evidence is available.

The cumulative static audit found both original findings corrected, with no
additional production-code defects in scope. SQL-text assertions were replaced
with structured `used_functions` checks because native parameter substitution
formats the AST before query logging; complete row-multiset and pinned-origin
assertions remain. This form passed all 28 cases on 23.3. Packaging/Twine checks
passed and archive production/test files match the current checkout.

At that intermediate audit, the aggregate result was not PR-ready. The 26.8
failures were all in the new test file; its 149 unchanged distributed/protocol
cases passed and remain reusable
because only new-test assertions/diagnostics subsequently changed. The 24.8
fixture failures explicitly show server code 241 (`MEMORY_LIMIT_EXCEEDED`), about
952 MiB required against the 768 MiB configured server limit. This does not prove
the cause of the earlier 26.8 metadata/client failures from that run.
The execution index is `.artifacts/simple-predicates/review-fix/summary.json`.

The user explicitly approved the test-profile adjustment and two additional
feature runs on 2026-10-21. Only the four distributed-test ClickHouse container
ceilings increase to 2 GiB, with server limits of 1.5 GiB; Keeper and host-wide
Docker configuration are unchanged. The failed runs above remain historical
evidence and are not counted as passing suites.

| Suite | Coverage / code state | Reason and overlap | Result |
| --- | --- | --- | --- |
| Final primary feature IT | ClickHouse 26.8.1.2041, updated 28-case file, 2 GiB container / 1.5 GiB server profile | One approved rerun after test assertions and memory configuration changed; reuse all 149 unchanged passing primary cases | 28 passed, zero failures/errors/skips in 89.95 seconds; `.artifacts/simple-predicates/review-fix/primary-feature-final` |
| Final older feature IT | ClickHouse 24.8.14.39, identical 28-case file and memory profile | One approved rerun after explicit code 241 fixture failures; reuse the passing 23.3 suite | 28 passed, zero failures/errors/skips in 47.07 seconds; `.artifacts/simple-predicates/review-fix/ch24.8-final` |

Retain the corrected 648 passing unit/contract cases, static checks and 37/37
mutation detections because their production/unit inputs are unchanged. Record
the new runtime fingerprint, effective container/server limits, versions, XML,
logs and cleanup for each run. No further infrastructure run is authorized.

Both approved reruns used runtime fingerprint
`247e410226146f836a3998a249987af5f38677ad90fc43f76564170be39c3df6`.
Docker inspection and each server's preprocessed configuration confirm
2147483648-byte container ceilings and 1610612736-byte server limits on all
four ClickHouse nodes for both versions. Keeper remains at 536870912 bytes.
Normalized dangling-image sets are unchanged, and label-specific checks confirm
no owned containers, networks, volumes or runtime tags remain after either run.

The final cumulative static review covers the recognizer and Boolean boundaries,
internal/explicit binding protocols, unsupported forms and fail-closed errors,
negative-test sensitivity, differential results and actual local Ray origins,
bounded test resources, documentation and CI collection. Both original findings
are fixed; no remaining actionable finding was identified. Source and short-test
files match the already verified package contents byte-for-byte. All requested
requirements now have implementation and passing evidence in the plan map.

This change meets local PR submission standards. The primary evidence combines
149 unchanged passing cases with the final 28-case run; it is not represented as
a new full-suite execution. The earlier 26.8 error causes are not inferred from
the later passing run. Existing dependency deprecation warnings remain visible.
The code is uncommitted; remote CI, commit, push and PR publication have not run.
Final evidence and the static audit are indexed by
`.artifacts/simple-predicates/review-fix/summary.json` and `final-review.md`.
