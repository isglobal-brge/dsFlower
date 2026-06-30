# dsFlower

`dsFlower` is the **node-side** DataSHIELD package that lets [Flower](https://flower.ai/)
federated learning run inside Opal/Rock servers under **differential privacy that the data
node alone decides and enforces**. The data-owning institution installs it; researchers use
the companion client,
[`dsFlowerClient`](https://github.com/isglobal-brge/dsFlowerClient), from their own R session.

The package is not an unrestricted remote Python executor. The trust model is simple: **the
data node is the only trusted party; the researcher (and everything they submit) is
untrusted.** Every release is differentially private, and the node — never the client —
chooses the privacy level and the mechanism.

- The client submits models as **declarative specs (data, not code)**; the node builds them
  from a vetted allow-list with stock layers.
- Differential privacy is **always on**: epsilon/delta/clipping come from the node's own
  DataSHIELD options (with hard ceilings), and an RDP/PRV budget ledger is debited
  **before any result is released**.
- The DP **mechanism is chosen server-side by submission type** (unforgeable routing).
- Arbitrary uploaded code runs **out-of-process**, with the node applying all DP itself.
- Raw rows, images, masks and staging files never leave the server. Model weights/updates
  travel Flower's gRPC/TLS plane; DataSHIELD carries handles, prep, status and approved
  metrics.

## Package roles

| Package | Installed where | Responsibility |
|---|---|---|
| `dsFlower` | Each Opal/Rock node | Validate requests, stage data locally, build models from specs, **decide + enforce DP**, run Flower SuperNodes, expose controlled status/metrics. |
| `dsFlowerClient` | Researcher workstation | Build the request, start the SuperLink, call authorised DataSHIELD methods, launch runs. |

## Installation

```r
remotes::install_github("isglobal-brge/dsFlower")        # each Opal/Rock node
```

The `configure` script prepares the node Python runtime and uses
[uv](https://docs.astral.sh/uv/) to provision per-template environments on demand (Flower,
PyTorch, XGBoost, Opacus).

## Server-authoritative differential privacy

There are **no privacy profiles and no client privacy knob**. The node sets the policy from
its own options and re-asserts hard ceilings as a fail-closed backstop:

| Option | Default | Ceiling |
|---|---|---|
| `dsflower.dp_epsilon` | 3.0 | `dsflower.dp_epsilon_ceiling` (10) |
| `dsflower.dp_delta` | 1e-5 | `dsflower.dp_delta_ceiling` (1e-3) |
| `dsflower.dp_clipping_norm` | 1.0 | `dsflower.dp_clip_ceiling` (100) |

Noise uses the **analytic Gaussian mechanism** (exact for all epsilon). Multi-round runs
compose `(epsilon/R, delta/R)` per round, and the budget ledger (keyed by data
fingerprint + target) is **reserved at prepare**, so a run that releases but never cleans up
still spent its budget. The client can only *query* the remaining budget
(`flowerPrivacyBudgetDS`).

## DP mechanism routing (chosen by the node, by construction)

| Submission | Mechanism |
|---|---|
| Declarative **neural** spec | Opacus **DP-SGD** (per-sample clip + noise) |
| **XGBoost** spec | **DP-GBDT** |
| Arbitrary uploaded **code** (Tier-2) | Out-of-process **output-perturbation floor** (sensitivity 2C), optionally sample-and-aggregate (2C/k) where a verified sandbox allows |

The declarative typed-graph model language covers the full per-sample DP-SGD space — MLP,
CNN (1/2/3D), TCN, ResNet, DenseNet, Inception, Transformer (attention from primitives),
squeeze-excitation, U-Net, LSTM/GRU — with **no researcher code on the node**. Plus XGBoost
(DP-GBDT). The Tier-2 egress path is for arbitrary code that cannot be expressed
declaratively; it runs the upload in an isolated interpreter while the trusted parent applies
all DP, so the upload can never disable the noise.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full trust boundary, the integrity gate
(`assert_stock_architecture`, per-sample-independence probe, releasability), the runner
hash-pinning, and the Tier-2 isolation/sandbox details.

## Server-side lifecycle (exported DataSHIELD methods)

| Stage | Methods |
|---|---|
| Connectivity / capabilities | `flowerPingDS`, `flowerCheckConnectivityDS`, `flowerGetCapabilitiesDS` |
| Handle lifecycle | `flowerInitDS`, `flowerDestroyDS` |
| Run preparation (DP set + budget reserved here) | `flowerPrepareRunDS` |
| SuperNode lifecycle | `flowerEnsureSuperNodeDS`, `flowerCleanupRunDS`, `flowerStatusDS` |
| Template / code integrity | `flowerListTemplatesDS`, `flowerGetTemplateDS`, `flowerVerifyAppHashDS` |
| Controlled outputs | `flowerMetricsDS`, `flowerLogDS`, `flowerPrivacyBudgetDS` |

Opal/Rock may run cleanup in a different process than the one that launched a SuperNode, so
the package records process metadata on disk and reaps stale staging dirs and orphan
SuperNodes (fork-free, to stay safe under the FL runtime's threads).

## Minimal client-side example

Researchers use `dsFlowerClient`, not this package directly:

```r
library(dsFlowerClient); library(DSI); library(DSOpal)

builder <- DSI::newDSLoginBuilder()
builder$append(server = "site1", url = "https://opal1.example.org",
               user = "researcher", password = "...",
               table = "PROJECT.training_data", driver = "OpalDriver")
conns <- DSI::datashield.login(builder$build(), assign = TRUE, symbol = "D")

# DP is always applied by the node; the analyst does not set it.
fit <- ds.flower.fit(conns, symbol = "D", target = "diagnosis", model = "pytorch_logreg")

ds.flower.metrics(fit)
ds.flower.privacy.budget(conns, symbol = "D")   # query the node's real remaining budget
DSI::datashield.logout(conns)
```

## Authors

- **David Sarrat González** — david.sarrat@isglobal.org
- **Juan R González** — juanr.gonzalez@isglobal.org

[Barcelona Institute for Global Health (ISGlobal)](https://www.isglobal.org/)
