# dsFlower

`dsFlower` is the server-side DataSHIELD package that lets approved
[Flower](https://flower.ai/) federated learning templates run inside
Opal/Rock nodes. It is installed by the data-owning institution. Researchers
use the companion client package,
[`dsFlowerClient`](https://github.com/isglobal-brge/dsFlowerClient), from their
local R session.

The package does not turn DataSHIELD into an unrestricted remote Python
executor. It exposes a controlled federated learning surface:

- authorised Flower templates installed on the server;
- server-side staging of selected columns, feature tables or image manifests;
- privacy profiles controlled by DataSHIELD server options;
- Secure Aggregation and Differential Privacy compatibility checks;
- code hash verification before training starts;
- disclosure-controlled metrics, logs and privacy-budget reporting;
- cleanup and orphan-process handling for Flower SuperNodes.

Model weights and updates travel through Flower's gRPC/TLS plane. DataSHIELD
continues to carry handles, preparation requests, status, approved metrics and
cleanup calls. Patient-level rows, raw images, masks and staging files remain
inside the institutional server.

## Package roles

| Package | Installed where | Main responsibility |
|---|---|---|
| `dsFlower` | Each Opal/Rock DataSHIELD server | Validate requests, stage data locally, enforce server policy, start Flower SuperNodes and expose controlled status/metrics. |
| `dsFlowerClient` | Researcher workstation | Build recipes, start the researcher-side Flower SuperLink, call authorised DataSHIELD methods, launch runs and render validation evidence. |

This architecture follows the DataSHIELD governance model: the researcher
selects from approved server-side capabilities rather than submitting arbitrary
training code to the data-owning institutions.

## Installation

Install the server package on each Opal/Rock node:

```r
remotes::install_github("isglobal-brge/dsFlower")
```

The package `configure` script prepares the server-side Python runtime root and
uses [uv](https://docs.astral.sh/uv/) to provision template-specific Python
environments on demand. Templates use Python libraries such as Flower,
scikit-learn, PyTorch, XGBoost and Opacus according to the selected model
family.

Install the client package on the researcher workstation:

```r
remotes::install_github("isglobal-brge/dsFlowerClient")
```

## Authorised templates

The current catalogue covers 18 server-side templates, exposed by
20 client-facing model constructors. `sklearn_svm` and
`sklearn_elastic_net` are convenience constructors that resolve to the
`sklearn_sgd` template with hinge-loss and elastic-net configurations.

| Family | Templates |
|---|---|
| scikit-learn linear models | `sklearn_logreg`, `sklearn_ridge`, `sklearn_sgd` |
| PyTorch tabular models | `pytorch_logreg`, `pytorch_mlp`, `pytorch_linear_regression`, `pytorch_multiclass`, `pytorch_multilabel`, `pytorch_poisson` |
| PyTorch survival models | `pytorch_coxph`, `pytorch_lognormal_aft`, `pytorch_cause_specific_cox` |
| PyTorch sequence models | `pytorch_lstm`, `pytorch_tcn` |
| PyTorch vision models | `pytorch_resnet18`, `pytorch_densenet121`, `pytorch_unet2d` |
| Gradient boosting | `xgboost` with a histogram aggregation protocol |

Each template declares its framework, accepted hyperparameters, data shape,
minimum row policy and privacy-profile compatibility. Requests outside those
server-side bounds fail before local data are staged.

## Privacy profiles

The client may request a privacy profile, but the server determines and enforces
the effective policy.

| Profile | Intended setting | Main controls |
|---|---|---|
| `sandbox_open` | Local development and catalogue checks | Minimal restrictions; requires explicit administrator opt-in. |
| `trusted_internal` | Controlled internal experiments | Staging and metric controls; per-node metrics may be visible. |
| `consortium_internal` | Multi-site consortium runs | Requires SecAgg+, fixed participation and suppressed per-node metrics. |
| `clinical_default` | Default clinical profile | Requires SecAgg+, stricter row/event guards and controlled output policy. |
| `clinical_hardened` | More restrictive clinical studies | Higher row/event thresholds and stricter participation requirements. |
| `clinical_update_noise` | Update or histogram noise hardening | Requires SecAgg+ and adds bounded Gaussian noise where the template supports it. |
| `high_sensitivity_dp` | Patient-level DP-SGD runs | Requires SecAgg+ and only allows templates validated for Opacus per-example gradients. |

`clinical_update_noise` and `high_sensitivity_dp` are deliberately distinct.
The first hardens released updates or histograms; it is not a patient-level
DP-SGD claim. The second is restricted to templates whose losses decompose by
sample and whose model layers are compatible with Opacus accounting.

## Server-side lifecycle

The exported DataSHIELD methods are organised by lifecycle stage:

| Stage | DataSHIELD methods |
|---|---|
| Connectivity and capabilities | `flowerPingDS()`, `flowerCheckConnectivityDS()`, `flowerGetCapabilitiesDS()` |
| Handle lifecycle | `flowerInitDS()`, `flowerDestroyDS()` |
| Run preparation | `flowerPrepareRunDS()` |
| SuperNode lifecycle | `flowerEnsureSuperNodeDS()`, `flowerCleanupRunDS()`, `flowerStatusDS()` |
| Template and code integrity | `flowerListTemplatesDS()`, `flowerGetTemplateDS()`, `flowerVerifyAppHashDS()` |
| Controlled outputs | `flowerMetricsDS()`, `flowerLogDS()`, `flowerPrivacyBudgetDS()` |

This separation is important in Opal/Rock deployments, where Rserve processes
may not be the same process that originally launched a SuperNode. `dsFlower`
therefore records process metadata on disk and includes cleanup logic for stale
staging directories and orphan SuperNode processes.

## Validation evidence

The current presentation evidence is maintained in the `dsFlowerClient`
repository, because validation runs are launched from the client and rendered
in its pkgdown articles.

| Evidence layer | Current role |
|---|---|
| Public clinical benchmarks | Breast Cancer Wisconsin, UCI Heart Disease, Pima Indians Diabetes and CDC Diabetes Health Indicators compare centralised and federated held-out metrics. |
| Clinical privacy profiles | `clinical_default`, `clinical_update_noise` and `high_sensitivity_dp` exercise SecAgg+, bounded histogram noise and Opacus DP-SGD where applicable. |
| SUPPORT2 method-family runs | Continuous regression, count regression, multiclass, multilabel and Cox survival tasks validate non-binary clinical families under `clinical_default`; the four DP-SGD-compatible families are repeated under `high_sensitivity_dp`. |
| Imaging handoff | LUNG1 radiomics features and LUNG1 direct-image assets are resolved through `dsImaging`; PathMNIST validates a larger direct-image ResNet path with 1,500 images. |
| Catalogue coverage | The 17 non-vision and 3 vision templates are also exercised on deterministic synthetic fixtures. It complements the biomedical validation reports by checking the full authorised template surface. |

Use the client pkgdown site for the rendered reports:
<https://isglobal-brge.github.io/dsFlowerClient/>.

## Minimal client-side example

The researcher normally uses `dsFlowerClient`, not this server package directly:

```r
library(dsFlowerClient)
library(DSI)
library(DSOpal)

builder <- DSI::newDSLoginBuilder()
builder$append(
  server = "site1",
  url = "https://opal1.example.org",
  user = "researcher",
  password = "...",
  table = "PROJECT.training_data",
  driver = "OpalDriver"
)
builder$append(
  server = "site2",
  url = "https://opal2.example.org",
  user = "researcher",
  password = "...",
  table = "PROJECT.training_data",
  driver = "OpalDriver"
)

conns <- DSI::datashield.login(builder$build(), assign = TRUE, symbol = "D")

fit <- ds.flower.fit(
  conns,
  symbol = "D",
  target = "diagnosis",
  features = c("age", "sex", "biomarker"),
  model = "sklearn_logreg",
  strategy = "fedavg",
  privacy = "clinical_default",
  rounds = 5L
)

ds.flower.metrics(fit)
DSI::datashield.logout(conns)
```

## Authors

- **David Sarrat González** — david.sarrat@isglobal.org
- **Juan R González** — juanr.gonzalez@isglobal.org

[Barcelona Institute for Global Health (ISGlobal)](https://www.isglobal.org/)
