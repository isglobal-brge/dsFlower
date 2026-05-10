# dsFlower

Privacy-preserving federated learning for biomedical research, integrating [DataSHIELD](https://www.datashield.org/) and [Flower](https://flower.ai/).

Train machine learning models across multiple hospitals without moving patient data. Each institution keeps its data behind its own DataSHIELD server. Only model weight updates travel between nodes via [Flower's](https://flower.ai/) gRPC protocol.

**dsFlower** is the server-side R package (installed on each Opal/Rock node). **[dsFlowerClient](https://github.com/isglobal-brge/dsFlowerClient)** is the client-side R package used by the researcher.

## Quick start

```r
library(dsFlowerClient)
library(DSI)
library(DSOpal)

# Connect to hospital nodes
builder <- DSI::newDSLoginBuilder()
builder$append(server = "hospital_a", url = "https://opal1.example.org",
               user = "researcher", password = "...",
               table = "PROJECT.clinical_data", driver = "OpalDriver")
builder$append(server = "hospital_b", url = "https://opal2.example.org",
               user = "researcher", password = "...",
               table = "PROJECT.clinical_data", driver = "OpalDriver")
conns <- DSI::datashield.login(logins = builder$build(),
                               assign = TRUE, symbol = "D")

# Train a federated model
flower <- ds.flower.connect(conns, symbol = "D")

result <- ds.flower.run(flower, ds.flower.recipe(
  model         = ds.flower.model.sklearn_logreg(),
  target_column = "diagnosis",
  num_rounds    = 10L
))

ds.flower.disconnect(flower)
DSI::datashield.logout(conns)
```

## Models

| Framework | Models |
|---|---|
| scikit-learn | `sklearn_logreg`, `sklearn_ridge`, `sklearn_sgd`, `sklearn_elastic_net`, `sklearn_svm` |
| PyTorch | `pytorch_mlp`, `pytorch_logreg`, `pytorch_linear_regression`, `pytorch_multiclass`, `pytorch_multilabel`, `pytorch_poisson` |
| PyTorch (survival) | `pytorch_coxph`, `pytorch_lognormal_aft`, `pytorch_cause_specific_cox` |
| PyTorch (vision) | `pytorch_resnet18`, `pytorch_densenet121`, `pytorch_unet2d` |
| PyTorch (sequence) | `pytorch_lstm`, `pytorch_tcn` |
| XGBoost | `xgboost` (histogram-based secure aggregation) |

## Strategies

`FedAvg` | `FedProx` | `FedAdam` | `FedAdagrad` | `FedBN`

## Privacy profiles

| Profile | Use case |
|---|---|
| `sandbox_open` | Development and testing |
| `trusted_internal` | Single institution |
| `consortium_internal` | Multi-site with SecAgg+ |
| `clinical_default` | Hospital deployment |
| `clinical_hardened` | High-security clinical |
| `high_sensitivity_dp` | Differential privacy (DP-SGD via Opacus) |

Profiles that require Secure Aggregation need a Flower runtime with
server-side `SecAggPlusWorkflow` support and at least three participating
clients. `flowerGetCapabilitiesDS()` reports this as
`secure_aggregation_supported`; dsFlower fails early if a requested profile
cannot be executed safely instead of launching a doomed training run.

## Installation

**Server** (on each Opal/Rock node):

```r
remotes::install_github("isglobal-brge/dsFlower")
```

The `configure` script auto-provisions Python environments (scikit-learn, PyTorch, XGBoost) via [uv](https://docs.astral.sh/uv/).

**Client** (researcher workstation):

```r
remotes::install_github("isglobal-brge/dsFlowerClient")
```

Requires Python with [Flower](https://flower.ai/): `pip install flwr>=1.13.0`

## Architecture

```
Researcher (R)                     Hospital A (Opal/Rock)
+-----------------+                +------------------------+
| dsFlowerClient  |  DataSHIELD   | dsFlower               |
|   ds.flower.*() |-------------->|   flowerInitDS()       |
|                 |               |   flowerPrepareRunDS() |
| SuperLink       |  gRPC/TLS    |   SuperNode            |
|   (localhost)   |<=============>|   (Flower ClientApp)   |
+-----------------+    weights    +------------------------+
        |
        |             Hospital B
        |             +------------------------+
        +============>| SuperNode              |
              gRPC    | (Flower ClientApp)     |
                      +------------------------+
                      data never leaves here
```

## Authors

- **David Sarrat González** — david.sarrat@isglobal.org
- **Juan R González** — juanr.gonzalez@isglobal.org

[Barcelona Institute for Global Health (ISGlobal)](https://www.isglobal.org/)
