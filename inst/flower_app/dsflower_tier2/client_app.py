"""Tier-2 trusted runner ClientApp (node side) — new Message API.

Runs the researcher's uploaded `local_update` (untrusted, sandboxed) from the
incoming global model, then applies the output-perturbation DP gate before
returning. The DP guarantee holds for any uploaded code (the gate, not the app,
bounds sensitivity + adds noise). DP params come from the tamper-proof manifest.
"""

from flwr.clientapp import ClientApp
from flwr.common import ArrayRecord, Context, Message, MetricRecord, RecordDict

from .task import load_data, load_privacy_config
from .tier2_lib import gated_local_update

try:
    import dp_harness
except ImportError:
    from . import dp_harness


app = ClientApp()


@app.train()
def train(msg: Message, context: Context) -> Message:
    cfg = context.run_config
    user_module = str(cfg.get("user-module", ""))
    if not user_module:
        raise ValueError("Tier-2 run requires 'user-module' in the run config.")

    X, y = load_data(context)
    pcfg = load_privacy_config(context)
    # The node runs the upload OUT-OF-PROCESS (gated_local_update spawns an isolated
    # interpreter); it never imports the untrusted module here.

    global_arrays = msg.content["arrays"].to_numpy_ndarrays()
    # Compose the DP budget over rounds (DP-FedAvg). Basic (sequential) composition of R
    # Gaussian releases each (eps/R, delta/R)-DP yields (eps, delta)-DP, so BOTH eps and
    # delta are split by num_rounds -- splitting only eps would leave the total at
    # (eps, R*delta) (e.g. 5 rounds at delta=1e-5 -> delta=5e-5), not (eps, delta).
    num_rounds = max(1, int(cfg.get("num-server-rounds", 1)))
    pcfg_round = dict(pcfg)
    pcfg_round["epsilon"] = float(pcfg["epsilon"]) / num_rounds
    pcfg_round["delta"] = float(pcfg["delta"]) / num_rounds
    gated = gated_local_update(user_module, global_arrays, X, y, dict(cfg), pcfg_round)

    n_examples = dp_harness.bucket_count(len(X))
    reply = RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=gated),
        "metrics": MetricRecord({"num-examples": n_examples}),
    })
    return Message(content=reply, reply_to=msg)
