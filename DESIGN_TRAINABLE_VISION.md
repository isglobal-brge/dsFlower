# Trainable vision contracts, 0.7.4

The default frozen branch retains its construction, model specification, seed
projection, feature preprocessing and DP-SGD path. Opt-in `backbone-mode`
(`finetune` or `scratch`) is a manifest-pinned semantic input. The R admission
layer requires the node-owned strict logical option
`dsflower.allow_trainable_backbone=TRUE`, patient adjacency and a supported
vision classification contract. It writes `allow-trainable-backbone=true` to
the protected manifest. Python independently enforces this permission, mode,
geometry and schedule before private image reads. No analyst config can enable
permission. Unsupported private validation, holdout and CV fail at public
admission.

The full model is a trusted sequential backbone and declarative bounded head.
Fine-tuning consumes the existing full-SHA-verified ImageNet1K V1 checkpoint or
fixed MONAI seed-0 encoder. Scratch uses isolated public seed 0 and no checkpoint
access. The 2D fine-tuning coordinator must also have the pinned checkpoint
preseeded to construct the initial full network; scratch and saved full-state
prediction need no pretrained file. A versioned trainable architecture profile
binds the public seed and
mode through `initialisation.py`; incoming global arrays remain semantic inputs
and the initial-content digest remains pinned. The default frozen spec and
checkpoint digests are unchanged. The same compatible runtime on each node gives
identical scratch initialisation; the coordinator sends all weights every round.

BatchNorm is converted only in this trusted trainable construction, retaining
its public affine parameters and dropping running buffers. GroupNorm has no
cross-example statistics. Every parameter is trainable and buffer-free. The
stock-module and release-key guards still inspect the whole model. The full
state dict is exactly the ordered trainable parameter set.

For patient p with m images, let z_p = mean_i encoder(x_pi) and
L_p = loss(head(z_p), y_p), with the established public target rule. First compute
z_p without retaining image graphs. Differentiate L_p through the head and z_p;
then recompute each image's encoder graph and backpropagate (dL_p/dz_p)/m.
This is the chain rule for the same patient mean, holding one image graph at a
time. Concatenate all encoder/head gradients conceptually and clip their joint
L2 norm to C. Non-finite gradient coordinates are totalized (NaN to zero, infinities to
+/-C), with coordinate saturation before the joint clipping step. Sum these clipped gradients, add independent Gaussian noise of
standard deviation sigma*C to every coordinate (also on an empty Poisson draw),
and divide by the fixed expected batch size.

The existing `effective_dpsgd_mechanism` calibrates sigma for the same secure
Poisson patient sampler, number of steps, epsilon/delta and replace-one
conversion. Dimensionality does not change this accountant: joint clipping
bounds a patient's complete vector. All optimiser inputs derived from private
patients are clipped and noised. The only release is the resulting weight
vector with the existing constant aggregation weight. FedAvg, bounded parameter
projection, local optimisation driven by these gradients and final serialization
are post-processing. Neither private gradients nor intermediate features,
patient counts, losses, BatchNorm statistics or new per-site metrics leave.
Existing sticky release and neighbourhood protections remain in force; this
feature does not upgrade the package's existing per-training guarantee into a
new claim of transcript DP across arbitrary repeated jobs.

The ordinary release guard and array validator admit larger payloads only for
this authorised mode: 512 arrays, 40M parameters and 160 MiB. The neural
neighbourhood payload codec already carries the complete ordered arrays, while
the separate Hook output-perturbation cache/envelope keeps its old limits.
Flower 1.31's gRPC ceiling is 2,147,483,647 bytes; DSI's 512 KiB chunks already
stream payloads through a bounded spool. Full models use `model.pt`; the existing
50 MiB estimated-JSON ceiling skips expensive list conversion.

CPU admission bounds public image geometry (2D <=64, 3D <=128), epochs (<=5),
rounds (<=20), nominal patient batch size (<=8), trainable parameters (<=40M),
and Torch threads (<=4). Memory holds one image graph plus model/optimizer,
patient-gradient and batch-gradient tensors. These are bounds on per-image
compute and schedule; total time remains proportional to the admitted cohort
and image multiplicity. No private-count work rejection or elapsed-time early
release is added. Full-network DP-SGD is often slow and has poor utility with
few patients; frozen features remain recommended then.
