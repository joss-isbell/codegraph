# Figure captions and metric limits

## main-process-private-commit-agent

Private committed virtual memory (MiB) for explicit positively identified main engine/proxy incarnations, not a product-group total or physical-RAM measure. Native observations are approximately 1 Hz, not continuous exact peaks. Panels preserve each arm's actual elapsed duration and phase boundaries. Missing/invalid observations break curves; helpers and unknown short-lived processes are not silently counted as zero.

## isolate-heaps-agent

Individual isolate JavaScript heap observations for Ordinary agent (3 MCP calls / 3 turns); MiB, periodic samples only, actual active/quiet/reheat windows. Heap used is included in total heap. Facets are not summed; missing samples and worker lifetimes remain gaps. Event-weighted snapshots, process RSS, external and ArrayBuffer fields are intentionally not mixed into these curves. Four participating isolates from one single lead: three MCP calls across three turns; not matched burst demand or an additive total.

MiB = 1,048,576 bytes. Lines connect adjacent valid samples only; no zero-fill, extrapolation after an isolate lifetime, or native-minus-JS residual. Phase alignment shifts each arm's actual time origin without rescaling durations.

Native panels show the reviewed main engine/proxy only, not helpers or a product-group total. Global quiet ticks are not an independently proven-alive coverage denominator. Native incarnation/helper coverage remains governed by the hash-bound independent review.

Separate ordinary single-lead agent arm: three MCP calls across three completed model turns, not the deterministic matched burst. Plot starts at the actual first model-turn offer and ends at actual reheat-turn completion. Active/model-turn intervals are not CodeGraph-only execution. Actual connected quiet bounds come from the client result after native snapshots, not raw phase envelopes. Initial standing, the later scheduled 60-second post-reheat quiet and post-exit observation are excluded; that later quiet is not the same equilibrium window. No post-exit zero tail. Reheat worker has only three periodic samples in the plotted window: points, not a supported continuous trend.
