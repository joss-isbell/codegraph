# Figure captions and metric limits

## main-process-private-commit

Private committed virtual memory (MiB) for explicit positively identified main engine/proxy incarnations, not a product-group total or physical-RAM measure. Native observations are approximately 1 Hz, not continuous exact peaks. Panels preserve each arm's actual elapsed duration and phase boundaries. Missing/invalid observations break curves; helpers and unknown short-lived processes are not silently counted as zero.

## isolate-heaps-observer

Individual isolate JavaScript heap observations for Observer on; MiB, periodic samples only, actual active/quiet/reheat windows. Heap used is included in total heap. Facets are not summed; missing samples and worker lifetimes remain gaps. Event-weighted snapshots, process RSS, external and ArrayBuffer fields are intentionally not mixed into these curves. Illustrative selection: main proxy/engine, three of twelve initial pool workers, and the later reheat worker; not a representative sample or aggregate.

MiB = 1,048,576 bytes. Lines connect adjacent valid samples only; no zero-fill, extrapolation after an isolate lifetime, or native-minus-JS residual. Phase alignment shifts each arm's actual time origin without rescaling durations.

Native panels show the reviewed main engine/proxy only, not helpers or a product-group total. Global quiet ticks are not an independently proven-alive coverage denominator. Native incarnation/helper coverage remains governed by the hash-bound independent review.
