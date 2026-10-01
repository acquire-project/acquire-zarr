#pragma once

#include <cstddef>
#include <functional>

namespace zarr {

// Fixed-size parallel-for with reduction, used for the per-frame tile scatter in
// Array::write_frame_to_chunks_. Replaces a `#pragma omp parallel for
// reduction(+:...)` so the library no longer depends on OpenMP.
//
// Splits the half-open range [0, n) into contiguous blocks across a small,
// persistent worker team (see kTileCopyTeamSize) and sums the size_t each block
// returns. `block(begin, end)` must process iterations [begin, end) and return
// the number of bytes it wrote.
//
// For small n (< kMinTilesForParallel) or a single-core host it runs inline on
// the calling thread with no fork/join -- the small-workload shortcut (Nathan
// Clack's observation) that avoids thread wake-up overhead dominating a tiny
// copy. The team is capped small because measured copy latency saturates by ~4
// threads; a per-core team only adds oversubscription/barrier cost at scale.
//
// Exceptions thrown by `block` are captured and rethrown on the calling thread
// after the team rejoins, preserving single-threaded error semantics.
size_t
parallel_for_reduce(int n,
                    const std::function<size_t(int begin, int end)>& block);

// Size of the persistent worker team actually in use on this host
// (min(kTileCopyTeamSize, hardware_concurrency)). Exposed for tests/benchmarks.
int
tile_copy_team_size();

} // namespace zarr
