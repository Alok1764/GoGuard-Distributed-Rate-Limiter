# GoGuard interview notes

This note matches the code that exists today: four in-memory limiters behind one `Allow` method, fixed window, sliding window log, sliding window counter, and token bucket. Each key has its own mutex. A sweep deletes idle keys. Redis, the HTTP gateway, the leaky bucket, and metrics are not built yet. Saying they are finished is the answer that fails the interview.

The invariant both limiters protect:

> For one key, over the interval that limiter defines, the number of allowed calls is at most `limit`. A refusal tells the caller when a slot opens again.

The interval is not the same thing in each algorithm. Fixed window uses an aligned bucket. The log uses the trailing window ending at `now`. The counter estimates that same trailing window from the previous bucket and the current bucket. The token bucket is not a window: it spends a token and refills at a steady rate.

---

## 1. What is built

| Piece | Where | What it does |
|---|---|---|
| `Limiter` | `ratelimit/limiter.go` | `Allow(ctx, key) (Decision, error)`. Every future algorithm will implement this. |
| `Decision` | same file | Allowed, limit, remaining, retry-after, reset time. This is what HTTP headers will be built from. |
| `Clock` | same file | `Now() time.Time`. Production uses the wall clock. Tests pass a clock they can move. |
| `FixedWindow` | `ratelimit/fixedwindow.go` | One counter per key per aligned bucket. The map lock only finds the pointer. The counter lock updates that key. `Evict` drops idle keys. `Close` stops the sweep. |
| `SlidingWindowLog` | `ratelimit/slidinglog.go` | One timestamp per allowed call still inside the trailing window, at most `limit` of them. Same lock order and the same `Allow` result type. |
| `SlidingWindowCounter` | `ratelimit/slidingcounter.go` | `prev` and `curr` bucket totals. Estimate is `prev * (window-elapsed) / window + curr`, computed with integer cross-multiplication. |
| `TokenBucket` | `ratelimit/tokenbucket.go` | `burst` tokens, refilled at `rate` per `per`. A new key starts full. Partial refill time is kept in `remainder`. |
| Tests | `ratelimit/*_test.go` | The three window limiters, plus the token bucket: burst then one token at a time, refill capped at burst, a 50ms leftover, and eviction only of a full bucket. |

A call is allowed when `count < limit`, then `count` becomes `count + 1`. A refused call does not change `count`. The bucket id is `now.Truncate(window)`, which is the start of the current window on the absolute clock, not the time of the first request.

`go test ./ratelimit/` passes. `go test -race` has not been run on this machine: the race detector needs a C compiler, and `gcc` is not installed. The concurrent test checks the allowed count. It does not replace the race detector.

---

## 1b. What changed from the first version, and why

The first version was correct and coarse. Two later changes replaced it. The algorithm (aligned bucket, allow while `count < limit`, refusals do not increment) did not change.

### Change 1. One mutex for every key became a map lock plus a lock per key

**Before.** `Allow` held `f.mu` for the whole decision: find the key, read the clock, maybe reset the bucket, maybe increment. Alice's request waited while Bob's counter was updated. The 64-goroutine test used one key, so it never showed that wait. It only showed that one key could not go over its limit.

**After.** `f.mu` is held only to find or insert the pointer. That key's own mutex covers the clock read, the bucket reset, and the increment. The map lock is released before `clock.Now()`. `TestFixedWindowKeysDoNotShareALock` blocks Alice inside `Now` and fails if Bob does not return.

**Why.** Unrelated callers were sharing a lock whose critical section belonged to one key. Splitting it removes that wait without changing `Allow`'s result.

**What it cost.** A burst of never-seen keys still lines up on the map lock. There is a second mutex per key, so the struct is larger. Lock order is now part of the program: map, then counter, in both `Allow` and `Evict`. The counter must be locked before the map lock is released. If it is not, `Evict` can drop the pointer and the in-flight call increments a counter the map no longer holds. The next call starts at zero and the key gets an extra allow.

**What I did not do.** Striped map locks. That is the next step only if a benchmark shows the map lock waiting. I have no such number yet.

### Change 2. The map kept every key forever; a sweep now deletes idle ones

**Before.** The first request for a key inserted a counter. Nothing deleted it. A caller who appeared once occupied memory until the process exited. The count was still correct. The leak was separate from the count.

**After.** `Evict` deletes a counter whose bucket is not the current one. A goroutine calls it once per window. `Close` stops that goroutine. `Len` is how a test sees the map without reading unexported fields. `TestFixedWindowEvictDropsIdleKeysOnly` advances the clock, lets Alice into the new bucket, and checks that only Bob is removed.

**Why.** An open set of client identities (source IPs, API keys) would grow the map without bound. Deleting on the next `Allow` cannot help a key that never calls again.

**What it cost.** One-shot keys live until the next sweep, up to one window. The sweep holds the map lock for the whole walk, so a large map stalls inserts. `Close` is now required in tests, or the goroutine outlives the test.

**What I did not do.** Delete a key that is still inside the current bucket, even if its limit is already spent. That key is not idle. Removing it would create a fresh counter and another `limit` allows in the same bucket. I also did not add a second deletion path inside `Allow`. Reset-on-next-call and sweep-the-absent-keys are different jobs, and only the sweep sees a key that never returns.

### Change 3. A second algorithm, because the bucket edge is a real hole

**Before.** Only `FixedWindow`. Limit 3 per second allows 3 calls at `T-1ms` and 3 more at `T`. The counter is right inside each bucket. The caller still got twice the limit in 2ms. `TestFixedWindowBoundaryBurst` exists to show that, not to hide it.

**After.** `SlidingWindowLog` implements the same `Limiter` interface. It stores the time of each allowed call and counts a hit while `now.Sub(t) < window`. A hit whose age is exactly the window is already gone. `TestSlidingLogDoesNotBurstAtAFixedBoundary` places 3 hits in the first 200ms of a 1-second window, checks that a 4th is refused with `Retry-After` of 800ms (when the oldest hit ages out), then steps to that instant and checks that exactly one new call is allowed.

**Why.** The fixed window cannot make the promise "at most N in any span of this length." The log can. Callers still see `Decision`. Nothing in `Allow`'s signature changed, so a later config switch does not rewrite the tests that only depend on the interface.

**What it cost.** Each live key holds a slice of timestamps, length at most `limit`. A limit of 100,000 means up to 100,000 `time.Time` values for that key. Every `Allow` walks from the front of that slice to drop expired hits. The fixed window is still the right tool when the quota is coarse and the boundary spike is acceptable. The log is the tool when the count has to be exact and the limit is small enough to store.

**What I did not do.** Store refused calls. They do not change the count, same as the fixed window. I also did not replace the fixed window. Both stay, because they answer different promises.

### Change 4. Two integers instead of a timestamp per call

**Before.** The log is exact and stores up to `limit` timestamps per live key. A large limit makes that list the cost.

**After.** `SlidingWindowCounter` stores `prev`, `curr`, and the bucket start. The estimate is `prev * (window - elapsed) / window + curr`. The code compares `prev * (window - elapsed)` with `(limit - curr) * window` so the weight is never a float. A call is refused when that estimate is already at least `limit`. `ErrOverflow` refuses a limit and window whose product does not fit in an int64, because a wrapped multiply would allow or deny the wrong call.

**Why.** Most of the log's accuracy, without a slice that grows with the limit. The fixed window is still there for a coarse quota. The log is still there when the count has to be exact and the limit is small.

**What it cost.** The weight assumes the previous bucket was spread evenly. `TestSlidingCounterDisagreesWithLogOnAClusteredPreviousBucket` fills a bucket and steps exactly one window: the log allows, because every hit has aged out, and the counter refuses, because the whole previous total still has weight 1. `TestSlidingCounterOverAdmitsAgainstTheLog` puts the burst on the last nanosecond of the bucket and steps nine seconds into the next one: the log still sees those hits, and the counter allows. A two-bucket gap sets both counts to 0. Copying `curr` into `prev` across that gap would invent traffic.

**What I did not do.** The leaky bucket. That is the other rate-shaped limiter: it refuses when the water would pass a line, instead of spending a saved token.

### Change 5. A burst plus a rate, instead of a window

**Before.** The three window limiters answer "how many calls fell in this interval?" A second burst in the fixed window is an accident of the bucket edge. The log and the counter argue about that interval. None of them has a knob that means "you may take 20 at once, then only 10 per second."

**After.** `TokenBucket` stores whole tokens, a leftover duration, and the last update. A new key starts at `burst`. `Allow` adds `elapsed * rate / per` whole tokens, caps at `burst`, and spends one. The division leftover stays in `remainder`. `TestTokenBucketKeepsThePartialToken` waits 250ms at 10 tokens per second and gets 2 tokens, then a refusal whose `Retry-After` is 50ms. There is no ticker goroutine per key.

**Why.** The burst is configured. Refill on the next call is the same math as a ticker, without one goroutine per caller.

**What it cost.** `tokens` and `remainder` move together, so the key mutex stays. A restart starts every key full, which is a free burst on deploy. `Evict` deletes only a bucket that is already full. Deleting an empty one would create a new full bucket and hand the caller the burst again.

**What I did not do.** The leaky bucket meter. A token bucket allows the burst up front. The meter is the version that refuses to let the level pass a line.

---

## 2. How one `Allow` runs

Key `"alice"`, limit 3, window 1 second, clock frozen at `10:00:00.000`. The map starts empty.

1. If the context is already canceled, return that error. The counter is untouched.
2. If the key is empty, return `ErrEmptyKey`.
3. Lock the map, find or create Alice's counter, lock that counter, then unlock the map. Bob can use the map as soon as the pointer is in hand. He does not wait for Alice's increment.
4. Read `now` from the clock while Alice's counter lock is held. `start = now.Truncate(window)` is `10:00:00.000`. `resetAt` is `10:00:01.000`.
5. If this counter's `start` is a different bucket, set `start` to the new one and `count` back to 0.
6. If `count` is already 3, return denied. `RetryAfter` is `resetAt - now`. `count` stays 3.
7. Otherwise add 1 and return allowed. After the third allow, `Remaining` is 0 and `Allowed` is still true. The fourth call is the one that is denied.
8. Returning unlocks Alice's counter. The map lock was already released in step 3.

The third allow meaning "allowed, and nothing left" is deliberate. `Remaining` is what is left after this call.

---

## 3. Interview questions and answers

Speak these in the first person. The numbers are from this code, not from a benchmark you have not run.

### Q1. What does your limiter actually guarantee?

For a single process, for one key, inside one aligned bucket, `Allow` returns `Allowed: true` at most `limit` times. Two goroutines on that key cannot both pass the check on the same count, because the read, the bucket reset, and the increment happen while that key's mutex is held. A different key uses a different mutex.

It does not guarantee a smooth rate. It does not guarantee the limit across two processes. It does not guarantee the limit across a bucket boundary. Those are the next designs, and they are not this code.

### Q2. Walk through the boundary bug with numbers.

Limit 100 per minute. Bucket A is `10:00:00.000` to `10:00:59.999`. Bucket B starts at `10:01:00.000`.

One hundred allows at `10:00:59.000` fill bucket A. One hundred allows at `10:01:00.000` fill bucket B. The counter is right in each bucket. The caller still got 200 requests in one second. `TestFixedWindowBoundaryBurst` freezes the clock 1ms before the roll, uses the limit, steps 1ms, and gets a fresh limit. That test exists so I can show the hole instead of describing it.

Fixed window is the right tool when the quota is coarse ("1000 requests per day") and a spike on the boundary is acceptable. It is the wrong tool when the product promise is "at most N per second, including across the second."

### Q3. Why a mutex, not an atomic integer?

The decision is two fields: which bucket (`start`) and how many allows (`count`). A new bucket must zero `count` and store the new `start` together. An atomic add on `count` alone lets two goroutines both observe the old bucket, both reset, and both increment. You mint extra allows, or you lose increments.

Atomics protect one word. A mutex protects an invariant that spans fields. A fixed window can be packed into one word and updated with a compare-and-swap loop. That version is longer, easier to get wrong, and it still does not help the sliding-window log, which stores a list of timestamps. The mutex is the version I can defend for every algorithm.

### Q4. Why not `sync.Map`?

`sync.Map` is built for a key that is written once and then read a lot, or for keys that are almost never deleted. This hot path is a read-modify-write of the counter on every call. `sync.Map` does not make that update atomic. I would still need a mutex inside each counter. Using `sync.Map` and claiming the lock is gone is the wrong answer.

### Q5. Why not one goroutine that owns the map, with requests sent over a channel?

That is correct. The owner is the only goroutine that touches the map, so there is no lock. Throughput is then capped at how fast that one loop can run, and every `Allow` pays a channel send and a reply. The lock-per-key version is what shipped. A stripe of locks is still available later if a profile shows the map lock itself waiting, and it would not change the `Allow` signature. I would pick the channel owner if the state machine were large and I wanted it easy to read. I would not pick it as the final shape of a limiter whose whole job is a few integer operations.

### Q6. Why not `RWMutex`?

A read lock helps when many callers only read. Every successful `Allow` writes `count`. The common case is a writer. A read lock would be the wrong optimization, and a writer would still exclude everyone.

### Q7. Why is `clock.Now()` inside the lock?

The timestamp chooses the bucket. Suppose a goroutine reads `10:00:00.999`, then gets paused, then takes the lock at `10:00:01.010` while still holding the old timestamp. It can reset the new bucket and throw away counts that already landed there.

So the sample of time and the update of the counter are one critical section, under that key's mutex. The map lock is already released, which is why a slow clock stalls only that key. `TestFixedWindowKeysDoNotShareALock` blocks inside Alice's `Now` and requires Bob to finish.

The clock I ship only reads `time.Now` or a test timestamp. A clock that calls back into `Allow` on the same key deadlocks, because a Go mutex is not re-entrant. That is a contract on `Clock`.

### Q8. Why `Truncate`, instead of starting the window at the first request?

`now.Truncate(window)` is `floor(now / window)` on the absolute clock. A request at `10:00:00.500` with a 1-second window belongs to the bucket that started at `10:00:00.000` and resets at `10:00:01.000`. The first request does not get a fresh private second. It gets whatever is left of the current bucket.

That is the same rule a Redis key will use later: one counter per `(key, bucket id)`, and every gateway computes the same bucket id from the server clock. Windows that start at "first time I saw this user" disagree across processes. Process A saw the user at `:00.100`. Process B saw the user at `:00.700`. They would reset at different times and the global limit would be fuzzy.

The cost of alignment: a user who arrives in the last millisecond of a bucket can be limited immediately, then limited again at the boundary. That feels unfair, and it is the same boundary spike as Q2. I accept it so that every instance means the same bucket.

### Q9. Why does a rejected call leave `count` unchanged?

Once the bucket is full, further rejections do not need to change anything. The caller is already told to wait until `resetAt`. Incrementing on rejection would not extend the window either, because the window is aligned to the clock, not to the last request. I still skip the write so the denial path does less under the lock.

If the product wanted "every attempt counts, including failures," I would increment before the comparison. That punishes a client that retries in a tight loop by... still filling the same bucket, which is already full. For a fixed window the difference is small. For a token bucket it matters: spending a token on a request you then fail upstream double-charges the client. The gateway should call `Allow` only for the request it is about to forward, or it should have an explicit refund. This code has no refund.

### Q10. The last allowed call returns `Remaining: 0`. Is that a bug?

No. `Remaining` is `limit - count` after this call consumes one. The call that takes the last slot is allowed. The next call finds `count >= limit` and is denied, with `RetryAfter` set. Clients that treat `Remaining == 0` as "this response is already a 429" are wrong. The status is `Allowed`.

### Q11. Why return `Decision` instead of `bool`?

The HTTP layer needs `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset`. If `Allow` returns only `bool`, the handler either re-implements the algorithm or the limiter grows HTTP types. `Decision` is the policy result. The handler formats headers. The limiter never imports `net/http`.

### Q12. Why is `context.Context` on an in-memory call that only checks it before the lock?

The interface is the thing callers bind to. A Redis implementation has to cancel an in-flight round trip when the request dies. Adding `ctx` later changes every caller and every test. Checking `ctx.Err()` before the lock is the honest in-memory behavior: the critical section is nanoseconds, and there is no way to abort a goroutine blocked on `Mutex.Lock` without redesigning the lock. I do not pretend the mutex wait is cancellable.

### Q13. What does the concurrent test prove, and what does it miss?

`TestFixedWindowConcurrentExactLimit` runs 64 goroutines, 20 calls each, limit 100, one key. The allowed total must be exactly 100. If the increment were outside the lock, that total drifts above 100 often enough to fail.

What it misses:

- A data race that still happens to produce the right count. `go test -race` finds that. This machine has not run it.
- Whether two keys share a lock. That is a different test: Alice blocks inside `Now`, and Bob must still return.
- A clock that moves during the race. The test uses the wall clock and a one-minute window, so the bucket does not roll mid-test.

### Q14. Why is the test package `ratelimit_test` and not `ratelimit`?

An external test can only touch exported names: `NewFixedWindow`, `Allow`, `Decision`, `Len`, `Evict`, `Close`, the sentinel errors. It cannot read `keys` or `count`. `Len` and `Evict` are the observation points for "was this idle key deleted?" A test that reached into the map would break for a layout change that users cannot see.

### Q15. What happens if the process restarts?

The map is gone. Every key gets a fresh bucket. A deploy is a free burst of `limit` requests per key. For a single-process demo that is acceptable if I say it. For a quota that must survive a restart, the counter has to live outside the process. That is the Redis step, and it is not built.

### Q16. Two gateway processes, same limit. What does the user get?

Each process has its own map. Limit 10 on two processes is about 20 allows. Each process is correct. The system is wrong. Local memory cannot see the other process's count. A shared counter, updated in one atomic step, is the fix. Two separate commands (`INCR`, then `EXPIRE`) are not that step: a crash between them leaves a key with no expiry, and that key then limits forever. One Lua script on the Redis server runs as a single step and reads Redis's clock, so the gateways do not have to agree on time.

Until that script exists, I should not describe this project as distributed.

### Q17. Redis is down. Do you allow or deny?

This is a product choice, and the code does not make it yet. Failing open keeps the site up and removes the limit during the outage. Failing closed returns 429 for everyone and keeps the limit meaningful. For a limiter whose job is to protect the backend, I would fail closed and record the Redis error on a metric that is itself not rate-limited. For a limiter that is only a courtesy cap, fail open is reasonable. I would state which one the config selects, and I would test both.

### Q18. One API key is hot enough that Redis is the bottleneck. Then what?

A strict check on every request serializes that key on one Redis slot, and every request waits on that round trip. The scalable pattern is to reserve a batch: the gateway asks Redis for 50 tokens and serves those 50 from the in-memory limiter. Redis traffic drops by about that factor. With R replicas the system can overshoot by about `50 * R` in a window. I would pick the batch size from the error I am willing to admit ("5% over the published limit"), and I would write that number down. I would not build batching until the strict one-request-one-Redis-call version is tested. Batching is an optimization with an error bound.

### Q19. How do you identify the caller once this sits in front of HTTP?

Prefer an API key the client presents. Fall back to the peer address. Trust `X-Forwarded-For` only when the immediate peer is my own proxy. If I trust that header from the open internet, any client sets it to a fresh value and gets a fresh counter. The limit disappears.

Also skip the limiter on `/healthz` and `/metrics`. A scraper that receives 429 looks like an outage.

### Q20. How do you choose among the five algorithms?

| Algorithm | State per key | What the caller experiences | When I would ship it |
|---|---|---|---|
| Fixed window | One integer and a bucket start | Cheap. Up to about `2 * limit` at the bucket edge. | Coarse quotas. Implemented. |
| Sliding window log | Up to `limit` timestamps, oldest first | Accurate over the true last window. A slot frees when the oldest hit ages out, not when a bucket rolls. | Small limits. Implemented. A limit of 100,000 stores up to 100,000 timestamps per key. |
| Sliding window counter | Previous bucket count and current bucket count | Estimate is `previous * overlap + current`. O(1) memory. The estimate can sit over or under the true count when the previous bucket is clustered. | The usual production compromise. Implemented. |
| Token bucket | Whole tokens, leftover refill time, and the last update | Burst up to capacity, then a steady refill. Refill is computed on the next call. No ticker per key. | "10 per second, with a burst of 20." Implemented. |
| Leaky bucket, as a meter | Water level and last leak time | Level rises by one per allow and leaks at a constant rate. Reject when it would pass capacity. This shape is the dual of a token bucket. | Policing: refuse, do not queue. |
| Leaky bucket, as a queue | A queue of waiting requests | Output toward the backend is smooth. If the queue is full, reject. | Shaping: I need to delay, not only refuse. `Allow` today returns immediately, so the meter fits the interface. The queue is a different API. |

Both leaky-bucket forms are not implemented. Claiming them is a false answer.

### Q21. Multi-region. Can you keep one global strict limit and stay fast?

A strict global limit needs one place that sees every increment. That place is far from somebody, and those requests wait on the cross-region round trip. The designs I would actually offer:

- A quota per region, `limit / regions`, enforced locally. No cross-region hop. A user who stays in one region only gets that region's slice.
- One global counter. Correct, and slow for distant users.
- Counters that sync after the fact. Fast, and they can overshoot while a region is partitioned.

I would ship regional quotas and say the sentence above. I would not say "CRDT" unless I can state the overshoot.

---

## 4. Limitations of this code, the fix, and the tradeoff

These are properties of `FixedWindow` as it stands. Each fix is real work, not a rename.

### L1. Up to about twice the limit at the bucket boundary

The counter resets when `Truncate` rolls. Traffic sitting on both sides of that instant is allowed twice. The test demonstrates it on purpose.

| Fix | What you gain | What you pay |
|---|---|---|
| Sliding window log | The true count over the last window. | Memory and CPU grow with requests in the window. |
| Sliding window counter | Two integers. The boundary spike shrinks to the error in the weighted estimate. | You can be slightly over or under. You must be able to say that. |
| Token bucket | The burst is a configured capacity, not an accident of the clock. | Two fields must still update together (`tokens`, `lastRefill`). Same mutex argument as Q3. A ticker per key would start one goroutine per caller. Compute refill from elapsed time instead. |

### L2. The map lock is still shared

Each key has its own counter mutex. Alice's increment does not wait for Bob's. `TestFixedWindowKeysDoNotShareALock` is the proof: Alice blocks inside the clock, Bob still returns.

What remains: creating a brand-new key still takes the map lock. A burst of never-seen keys lines up there. The counter lock does not help that burst.

| Next step | What you gain | What you pay |
|---|---|---|
| Striped locks: 64 or 256 mutexes, index is `hash(key) % n`. | Parallel inserts across stripes. | Keys that land in the same stripe still wait. The stripe count comes from a benchmark, not from taste. I do not have that benchmark yet. |
| Channel owner (Q5) | No lock. | One core. |

Lock order stays "map, then counter." `Evict` holds the map lock and then each counter lock. `Allow` takes the counter lock before it releases the map lock. Reversing either one deadlocks.

### L3. Idle keys are deleted on a timer, and only then

`Allow` resets a key when that key shows up in a new bucket. A key that never returns stays until `Evict`. The sweep calls `Evict` once per window. `Close` stops the goroutine. `TestFixedWindowEvictDropsIdleKeysOnly` advances the fake clock, lets Alice into the new bucket, and checks that only Bob is removed.

What remains: between sweeps, one-shot keys occupy memory for up to one window. The sweep holds the map lock for the whole walk, so a large map stalls new inserts. A key is idle when its `start` is not the current bucket. A key that used its whole limit and is still inside the bucket is kept. Deleting it would hand the next call a fresh counter and an extra allow.

### L4. Every key has the same limit

`NewFixedWindow` takes one `limit` and one `window`. A login route and a health check cannot differ. A paid tier and a free tier cannot differ.

Fix: a policy in front of `Allow` that maps the route and the tier to a limiter, or that passes a cost. This struct does not need a map of limits inside it. The tradeoff is another layer, and tests for "this route uses this limiter." Putting per-key limits inside `FixedWindow` mixes identity policy with the counter.

### L5. Every request costs 1

A tiny health check and a large export consume the same unit. There is no `AllowN`.

Fix: `Allow` stays cost 1 for the gateway (one HTTP request). Add `AllowN` when a caller must spend more than one unit. The tradeoff is a wider API. I would add it when a real caller needs it, and the fixed-window version is "reject if `count + n > limit`, else add `n`," still under the same lock.

### L6. Quotas vanish on restart

Covered in Q15. Fix is an external counter. The tradeoff is a network round trip on the allow path, a new failure mode (Q17), and operational dependence on Redis.

### L7. The limit multiplies by the number of processes

Covered in Q16. Fix is one atomic update in Redis (Lua), using the server clock. The tradeoff is latency, a hot key (Q18), and the fail-open versus fail-closed choice.

`MULTI`/`EXEC` without Lua still interleaves two clients' read-modify-write. A transaction is not a lock around your own logic unless the whole decision runs inside the server.

### L8. Context cancellation does not unblock the mutex

Covered in Q12. For this in-memory lock the wait is short, so I leave it. The Redis client must use the context on the round trip. Pretending `Mutex.Lock` observes `ctx.Done()` would be a lie.

### L9. `Clock.Now` runs under the key lock

Covered in Q7. A slow clock stalls that key only. A clock that calls `Allow` for the same key deadlocks. `Now` must not re-enter the limiter. Moving the call outside the key lock reintroduces the stale-bucket bug.

### L10. Time can move backwards

If `resetAt.Sub(now)` is negative, `RetryAfter` is clamped to 0. The caller may retry immediately. A backward jump that crosses a bucket boundary also opens a new counter via `Truncate`.

Wall clocks jump. A monotonic clock does not, and it also does not answer "what bucket is this on the wall calendar," which is what aligned windows need. I use the wall clock and I clamp a negative retry. I do not have a separate monotonic path.

### L11. The race detector has not run

The concurrent test passed under `go test`. `-race` failed to build because `gcc` is absent (`cgo` is required for the race detector on Windows).

Fix: install a C compiler, or run `go test -race ./ratelimit/` in a Linux container. Until that is green, "I ran the race detector" is a false claim. The honest claim is: a 64-goroutine test observed exactly `limit` allows, and the race detector is still outstanding.

### L12. No measured throughput

There is no `testing.B` yet. Any number I quote for "requests per second" would be invented. The interview line is: I will benchmark one key and many keys, with `b.RunParallel`, and I will paste the command output. I will not choose a stripe count before that.

### L13. No HTTP behavior

There is no 429, no `Retry-After` header, no proxy, no metrics. `Decision.RetryAfter` is the value those headers will use. Building the gateway before the counter is race-clean would test a server whose quota I do not trust.

---

## 5. What I say if they push

- "Is this production?" It is a correct single-process fixed window and a correct single-process sliding log, with tests. Production multi-instance needs the shared counter and a measured lock design. I can point at which of those is missing.
- "Why not atomics?" The bucket id and the count move together.
- "Why not `sync.Map`?" The hot path updates a counter. The map is a colder problem.
- "Where can this over-admit?" The fixed window, at the bucket boundary. The log does not, inside one process. Both do after a restart, and both do once per process if I run more than one process.
- "What will you build next?" The leaky bucket meter, behind the same `Allow`. Striped map locks only after a benchmark shows the map lock waiting. The gateway comes after the algorithm set is real.

---

## 6. Short glossary

| Word | Meaning in this code |
|---|---|
| Bucket | The half-open interval `[start, start+window)` where `start = now.Truncate(window)`. |
| Key | The caller id string. Empty is an error. The HTTP layer will pass an API key or an IP. |
| Critical section | The lines between `f.mu.Lock` and the unlock: read the clock, find or reset the counter, maybe increment. |
| Aligned window | Every caller and, later, every gateway computes the same `start` from the clock. |
| Decision | The whole answer: allowed or not, what remains, when to retry. |
