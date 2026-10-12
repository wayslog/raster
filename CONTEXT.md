# RasterKV domain terminology

RasterKV is a Rust implementation of FASTER that follows the FASTER C++ contract. Terms below match the upstream concepts.

## Language

**Store (RasterKV):** The complete keyspace, hash index, hybrid log, and checkpoint history. Do not call it a session.

**Session:** A thread's execution context against the store. It holds epoch protection, the serial number, the current phase and version, and its pending and retry queues. A session must call `refresh` periodically; an unrefreshed session blocks memory reclamation and phase changes.

**Serial number:** The monotonic operation identifier supplied by the caller within one session. CPR checkpoints record per-session serial numbers as the recovery boundary.

**Status:** The result of an operation: `Ok`, `Pending`, `NotFound`, `Aborted`, `OutOfMemory`, `IoError`, or `Corruption`. `Pending` means the operation context was copied into the session and completes later through `complete_pending`.

**Epoch protection:** The light-epoch mechanism that defers reclamation (page eviction, index growth, buffer reuse) until every protected session has moved past the epoch in which the resource was retired.

**Log address:** A 48-bit location in the hybrid-log address space, composed of a page number and an offset within the page. It is not a memory pointer.

**Hybrid log:** The address-ordered record space with a mutable in-memory tail, a read-only in-memory region, and a stable on-disk region. Boundaries are the begin, head, read-only, and tail addresses.

**Record:** An 8-byte `RecordInfo` header (previous address, checkpoint version, invalid, tombstone, final bits) followed by the inline key and value.

**In-place update:** An update applied directly to a record in the mutable region. Concurrency safety of in-place updates is the responsibility of the value type.

**Tombstone:** A record that marks a key as deleted and masks older records.

**CPR checkpoint:** A concurrent prefix-recovery checkpoint of the index, the hybrid log, or both, using the phases Prepare, InProgress, WaitPending, WaitFlush, PersistenceCallback, and Rest.

**Compaction:** Copying live records from the beginning of the log to the tail so that the log begin address can advance.

**Shift begin address:** Making the oldest part of the log invalid and reclaimable.
