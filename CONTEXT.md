# RasterKV domain terminology

RasterKV is a Rust embedded key-value store that reproduces the functional and behavioral baseline of FASTER C++. Phase I uses an independent on-disk format. F2 and hot/cold tiering are deferred to a later phase.

## Language

**Storage instance (RasterKV):** The complete keyspace, index, log, and recovery history. Do not call it a session.

**Session:** An ordered stream of application operations with an independent identity and resumable progress. Do not use "affair" or "connection" for this concept.

**Operation number (serial):** The monotonic identifier supplied for a commit operation within one session. It is distinct from a log address, completion order, and global transaction number.

**Operation completion:** An operation has final results that the caller can collect. Completion does not imply persistence or checkpoint submission.

**Durable progress:** The session recovery boundary guaranteed by successful checkpoints. It is not simply the largest sequence number returned to the caller.

**Checkpoint:** Persistable index state, log state, or both, together with the recovery information that binds them.

**Recovery set:** A composable index checkpoint and log checkpoint plus the information required to verify that they match.

**Log address:** The location of a record in the hybrid-log address space. It is not a memory pointer or a count of valid keys.

**Hybrid log:** An address-ordered record space containing mutable in-memory records as well as stable log and disk history. It is not an append-only WAL abstraction.

**Tombstone:** A record that marks a key as deleted and masks older records. It does not mean that a physical file was deleted.

**Blind delete:** A delete that does not read old disk records to determine whether a live value existed. Success means that the delete took effect; the returned result still depends on discoverable index entries.

**Forced tombstone:** A delete that retains a queryable tombstone and keeps the corresponding index entry reachable. Later overwrites and explicit truncation still follow the normal lifecycle.

**Safe frontier:** The location after participating access has met the required safety conditions, so disk flush or recycling can proceed. Do not call this the target boundary.

**Compaction:** Identify and migrate records that must be retained in order to reduce expired history. It is not byte compression or unconditional truncation.

**Shift begin:** Make an old part of the log fall outside the valid address range. It does not automatically preserve the newest value in the truncated range.

**Record scan:** Traverse physical records by log address. It is not a current valid-key snapshot or a range-ordered query.
