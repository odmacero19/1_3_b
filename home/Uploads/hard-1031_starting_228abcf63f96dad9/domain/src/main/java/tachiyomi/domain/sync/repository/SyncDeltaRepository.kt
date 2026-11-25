package tachiyomi.domain.sync.repository

import kotlinx.coroutines.flow.Flow
import tachiyomi.domain.sync.model.ApplyResult
import tachiyomi.domain.sync.model.RemoteDelta
import tachiyomi.domain.sync.model.SyncDeltaBatch
import tachiyomi.domain.sync.model.SyncRecordRef

/**
 * Persistence side of the delta sync engine.
 *
 * Every function that changes more than one row runs in a single database transaction, and no function
 * keeps a transaction open across network I/O: a push is `claimNextBatch` → upload → `completeBatch` /
 * `releaseBatch`, each its own short transaction.
 */
interface SyncDeltaRepository {

    /**
     * Atomically moves up to [limit] PENDING queue entries to IN_FLIGHT and loads only the rows they refer to.
     * Returns null when nothing is queued.
     */
    suspend fun claimNextBatch(limit: Int): SyncDeltaBatch?

    /** The batch was accepted by the server: forget its entries. */
    suspend fun completeBatch(batchId: String)

    /**
     * Records the per-record outcome of a batch in one transaction, so that one bad record does not hold
     * back the others:
     *  - [rejected]: the server refused this record; it is parked as FAILED with the given message
     *  - [retry]: not delivered for a transient reason; goes back to PENDING (FAILED after `maxAttempts`)
     *  - every other entry of the batch was delivered and is removed
     * Records edited again while in flight keep their newer PENDING entry.
     */
    suspend fun settleBatch(
        batchId: String,
        rejected: Map<SyncRecordRef, String>,
        retry: Set<SyncRecordRef>,
        retryError: String?,
        maxAttempts: Int = 10,
    )

    /**
     * The batch was not delivered. Entries go back to PENDING so the next run retries them, or to FAILED once
     * [permanent] is set or they exhausted [maxAttempts]. Changes made while the batch was in flight are kept.
     */
    suspend fun releaseBatch(batchId: String, error: String?, permanent: Boolean = false, maxAttempts: Int = 10)

    /**
     * Returns entries stuck IN_FLIGHT (process died mid-upload) to PENDING, and retries FAILED ones that have been
     * parked for more than [failedOlderThanSeconds].
     */
    suspend fun recoverQueue(failedOlderThanSeconds: Long)

    suspend fun getPendingCount(): Long

    fun getPendingCountAsFlow(): Flow<Long>

    /** Adds a record to the queue by hand; a no-op when it is already queued. */
    suspend fun enqueue(table: String, recordId: Long)

    /**
     * Applies remote changes and stores [cursor] in one transaction, with `is_syncing` raised on every touched
     * row so the version triggers do not mistake them for local edits. Either all of it is applied or none.
     */
    suspend fun applyRemoteDelta(delta: RemoteDelta, cursor: String?): ApplyResult

    suspend fun getPullCursor(): String?

    /** Lowers any `is_syncing` flag left behind by an interrupted write. */
    suspend fun resetSyncingFlags()
}
