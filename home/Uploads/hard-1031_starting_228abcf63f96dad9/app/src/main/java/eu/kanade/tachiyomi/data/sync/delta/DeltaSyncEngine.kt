package eu.kanade.tachiyomi.data.sync.delta

import eu.kanade.tachiyomi.network.HttpException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerializationException
import logcat.LogPriority
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.sync.model.ApplyResult
import tachiyomi.domain.sync.model.SyncRecordRef
import tachiyomi.domain.sync.model.SyncTable
import tachiyomi.domain.sync.model.SyncedChapter
import tachiyomi.domain.sync.model.SyncedManga
import tachiyomi.domain.sync.repository.SyncDeltaRepository
import tachiyomi.domain.sync.service.DeltaSyncPreferences
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * Drives incremental synchronisation: pushes locally queued changes and applies remote ones.
 *
 * Transactions are short and never span network I/O. A push is claim (transaction) → HTTP → complete or
 * release (transaction); a pull is HTTP → apply page and advance cursor (one transaction). If anything goes
 * wrong in between, the database is left in a state that the next run can pick up again.
 */
class DeltaSyncEngine(
    private val repository: SyncDeltaRepository,
    private val service: DeltaSyncService,
    private val preferences: DeltaSyncPreferences,
) {

    sealed interface PushResult {
        /** Nothing was waiting. */
        data object Idle : PushResult

        /** Everything that was queued when the run started has been delivered. */
        data class Drained(val entries: Int) : PushResult

        /** Sync is switched off or has no server configured; the queue is left alone. */
        data object Disabled : PushResult

        /** Stopped early, entries were kept for another attempt. */
        data class Retry(val entries: Int, val cause: Throwable?) : PushResult

        /** The server rejected some entries; they are parked and retried later, the rest was delivered. */
        data class PartiallyRejected(val delivered: Int, val rejected: Int) : PushResult
    }

    sealed interface PullResult {
        data object Disabled : PullResult
        data class Pulled(val applied: ApplyResult, val pages: Int) : PullResult
        data class Retry(val cause: Throwable) : PullResult
        data class Failed(val cause: Throwable) : PullResult
    }

    private val pushMutex = Mutex()
    private val pullMutex = Mutex()

    private fun isActive() = preferences.enabled.get() && service.isConfigured()

    /** Uploads queued changes in batches until the queue is empty or an upload fails. */
    suspend fun push(): PushResult = pushMutex.withLock {
        if (!isActive()) return@withLock PushResult.Disabled

        // Only this mutex holder ever has entries IN_FLIGHT, so anything still in that state is left over
        // from an upload that was cut short (process death, cancelled job) and has to be retried.
        repository.recoverQueue(failedOlderThanSeconds = FAILED_RETRY_DELAY_SECONDS)

        val deviceId = preferences.deviceId()
        var delivered = 0
        var rejected = 0

        repeat(MAX_BATCHES_PER_RUN) {
            val batch = repository.claimNextBatch(BATCH_SIZE)
                ?: return@withLock when {
                    rejected > 0 -> PushResult.PartiallyRejected(delivered, rejected)
                    delivered > 0 -> PushResult.Drained(delivered)
                    else -> PushResult.Idle
                }

            if (batch.isEmpty) {
                // Every queued row was deleted in the meantime, there is nothing to send
                repository.completeBatch(batch.batchId)
                return@repeat
            }

            val outcome = Delivery(batch.mangas.map { Item.of(it) } + batch.chapters.map { Item.of(it) })
                .also { it.send(deviceId, batch.batchId, it.items) }

            // One transaction for the whole batch: good records are removed, refused ones parked, transient
            // failures requeued. If this throws, the entries stay IN_FLIGHT and the next run re-sends them.
            repository.settleBatch(
                batchId = batch.batchId,
                rejected = outcome.rejected,
                retry = outcome.retry,
                retryError = outcome.transientError?.let { it.message ?: it::class.java.simpleName },
                maxAttempts = MAX_ATTEMPTS,
            )

            delivered += batch.entryCount - outcome.rejected.size - outcome.retry.size
            rejected += outcome.rejected.size

            if (outcome.transientError != null) return@withLock PushResult.Retry(delivered, outcome.transientError)
            if (outcome.fatal) return@withLock PushResult.PartiallyRejected(delivered, rejected)
        }

        // More than a run's worth of changes: come back for the rest
        PushResult.Retry(delivered, null)
    }

    private sealed class Item(val ref: SyncRecordRef) {
        class Manga(val value: SyncedManga) : Item(SyncRecordRef(SyncTable.MANGAS, value.manga.id))
        class Chapter(val value: SyncedChapter) : Item(SyncRecordRef(SyncTable.CHAPTERS, value.chapter.id))

        companion object {
            fun of(manga: SyncedManga): Item = Manga(manga)
            fun of(chapter: SyncedChapter): Item = Chapter(chapter)
        }
    }

    /**
     * Working state for delivering one batch. A record-level refusal only takes that record out of the batch:
     * either the server names it in its response, or, when it rejects the request as a whole with 400/413/422,
     * the batch is split in halves until the offending record stands alone.
     */
    private inner class Delivery(val items: List<Item>) {
        val rejected = mutableMapOf<SyncRecordRef, String>()
        val retry = mutableSetOf<SyncRecordRef>()
        var transientError: Throwable? = null
        var fatal = false

        suspend fun send(deviceId: String, batchId: String, subset: List<Item>, part: String = "") {
            if (subset.isEmpty()) return
            if (transientError != null || fatal) {
                // An earlier part of this batch already stopped the run; don't hammer the server
                retry += subset.map { it.ref }.filter { it !in rejected }
                return
            }

            val payload = DeltaSyncPayload(
                deviceId = deviceId,
                // Parts of a split batch need their own idempotency key
                batchId = batchId + part,
                mangas = subset.filterIsInstance<Item.Manga>().map { it.value.toDelta() },
                chapters = subset.filterIsInstance<Item.Chapter>().map { it.value.toDelta() },
            )

            try {
                val response = service.push(payload)

                val unmatched = mutableListOf<DeltaRejection>()
                response.rejected.forEach { refusal ->
                    val item = subset.firstOrNull { it.matches(refusal) }
                    if (item == null) unmatched += refusal else rejected[item.ref] = refusal.reason.ifBlank { "Rejected by server" }
                }

                if (unmatched.isNotEmpty()) {
                    // The server says something was refused but we cannot tell which of our records it means, so
                    // we cannot know what it actually stored. Treating the rest as delivered would drop edits
                    // that may never have synced; keep them queued and try again later. Attempts are counted,
                    // so a server that keeps doing this ends up parking them instead of looping.
                    val describe = unmatched.joinToString { "${it.mangaUrl}${it.chapterUrl}" }
                    logcat(LogPriority.WARN) { "Server rejected unknown record(s): $describe; keeping batch queued" }
                    transientError = IOException("Server rejected record(s) that were not in the request: $describe")
                    retry += subset.map { it.ref }.filter { it !in rejected }
                }
            } catch (e: CancellationException) {
                // Leave the entries IN_FLIGHT: the next run recovers them. A redelivery is harmless, versions
                // make applying a delta idempotent.
                throw e
            } catch (e: Exception) {
                when {
                    e.isRecordSpecific() && subset.size > 1 -> {
                        logcat(LogPriority.WARN, e) { "Server refused a batch of ${subset.size}, splitting" }
                        val half = subset.size / 2
                        send(deviceId, batchId, subset.subList(0, half), "$part.a")
                        send(deviceId, batchId, subset.subList(half, subset.size), "$part.b")
                    }
                    e.isRecordSpecific() -> {
                        logcat(LogPriority.WARN, e) { "Server refused a record" }
                        rejected[subset.single().ref] = e.message ?: "Rejected by server"
                    }
                    e.isPermanent() -> {
                        // e.g. 401/404: nothing to do with the records, everything would fail the same way
                        logcat(LogPriority.WARN, e) { "Delta push refused" }
                        fatal = true
                        subset.forEach { rejected[it.ref] = e.message ?: "Rejected by server" }
                    }
                    else -> {
                        logcat(LogPriority.WARN, e) { "Delta push failed, will retry" }
                        transientError = e
                        retry += subset.map { it.ref }
                    }
                }
            }
        }
    }

    private fun Item.matches(refusal: DeltaRejection): Boolean = when (this) {
        is Item.Manga -> refusal.kind == DeltaRejection.KIND_MANGA &&
            refusal.mangaSource == value.manga.source && refusal.mangaUrl == value.manga.url
        is Item.Chapter -> refusal.kind == DeltaRejection.KIND_CHAPTER &&
            refusal.mangaSource == value.mangaSource && refusal.mangaUrl == value.mangaUrl &&
            refusal.chapterUrl == value.chapter.url
    }

    /** Fetches remote changes page by page and applies each page atomically. */
    suspend fun pull(): PullResult = pullMutex.withLock {
        if (!isActive()) return@withLock PullResult.Disabled

        val deviceId = preferences.deviceId()
        var cursor = repository.getPullCursor()
        var total = ApplyResult()
        var pages = 0

        try {
            while (pages < MAX_PULL_PAGES) {
                val response = service.pull(deviceId, cursor, PULL_PAGE_SIZE)
                pages++

                val nextCursor = response.cursor.ifEmpty { null }
                val result = repository.applyRemoteDelta(response.toRemoteDelta(), nextCursor)
                total = ApplyResult(
                    mangasApplied = total.mangasApplied + result.mangasApplied,
                    chaptersApplied = total.chaptersApplied + result.chaptersApplied,
                    skipped = total.skipped + result.skipped,
                    orphaned = total.orphaned + result.orphaned,
                )

                if (nextCursor != null) cursor = nextCursor
                if (!response.hasMore) break
            }
            PullResult.Pulled(total, pages)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logcat(LogPriority.WARN, e) { "Delta pull failed after $pages page(s)" }
            // Pages applied so far are committed along with their cursor; the next run resumes after them
            if (e.isPermanent()) PullResult.Failed(e) else PullResult.Retry(e)
        }
    }

    /**
     * Network trouble and server-side errors are worth retrying. A 4xx (other than timeouts and throttling) or
     * a body that cannot be decoded will fail the same way next time.
     */
    /** The server objected to the content of the request, not to the request as such. */
    private fun Throwable.isRecordSpecific(): Boolean = this is HttpException && code in RECORD_SPECIFIC_ERRORS

    private fun Throwable.isPermanent(): Boolean = when (this) {
        is HttpException -> code !in RETRYABLE_CLIENT_ERRORS && code < 500
        is IOException -> false
        is SerializationException -> true
        is IllegalArgumentException -> true
        else -> false
    }

    companion object {
        const val BATCH_SIZE = 100
        const val MAX_BATCHES_PER_RUN = 50
        const val MAX_ATTEMPTS = 10
        const val PULL_PAGE_SIZE = 100
        const val MAX_PULL_PAGES = 50
        val FAILED_RETRY_DELAY_SECONDS = TimeUnit.HOURS.toSeconds(1)

        private val RETRYABLE_CLIENT_ERRORS = setOf(408, 425, 429)
        private val RECORD_SPECIFIC_ERRORS = setOf(400, 413, 422)
    }
}
