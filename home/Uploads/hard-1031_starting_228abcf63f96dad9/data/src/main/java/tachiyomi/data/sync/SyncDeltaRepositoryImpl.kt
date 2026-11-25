package tachiyomi.data.sync

import app.cash.sqldelight.async.coroutines.awaitAsList
import app.cash.sqldelight.async.coroutines.awaitAsOne
import app.cash.sqldelight.async.coroutines.awaitAsOneOrNull
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.json.JsonObject
import tachiyomi.data.Database
import tachiyomi.data.MemoColumnAdapter
import tachiyomi.data.UpdateStrategyColumnAdapter
import tachiyomi.data.manga.MangaMapper
import tachiyomi.data.subscribeToOne
import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.manga.model.Manga
import tachiyomi.domain.sync.model.ApplyResult
import tachiyomi.domain.sync.model.RemoteDelta
import tachiyomi.domain.sync.model.SyncDeltaBatch
import tachiyomi.domain.sync.model.SyncRecordRef
import tachiyomi.domain.sync.model.SyncTable
import tachiyomi.domain.sync.model.SyncedChapter
import tachiyomi.domain.sync.model.SyncedManga
import tachiyomi.domain.sync.repository.SyncDeltaRepository
import java.util.UUID

class SyncDeltaRepositoryImpl(
    private val database: Database,
) : SyncDeltaRepository {

    private val queue get() = database.sync_delta_queueQueries

    override suspend fun claimNextBatch(limit: Int): SyncDeltaBatch? {
        require(limit > 0) { "limit must be positive" }
        val batchId = UUID.randomUUID().toString()

        // Claiming and loading share one transaction, so the snapshot always matches the entries: a row that
        // changes afterwards gets a fresh PENDING entry and is uploaded again by the next batch.
        return database.transactionWithResult {
            queue.claimBatch(batchId, limit.toLong())
            val claimed = queue.countClaimed(batchId).awaitAsOne()
            if (claimed == 0L) return@transactionWithResult null

            val categoryNames = queue.getClaimedMangaCategoryNames(batchId).awaitAsList()
                .groupBy({ it.manga_id }, { it.name })

            val mangas = queue.getClaimedMangas(batchId, MangaMapper::mapManga).awaitAsList()
                .map { SyncedManga(manga = it, categoryNames = categoryNames[it.id].orEmpty()) }

            val chapters = queue.getClaimedChapters(batchId, ::mapClaimedChapter).awaitAsList()

            SyncDeltaBatch(
                batchId = batchId,
                entryCount = claimed.toInt(),
                mangas = mangas,
                chapters = chapters,
            )
        }
    }

    override suspend fun completeBatch(batchId: String) {
        queue.completeBatch(batchId)
    }

    override suspend fun releaseBatch(batchId: String, error: String?, permanent: Boolean, maxAttempts: Int) {
        database.transaction {
            queue.releaseBatch(
                permanent = if (permanent) 1L else 0L,
                maxAttempts = maxAttempts.toLong(),
                error = error?.take(MAX_ERROR_LENGTH),
                batchId = batchId,
            )
            // Entries left over were superseded by a newer PENDING entry for the same record
            queue.completeBatch(batchId)
        }
    }

    override suspend fun settleBatch(
        batchId: String,
        rejected: Map<SyncRecordRef, String>,
        retry: Set<SyncRecordRef>,
        retryError: String?,
        maxAttempts: Int,
    ) {
        database.transaction {
            rejected.forEach { (ref, message) ->
                queue.releaseRecord(
                    permanent = 1L,
                    maxAttempts = maxAttempts.toLong(),
                    error = message.take(MAX_ERROR_LENGTH),
                    batchId = batchId,
                    tableName = ref.table.tableName,
                    recordId = ref.recordId,
                )
            }
            retry.forEach { ref ->
                queue.releaseRecord(
                    permanent = 0L,
                    maxAttempts = maxAttempts.toLong(),
                    error = retryError?.take(MAX_ERROR_LENGTH),
                    batchId = batchId,
                    tableName = ref.table.tableName,
                    recordId = ref.recordId,
                )
            }
            // Whatever is still IN_FLIGHT was delivered (or superseded by a newer PENDING entry)
            queue.completeBatch(batchId)
        }
    }

    override suspend fun recoverQueue(failedOlderThanSeconds: Long) {
        val olderThan = System.currentTimeMillis() / 1000 - failedOlderThanSeconds
        database.transaction {
            queue.recoverInFlight()
            queue.dropRecoveredLeftovers()
            queue.requeueFailed(olderThan)
            queue.dropRequeuedLeftovers(olderThan)
        }
    }

    override suspend fun getPendingCount(): Long {
        return queue.countPending().awaitAsOne()
    }

    override fun getPendingCountAsFlow(): Flow<Long> {
        return queue.countPending().subscribeToOne()
    }

    override suspend fun enqueue(table: String, recordId: Long) {
        require(SyncTable.entries.any { it.tableName == table }) { "Unsupported table: $table" }
        queue.enqueue(table, recordId)
    }

    override suspend fun getPullCursor(): String? {
        return queue.getState(PULL_CURSOR_KEY).awaitAsOneOrNull()
    }

    override suspend fun resetSyncingFlags() {
        database.transaction {
            database.mangasQueries.resetIsSyncing()
            database.chaptersQueries.resetIsSyncing()
        }
    }

    override suspend fun applyRemoteDelta(delta: RemoteDelta, cursor: String?): ApplyResult {
        return database.transactionWithResult {
            val categoryIdsByName = queue.getCategoryIdsAndNames().awaitAsList()
                .associate { it.name to it._id }

            var mangasApplied = 0
            var chaptersApplied = 0
            var skipped = 0
            var orphaned = 0

            // Manga first, so that chapters that arrive in the same page find their manga
            delta.mangas.forEach { remote ->
                if (applyManga(remote, categoryIdsByName)) mangasApplied++ else skipped++
            }

            delta.chapters.forEach { remote ->
                when (applyChapter(remote)) {
                    Outcome.APPLIED -> chaptersApplied++
                    Outcome.SKIPPED -> skipped++
                    Outcome.ORPHANED -> orphaned++
                }
            }

            // Same transaction as the data: the cursor never gets ahead of what was applied
            if (cursor != null) {
                queue.setState(PULL_CURSOR_KEY, cursor)
            }

            ApplyResult(
                mangasApplied = mangasApplied,
                chaptersApplied = chaptersApplied,
                skipped = skipped,
                orphaned = orphaned,
            )
        }
    }

    /**
     * Every write below happens while `is_syncing = 1` on the row, which makes the version triggers ignore it,
     * so remote data is neither version-bumped nor queued for upload again. The flag is lowered before the
     * transaction ends (and rolled back with it should anything throw).
     *
     * @return true when the remote state was applied.
     */
    private suspend fun applyManga(remote: SyncedManga, categoryIdsByName: Map<String, Long>): Boolean {
        val incoming = remote.manga
        val local = database.mangasQueries
            .getMangaByUrlAndSource(incoming.url, incoming.source, MangaMapper::mapManga)
            .awaitAsOneOrNull()

        val mangaId: Long
        if (local == null) {
            mangaId = database.mangasQueries.insertReturningId(
                source = incoming.source,
                url = incoming.url,
                artist = incoming.artist,
                author = incoming.author,
                description = incoming.description,
                genre = incoming.genre,
                title = incoming.title,
                status = incoming.status,
                thumbnailUrl = incoming.thumbnailUrl,
                favorite = incoming.favorite,
                lastUpdate = 0L,
                nextUpdate = 0L,
                calculateInterval = 0L,
                initialized = incoming.initialized,
                viewerFlags = incoming.viewerFlags,
                chapterFlags = incoming.chapterFlags,
                coverLastModified = 0L,
                dateAdded = incoming.dateAdded,
                updateStrategy = incoming.updateStrategy,
                version = incoming.version,
                notes = incoming.notes,
                memo = incoming.memo,
            ).awaitAsOne()
            queue.setMangaSyncing(1L, mangaId)
        } else {
            mangaId = local.id
            if (!shouldApply(incoming.version, local.version, SyncTable.MANGAS, mangaId)) return false

            database.mangasQueries.update(
                source = null,
                url = null,
                artist = incoming.artist,
                author = incoming.author,
                description = incoming.description,
                genre = incoming.genre?.joinToString(separator = ", "),
                title = incoming.title,
                status = incoming.status,
                thumbnailUrl = incoming.thumbnailUrl,
                favorite = incoming.favorite,
                lastUpdate = null,
                nextUpdate = null,
                calculateInterval = null,
                initialized = incoming.initialized || local.initialized,
                viewer = incoming.viewerFlags,
                chapterFlags = incoming.chapterFlags,
                coverLastModified = null,
                dateAdded = incoming.dateAdded,
                mangaId = mangaId,
                updateStrategy = UpdateStrategyColumnAdapter.encode(incoming.updateStrategy),
                version = incoming.version,
                isSyncing = 1L,
                notes = incoming.notes,
                memo = MemoColumnAdapter.encode(incoming.memo),
            )
        }

        // Category membership is replaced wholesale; the insert trigger also honours the manga's flag
        val categoryIds = remote.categoryNames.mapNotNull(categoryIdsByName::get).distinct()
        database.mangas_categoriesQueries.deleteMangaCategoryByMangaId(mangaId)
        categoryIds.forEach { database.mangas_categoriesQueries.insert(mangaId, it) }

        queue.setMangaSyncing(0L, mangaId)
        return true
    }

    private suspend fun applyChapter(remote: SyncedChapter): Outcome {
        val mangaId = queue.getMangaIdBySourceAndUrl(remote.mangaSource, remote.mangaUrl).awaitAsOneOrNull()
            ?: return Outcome.ORPHANED
        val incoming = remote.chapter

        val local = database.chaptersQueries
            .getChapterByUrlAndMangaId(incoming.url, mangaId, ::mapChapter)
            .awaitAsOneOrNull()

        if (local == null) {
            database.chaptersQueries.insertReturningId(
                mangaId = mangaId,
                url = incoming.url,
                name = incoming.name,
                scanlator = incoming.scanlator,
                read = incoming.read,
                bookmark = incoming.bookmark,
                lastPageRead = incoming.lastPageRead,
                chapterNumber = incoming.chapterNumber,
                sourceOrder = incoming.sourceOrder,
                dateFetch = incoming.dateFetch,
                dateUpload = incoming.dateUpload,
                version = incoming.version,
                memo = incoming.memo,
            ).awaitAsOne()
            return Outcome.APPLIED
        }

        if (!shouldApply(incoming.version, local.version, SyncTable.CHAPTERS, local.id)) return Outcome.SKIPPED

        database.chaptersQueries.update(
            mangaId = null,
            url = null,
            name = null,
            scanlator = null,
            read = incoming.read,
            bookmark = incoming.bookmark,
            lastPageRead = incoming.lastPageRead,
            chapterNumber = null,
            sourceOrder = null,
            dateFetch = null,
            dateUpload = null,
            chapterId = local.id,
            version = incoming.version,
            isSyncing = 1L,
            memo = MemoColumnAdapter.encode(incoming.memo),
        )
        queue.setChapterSyncing(0L, local.id)
        return Outcome.APPLIED
    }

    /**
     * Versions order changes to the same row. A remote change is applied only when it is newer than the local
     * row and the local row has no change of its own waiting to be uploaded; in that case the local edit is
     * kept and reaches the server with the next push, where it can be reconciled.
     */
    private suspend fun shouldApply(
        remoteVersion: Long,
        localVersion: Long,
        table: SyncTable,
        recordId: Long,
    ): Boolean {
        if (remoteVersion <= localVersion) return false
        return queue.countQueued(table.tableName, recordId).awaitAsOne() == 0L
    }

    private enum class Outcome { APPLIED, SKIPPED, ORPHANED }

    @Suppress("UNUSED_PARAMETER")
    private fun mapClaimedChapter(
        id: Long,
        mangaId: Long,
        url: String,
        name: String,
        scanlator: String?,
        read: Boolean,
        bookmark: Boolean,
        lastPageRead: Long,
        chapterNumber: Double,
        sourceOrder: Long,
        dateFetch: Long,
        dateUpload: Long,
        lastModifiedAt: Long,
        version: Long,
        isSyncing: Long,
        memo: JsonObject,
        mangaSource: Long,
        mangaUrl: String,
    ): SyncedChapter = SyncedChapter(
        mangaSource = mangaSource,
        mangaUrl = mangaUrl,
        chapter = mapChapter(
            id, mangaId, url, name, scanlator, read, bookmark, lastPageRead, chapterNumber, sourceOrder,
            dateFetch, dateUpload, lastModifiedAt, version, isSyncing, memo,
        ),
    )

    @Suppress("UNUSED_PARAMETER")
    private fun mapChapter(
        id: Long,
        mangaId: Long,
        url: String,
        name: String,
        scanlator: String?,
        read: Boolean,
        bookmark: Boolean,
        lastPageRead: Long,
        chapterNumber: Double,
        sourceOrder: Long,
        dateFetch: Long,
        dateUpload: Long,
        lastModifiedAt: Long,
        version: Long,
        isSyncing: Long,
        memo: JsonObject,
    ): Chapter = Chapter(
        id = id,
        mangaId = mangaId,
        read = read,
        bookmark = bookmark,
        lastPageRead = lastPageRead,
        dateFetch = dateFetch,
        sourceOrder = sourceOrder,
        url = url,
        name = name,
        dateUpload = dateUpload,
        chapterNumber = chapterNumber,
        scanlator = scanlator,
        lastModifiedAt = lastModifiedAt,
        version = version,
        memo = memo,
    )

    private companion object {
        const val PULL_CURSOR_KEY = "delta_pull_cursor"
        const val MAX_ERROR_LENGTH = 500
    }
}
