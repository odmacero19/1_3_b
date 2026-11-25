package tachiyomi.domain.sync.model

import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.manga.model.Manga

/**
 * Tables whose rows are tracked by the delta sync engine.
 * [tableName] matches the value stored in `sync_delta_queue.table_name`.
 */
enum class SyncTable(val tableName: String) {
    MANGAS("mangas"),
    CHAPTERS("chapters"),
}

/** Identifies one queue entry / local row. */
data class SyncRecordRef(
    val table: SyncTable,
    val recordId: Long,
)

/**
 * A manga row that changed, together with the names of the categories it belongs to.
 * Categories travel by name because category ids and ordering differ between devices.
 */
data class SyncedManga(
    val manga: Manga,
    val categoryNames: List<String>,
)

/**
 * A chapter row that changed. Local row ids don't mean anything on other devices, so the owning manga
 * is identified by its natural key ([mangaSource], [mangaUrl]) and the chapter by [Chapter.url].
 */
data class SyncedChapter(
    val mangaSource: Long,
    val mangaUrl: String,
    val chapter: Chapter,
)

/**
 * Snapshot of the rows behind a set of queue entries that were claimed for one upload attempt.
 *
 * @property batchId identifies the claimed entries; pass it back to complete or release them.
 * @property entryCount number of queue entries in the batch. May exceed the number of rows when some of
 * the queued rows have been deleted since.
 */
data class SyncDeltaBatch(
    val batchId: String,
    val entryCount: Int,
    val mangas: List<SyncedManga>,
    val chapters: List<SyncedChapter>,
) {
    val isEmpty: Boolean
        get() = mangas.isEmpty() && chapters.isEmpty()

    /** Local ids of the rows in this batch. */
    val refs: List<SyncRecordRef>
        get() = mangas.map { SyncRecordRef(SyncTable.MANGAS, it.manga.id) } +
            chapters.map { SyncRecordRef(SyncTable.CHAPTERS, it.chapter.id) }
}

/** Changes received from the remote side. */
data class RemoteDelta(
    val mangas: List<SyncedManga> = emptyList(),
    val chapters: List<SyncedChapter> = emptyList(),
)

data class ApplyResult(
    val mangasApplied: Int = 0,
    val chaptersApplied: Int = 0,
    /** Older than (or equal to) local state, or conflicting with an unsent local change. */
    val skipped: Int = 0,
    /** Chapters whose manga is not in the local database. */
    val orphaned: Int = 0,
) {
    val appliedCount: Int
        get() = mangasApplied + chaptersApplied
}
