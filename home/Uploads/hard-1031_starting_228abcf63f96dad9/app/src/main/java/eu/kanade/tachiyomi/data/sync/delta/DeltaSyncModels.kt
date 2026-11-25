package eu.kanade.tachiyomi.data.sync.delta

import eu.kanade.tachiyomi.data.backup.models.BackupChapter
import eu.kanade.tachiyomi.data.backup.models.BackupManga
import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoNumber

/**
 * Wire models of the delta sync protocol.
 *
 * Rows are carried as [BackupManga] / [BackupChapter], the same protobuf messages the backup format uses, so
 * both formats share field numbers and evolve together. Unlike a backup these messages only ever describe
 * rows that changed: a [DeltaManga] never embeds its chapters, chapters travel as separate [DeltaChapter]s.
 */

/** A manga whose row changed. [categoryNames] is the full set of categories it is in now. */
@Serializable
class DeltaManga(
    @ProtoNumber(1) val manga: BackupManga,
    @ProtoNumber(2) val categoryNames: List<String> = emptyList(),
)

/** A chapter whose row changed, addressed through its manga's natural key. */
@Serializable
class DeltaChapter(
    @ProtoNumber(1) val mangaSource: Long,
    @ProtoNumber(2) val mangaUrl: String,
    @ProtoNumber(3) val chapter: BackupChapter,
)

/** Body of a push request. [batchId] doubles as an idempotency key for the server. */
@Serializable
class DeltaSyncPayload(
    @ProtoNumber(1) val deviceId: String,
    @ProtoNumber(2) val batchId: String,
    @ProtoNumber(3) val mangas: List<DeltaManga> = emptyList(),
    @ProtoNumber(4) val chapters: List<DeltaChapter> = emptyList(),
)

/**
 * One record the server refused. [kind] is [KIND_MANGA] or [KIND_CHAPTER]; records are addressed by natural key
 * ([chapterUrl] is empty for manga).
 */
@Serializable
class DeltaRejection(
    @ProtoNumber(1) val kind: Int,
    @ProtoNumber(2) val mangaSource: Long,
    @ProtoNumber(3) val mangaUrl: String,
    @ProtoNumber(4) val chapterUrl: String = "",
    @ProtoNumber(5) val reason: String = "",
) {
    companion object {
        const val KIND_MANGA = 1
        const val KIND_CHAPTER = 2
    }
}

/**
 * Optional body of a successful push response. Everything in the batch that is not listed in [rejected] counts
 * as accepted; an empty body means the whole batch was accepted.
 */
@Serializable
class DeltaPushResponse(
    @ProtoNumber(1) val rejected: List<DeltaRejection> = emptyList(),
)

/**
 * Body of a pull response: one page of remote changes.
 *
 * @property cursor opaque position to send with the next pull request. Empty when the server has nothing new.
 * @property hasMore whether another page is available right now.
 */
@Serializable
class DeltaPullResponse(
    @ProtoNumber(1) val cursor: String = "",
    @ProtoNumber(2) val hasMore: Boolean = false,
    @ProtoNumber(3) val mangas: List<DeltaManga> = emptyList(),
    @ProtoNumber(4) val chapters: List<DeltaChapter> = emptyList(),
)
