package eu.kanade.tachiyomi.data.backup.create.creators
import eu.kanade.tachiyomi.data.backup.models.BackupManga
import eu.kanade.tachiyomi.ui.reader.setting.ReadingMode
import tachiyomi.data.MemoColumnAdapter
import tachiyomi.domain.manga.model.Manga

internal fun Manga.toBackupManga() =
    BackupManga(
        url = this.url,
        title = this.title,
        artist = this.artist,
        author = this.author,
        description = this.description,
        genre = this.genre.orEmpty(),
        status = this.status.toInt(),
        thumbnailUrl = this.thumbnailUrl,
        favorite = this.favorite,
        source = this.source,
        dateAdded = this.dateAdded,
        viewer = (this.viewerFlags.toInt() and ReadingMode.MASK),
        viewer_flags = this.viewerFlags.toInt(),
        chapterFlags = this.chapterFlags.toInt(),
        updateStrategy = this.updateStrategy,
        lastModifiedAt = this.lastModifiedAt,
        favoriteModifiedAt = this.favoriteModifiedAt,
        version = this.version,
        notes = this.notes,
        initialized = this.initialized,
        memo = MemoColumnAdapter.encode(this.memo),
    )
