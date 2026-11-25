package eu.kanade.tachiyomi.data.sync.delta

import eu.kanade.tachiyomi.data.backup.create.creators.toBackupManga
import eu.kanade.tachiyomi.data.backup.models.BackupChapter
import tachiyomi.data.MemoColumnAdapter
import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.sync.model.RemoteDelta
import tachiyomi.domain.sync.model.SyncedChapter
import tachiyomi.domain.sync.model.SyncedManga

fun DeltaPullResponse.toRemoteDelta(): RemoteDelta = RemoteDelta(
    mangas = mangas.map { SyncedManga(manga = it.manga.getMangaImpl(), categoryNames = it.categoryNames) },
    chapters = chapters.map {
        SyncedChapter(
            mangaSource = it.mangaSource,
            mangaUrl = it.mangaUrl,
            chapter = it.chapter.toChapterImpl(),
        )
    },
)

fun SyncedManga.toDelta() = DeltaManga(
    // Chapters, history and tracking are deliberately left out: a manga delta describes the manga row only
    manga = manga.toBackupManga(),
    categoryNames = categoryNames,
)

fun SyncedChapter.toDelta() = DeltaChapter(
    mangaSource = mangaSource,
    mangaUrl = mangaUrl,
    chapter = chapter.toBackupChapter(),
)

private fun Chapter.toBackupChapter() = BackupChapter(
    url = url,
    name = name,
    scanlator = scanlator,
    read = read,
    bookmark = bookmark,
    lastPageRead = lastPageRead,
    dateFetch = dateFetch,
    dateUpload = dateUpload,
    chapterNumber = chapterNumber.toFloat(),
    sourceOrder = sourceOrder,
    lastModifiedAt = lastModifiedAt,
    version = version,
    memo = MemoColumnAdapter.encode(memo),
)
