# Delta sync harness

A small stand-alone Gradle build (not part of the Mihon build) that compiles the delta sync code against the
SQLDelight schema in `data/src/main/sqldelight` and runs it on an in-memory SQLite database, so the trigger /
queue / transaction behaviour can be checked on a desktop JVM without an Android SDK.

What is compiled is the real source, linked from `src/main/kotlin/real` (symlinks into `data`, `domain`, `app`,
`core`): `SyncDeltaRepositoryImpl`, `DeltaSyncEngine`, `HttpDeltaSyncService`, the wire models and mapper, plus
the SQLDelight-generated queries. Only Android-specific pieces are stubbed (`src/main/kotlin/stubs`).
`MangaBackupCreatorExtract.kt` is a verbatim copy of `Manga.toBackupManga()`; keep it in sync if that changes.

    cd tools/delta-sync-harness
    gradle test            # Gradle 8.x, JDK 21, needs Maven Central

The `mock_server` tests start `tools/delta-sync-mock-server/mock_server.py` (needs `python3`).

The SQL-only checks (triggers, queue queries, migration 14 vs. fresh schema) live in
`data/src/test/sql/test_delta_sync_sql.py` and need nothing but Python 3.
