import app.cash.sqldelight.async.coroutines.awaitAsList
import app.cash.sqldelight.async.coroutines.awaitAsOne
import app.cash.sqldelight.async.coroutines.awaitAsOneOrNull
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import com.sun.net.httpserver.HttpServer
import eu.kanade.tachiyomi.data.backup.models.BackupChapter
import eu.kanade.tachiyomi.data.backup.models.BackupManga
import eu.kanade.tachiyomi.data.sync.delta.DeltaChapter
import eu.kanade.tachiyomi.data.sync.delta.DeltaManga
import eu.kanade.tachiyomi.data.sync.delta.DeltaPullResponse
import eu.kanade.tachiyomi.data.sync.delta.DeltaPushResponse
import eu.kanade.tachiyomi.data.sync.delta.DeltaRejection
import eu.kanade.tachiyomi.data.sync.delta.DeltaSyncEngine
import eu.kanade.tachiyomi.data.sync.delta.DeltaSyncPayload
import eu.kanade.tachiyomi.data.sync.delta.DeltaSyncService
import eu.kanade.tachiyomi.data.sync.delta.HttpDeltaSyncService
import eu.kanade.tachiyomi.network.HttpException
import eu.kanade.tachiyomi.network.NetworkHelper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.protobuf.ProtoBuf
import tachiyomi.core.common.preference.Preference
import tachiyomi.core.common.preference.PreferenceStore
import tachiyomi.data.Chapters
import tachiyomi.data.Database
import tachiyomi.data.Mangas
import tachiyomi.data.MemoColumnAdapter
import tachiyomi.data.StringListColumnAdapter
import tachiyomi.data.UpdateStrategyColumnAdapter
import tachiyomi.data.History
import tachiyomi.data.DateColumnAdapter
import tachiyomi.data.sync.SyncDeltaRepositoryImpl
import tachiyomi.domain.sync.model.RemoteDelta
import tachiyomi.domain.sync.model.SyncedChapter
import tachiyomi.domain.sync.model.SyncedManga
import tachiyomi.domain.sync.service.DeltaSyncPreferences
import java.net.InetSocketAddress
import java.util.Properties
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.assertIs
import kotlin.test.assertNull

class MemPreference<T>(private val k: String, default: T) : Preference<T> {
    private val state = MutableStateFlow(default)
    private val def = default
    override fun key() = k
    override fun get() = state.value
    override fun set(value: T) { state.value = value }
    override fun isSet() = state.value != def
    override fun delete() { state.value = def }
    override fun defaultValue() = def
    override fun changes(): Flow<T> = state
    override fun stateIn(scope: CoroutineScope) = throw UnsupportedOperationException()
}

class MemStore : PreferenceStore {
    override fun getString(key: String, defaultValue: String) = MemPreference(key, defaultValue)
    override fun getLong(key: String, defaultValue: Long) = MemPreference(key, defaultValue)
    override fun getInt(key: String, defaultValue: Int) = MemPreference(key, defaultValue)
    override fun getFloat(key: String, defaultValue: Float) = MemPreference(key, defaultValue)
    override fun getBoolean(key: String, defaultValue: Boolean) = MemPreference(key, defaultValue)
    override fun getStringSet(key: String, defaultValue: Set<String>) = MemPreference(key, defaultValue)
    override fun <T> getObjectFromString(key: String, defaultValue: T, serializer: (T) -> String, deserializer: (String) -> T) = MemPreference(key, defaultValue)
    override fun <T> getObjectFromInt(key: String, defaultValue: T, serializer: (T) -> Int, deserializer: (Int) -> T) = MemPreference(key, defaultValue)
    override fun <T> getObjectSetFromStringSet(key: String, defaultValue: Set<T>, serializer: (T) -> String, deserializer: (String) -> T?) = MemPreference(key, defaultValue)
    override fun getAll(): Map<String, *> = emptyMap<String, Any>()
}

class FakeService : DeltaSyncService {
    val pushed = mutableListOf<DeltaSyncPayload>()
    var pushFailure: Throwable? = null
    var failNextPushes = 0
    var pullPages = ArrayDeque<DeltaPullResponse>()
    var pullCursors = mutableListOf<String?>()
    override fun isConfigured() = true
    var rejectUrlsContaining: String? = null
    override suspend fun push(payload: DeltaSyncPayload): DeltaPushResponse {
        if (failNextPushes > 0) { failNextPushes--; throw pushFailure!! }
        pushed += payload
        return DeltaPushResponse()
    }
    override suspend fun pull(deviceId: String, cursor: String?, limit: Int): DeltaPullResponse {
        pullCursors += cursor
        return pullPages.removeFirstOrNull() ?: DeltaPullResponse()
    }
}

class Env(serviceOverride: DeltaSyncService? = null, val prefs: DeltaSyncPreferences = DeltaSyncPreferences(MemStore())) {
    val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY, Properties().apply { put("foreign_keys", "true") })
    val db: Database
    init {
        runBlocking { Database.Schema.create(driver).await() }
        db = Database(
            driver = driver,
            historyAdapter = History.Adapter(last_readAdapter = DateColumnAdapter),
            mangasAdapter = Mangas.Adapter(
                genreAdapter = StringListColumnAdapter,
                update_strategyAdapter = UpdateStrategyColumnAdapter,
                memoAdapter = MemoColumnAdapter,
            ),
            chaptersAdapter = Chapters.Adapter(memoAdapter = MemoColumnAdapter),
        )
    }
    val repo = SyncDeltaRepositoryImpl(db)
    val service = FakeService()
    val engine = DeltaSyncEngine(repo, serviceOverride ?: service, prefs)

    fun exec(sql: String) = driver.execute(null, sql, 0).value
    fun <T> query(sql: String, map: (app.cash.sqldelight.db.SqlCursor) -> T): List<T> =
        driver.executeQuery(null, sql, { c ->
            val out = mutableListOf<T>()
            while (c.next().value) out += map(c)
            app.cash.sqldelight.db.QueryResult.Value(out)
        }, 0).value

    fun addManga(url: String, source: Long = 1, favorite: Boolean = false, version: Long = 0): Long {
        exec("INSERT INTO mangas(source,url,title,status,favorite,initialized,viewer,chapter_flags,cover_last_modified,date_added,version) VALUES($source,'$url','t-$url',0,${if (favorite) 1 else 0},1,0,0,0,0,$version)")
        return query("SELECT max(_id) FROM mangas") { it.getLong(0)!! }.single()
    }
    fun addChapter(mangaId: Long, url: String, version: Long = 0): Long {
        exec("INSERT INTO chapters(manga_id,url,name,read,bookmark,last_page_read,chapter_number,source_order,date_fetch,date_upload,version) VALUES($mangaId,'$url','n-$url',0,0,0,1.0,0,0,0,$version)")
        return query("SELECT max(_id) FROM chapters") { it.getLong(0)!! }.single()
    }
    fun queue() = query("SELECT table_name, record_id, status, attempt_count FROM sync_delta_queue ORDER BY _id") {
        listOf(it.getString(0), it.getLong(1), it.getString(2), it.getLong(3))
    }
    fun value(sql: String) = query(sql) { it.getLong(0) }.single()
}

private fun backupManga(url: String, source: Long = 1, version: Long, favorite: Boolean = true, description: String? = null) =
    BackupManga(source = source, url = url, title = "remote-$url", favorite = favorite, version = version, description = description)

private fun backupChapter(url: String, version: Long, read: Boolean = true, page: Long = 0) =
    BackupChapter(url = url, name = "remote-$url", read = read, lastPageRead = page, version = version)

class DeltaSyncIntegrationTest {

    @Test
    fun `local edits are queued by triggers and pushed as a delta of only the changed rows`() = runBlocking {
        val e = Env()
        val m1 = e.addManga("/changed")
        val m2 = e.addManga("/untouched")
        val c1 = e.addChapter(m1, "/changed/c1")
        e.addChapter(m1, "/changed/c2")
        e.addChapter(m2, "/untouched/c1")
        assertTrue(e.queue().isEmpty())

        e.exec("UPDATE chapters SET read = 1, last_page_read = 4 WHERE _id = $c1")
        e.exec("UPDATE mangas SET favorite = 1 WHERE _id = $m1")
        assertEquals(2, e.queue().size)

        val result = e.engine.push()
        assertEquals(DeltaSyncEngine.PushResult.Drained(2), result)

        val payload = e.service.pushed.single()
        assertEquals(listOf("/changed"), payload.mangas.map { it.manga.url })
        assertEquals(listOf("/changed/c1"), payload.chapters.map { it.chapter.url })
        assertEquals(true, payload.chapters.single().chapter.read)
        assertEquals(4L, payload.chapters.single().chapter.lastPageRead)
        assertEquals("/changed", payload.chapters.single().mangaUrl)
        assertTrue(payload.mangas.single().manga.chapters.isEmpty(), "manga delta must not embed chapters")
        assertTrue(e.queue().isEmpty())
        assertEquals(DeltaSyncEngine.PushResult.Idle, e.engine.push())
    }

    @Test
    fun `payload survives a protobuf round trip`() = runBlocking {
        val e = Env()
        val m = e.addManga("/m")
        e.exec("UPDATE mangas SET favorite = 1, description = 'hi' WHERE _id = $m")
        e.engine.push()
        val proto = ProtoBuf
        val bytes = proto.encodeToByteArray(DeltaSyncPayload.serializer(), e.service.pushed.single())
        val back = proto.decodeFromByteArray(DeltaSyncPayload.serializer(), bytes)
        assertEquals("hi", back.mangas.single().manga.description)
        assertEquals(true, back.mangas.single().manga.favorite)
        assertEquals(1L, back.mangas.single().manga.version)
    }

    @Test
    fun `transient failure keeps the queue intact and the retry delivers it`() = runBlocking {
        val e = Env()
        val m = e.addManga("/m")
        e.exec("UPDATE mangas SET favorite = 1 WHERE _id = $m")
        e.service.pushFailure = HttpException(503)
        e.service.failNextPushes = 1

        val first = e.engine.push()
        assertIs<DeltaSyncEngine.PushResult.Retry>(first)
        assertEquals(listOf(listOf("mangas", m, "PENDING", 1L)), e.queue())
        assertTrue(e.service.pushed.isEmpty())

        // edited again while waiting: still a single entry
        e.exec("UPDATE mangas SET description = 'x' WHERE _id = $m")
        assertEquals(1, e.queue().size)

        assertEquals(DeltaSyncEngine.PushResult.Drained(1), e.engine.push())
        assertEquals("x", e.service.pushed.single().mangas.single().manga.description)
        assertTrue(e.queue().isEmpty())
    }

    @Test
    fun `io failure is retried and rejected batches do not block the queue`() = runBlocking {
        val e = Env()
        val m = e.addManga("/m")
        e.exec("UPDATE mangas SET favorite = 1 WHERE _id = $m")
        e.service.pushFailure = java.io.IOException("offline")
        e.service.failNextPushes = 1
        assertIs<DeltaSyncEngine.PushResult.Retry>(e.engine.push())
        assertEquals("PENDING", e.queue().single()[2])

        e.service.pushFailure = HttpException(400)
        e.service.failNextPushes = 1
        val r = e.engine.push()
        assertEquals(DeltaSyncEngine.PushResult.PartiallyRejected(0, 1), r)
        assertEquals("FAILED", e.queue().single()[2])

        // parked entries stay parked for an hour, then come back
        assertEquals(DeltaSyncEngine.PushResult.Idle, e.engine.push())
        e.exec("UPDATE sync_delta_queue SET updated_at = updated_at - 7200 WHERE status = 'FAILED'")
        assertEquals(DeltaSyncEngine.PushResult.Drained(1), e.engine.push())
    }

    @Test
    fun `entries left in flight by a dead process are recovered`() = runBlocking {
        val e = Env()
        val m = e.addManga("/m")
        e.exec("UPDATE mangas SET favorite = 1 WHERE _id = $m")
        val batch = e.repo.claimNextBatch(10)
        assertNotNull(batch)
        assertEquals("IN_FLIGHT", e.queue().single()[2])
        // ... process dies here; a new run starts
        assertEquals(DeltaSyncEngine.PushResult.Drained(1), e.engine.push())
        assertTrue(e.queue().isEmpty())
    }

    @Test
    fun `remote deltas are applied without being queued again`() = runBlocking {
        val e = Env()
        val m = e.addManga("/m", favorite = false, version = 1)
        e.exec("INSERT INTO categories(name,sort,flags) VALUES('Reading',1,0)")
        val c = e.addChapter(m, "/m/c1", version = 0)

        val delta = RemoteDelta(
            mangas = listOf(
                SyncedManga(backupManga("/m", version = 5, description = "remote desc").getMangaImpl(), listOf("Reading", "Unknown")),
                SyncedManga(backupManga("/new", version = 2).getMangaImpl(), emptyList()),
            ),
            chapters = listOf(
                SyncedChapter(1, "/m", backupChapter("/m/c1", version = 3, page = 9).toChapterImpl()),
                SyncedChapter(1, "/m", backupChapter("/m/c2", version = 1).toChapterImpl()),
                SyncedChapter(1, "/gone", backupChapter("/gone/c1", version = 1).toChapterImpl()),
            ),
        )
        val result = e.repo.applyRemoteDelta(delta, cursor = "42")

        assertEquals(2, result.mangasApplied)
        assertEquals(2, result.chaptersApplied)
        assertEquals(1, result.orphaned)
        assertTrue(e.queue().isEmpty(), "applying remote data must not queue it for upload: ${e.queue()}")
        assertEquals(5L, e.value("SELECT version FROM mangas WHERE _id = $m"))
        assertEquals(1L, e.value("SELECT favorite FROM mangas WHERE _id = $m"))
        assertEquals(3L, e.value("SELECT version FROM chapters WHERE _id = $c"))
        assertEquals(9L, e.value("SELECT last_page_read FROM chapters WHERE _id = $c"))
        assertEquals(1L, e.value("SELECT count(*) FROM chapters WHERE url = '/m/c2'"))
        assertEquals(1L, e.value("SELECT count(*) FROM mangas_categories WHERE manga_id = $m"))
        assertEquals(0L, e.value("SELECT count(*) FROM mangas WHERE is_syncing = 1"))
        assertEquals(0L, e.value("SELECT count(*) FROM chapters WHERE is_syncing = 1"))
        assertEquals("42", e.repo.getPullCursor())

        // and a subsequent push has nothing to do: no ping-pong
        assertEquals(DeltaSyncEngine.PushResult.Idle, e.engine.push())

        // local edits still work afterwards
        e.exec("UPDATE chapters SET bookmark = 1 WHERE _id = $c")
        assertEquals(2, e.queue().size)
        assertEquals(4L, e.value("SELECT version FROM chapters WHERE _id = $c"))
    }

    @Test
    fun `stale remote data and conflicts with unsent local edits are skipped`() = runBlocking {
        val e = Env()
        val m = e.addManga("/m", version = 7)
        val c = e.addChapter(m, "/m/c1", version = 0)
        e.exec("UPDATE chapters SET read = 1 WHERE _id = $c")   // local, unsent; version 1, queue: chapter + manga

        val delta = RemoteDelta(
            mangas = listOf(
                SyncedManga(backupManga("/m", version = 3).getMangaImpl(), emptyList()),  // older
                SyncedManga(backupManga("/m", version = 9).getMangaImpl(), emptyList()),  // newer but local manga is dirty
            ),
            chapters = listOf(SyncedChapter(1, "/m", backupChapter("/m/c1", version = 5, read = false).toChapterImpl())),
        )
        val result = e.repo.applyRemoteDelta(delta, "7")
        assertEquals(0, result.appliedCount)
        assertEquals(3, result.skipped)
        assertEquals(1L, e.value("SELECT read FROM chapters WHERE _id = $c"))
        assertEquals(2, e.queue().size)
    }

    @Test
    fun `a failure while applying rolls back the whole page including the cursor`() = runBlocking {
        val e = Env()
        val m = e.addManga("/m", version = 1)
        e.repo.applyRemoteDelta(RemoteDelta(), "1")
        e.exec("CREATE TRIGGER boom BEFORE INSERT ON chapters BEGIN SELECT RAISE(ABORT, 'boom'); END")

        val delta = RemoteDelta(
            mangas = listOf(SyncedManga(backupManga("/m", version = 5, favorite = true).getMangaImpl(), emptyList())),
            chapters = listOf(SyncedChapter(1, "/m", backupChapter("/m/new", version = 1).toChapterImpl())),
        )
        assertFailsWith<Exception> { e.repo.applyRemoteDelta(delta, "2") }

        assertEquals(1L, e.value("SELECT version FROM mangas WHERE _id = $m"))
        assertEquals(0L, e.value("SELECT favorite FROM mangas WHERE _id = $m"))
        assertEquals(0L, e.value("SELECT is_syncing FROM mangas WHERE _id = $m"))
        assertEquals("1", e.repo.getPullCursor())
        assertTrue(e.queue().isEmpty())
    }

    @Test
    fun `engine pulls page by page and resumes from the stored cursor`() = runBlocking {
        val e = Env()
        e.addManga("/m", version = 0)
        e.service.pullPages += DeltaPullResponse(cursor = "a", hasMore = true, mangas = listOf(DeltaManga(backupManga("/m", version = 1, favorite = true))))
        e.service.pullPages += DeltaPullResponse(cursor = "b", hasMore = false, chapters = listOf(DeltaChapter(1, "/m", backupChapter("/m/c1", version = 1))))

        val r = e.engine.pull()
        assertIs<DeltaSyncEngine.PullResult.Pulled>(r)
        assertEquals(2, r.pages)
        assertEquals(2, r.applied.appliedCount)
        assertEquals(listOf<String?>(null, "a"), e.service.pullCursors)
        assertEquals("b", e.repo.getPullCursor())
        assertTrue(e.queue().isEmpty())

        e.engine.pull()
        assertEquals("b", e.service.pullCursors.last())
    }

    @Test
    fun `disabled sync leaves the queue alone`() = runBlocking {
        val e = Env()
        val m = e.addManga("/m")
        e.exec("UPDATE mangas SET favorite = 1 WHERE _id = $m")
        e.prefs.enabled.set(false)
        assertEquals(DeltaSyncEngine.PushResult.Disabled, e.engine.push())
        assertEquals(DeltaSyncEngine.PullResult.Disabled, e.engine.pull())
        assertEquals("PENDING", e.queue().single()[2])
    }

    @Test
    fun `stuck is_syncing flags are cleared so edits are tracked again`() = runBlocking {
        val e = Env()
        val m = e.addManga("/m")
        e.exec("UPDATE mangas SET is_syncing = 1 WHERE _id = $m")
        e.exec("UPDATE mangas SET favorite = 1 WHERE _id = $m")
        assertTrue(e.queue().isEmpty())
        e.repo.resetSyncingFlags()
        e.exec("UPDATE mangas SET description = 'again' WHERE _id = $m")
        assertEquals(1, e.queue().size)
    }

    @Test
    fun `http service speaks the documented protocol`() = runBlocking {
        val received = mutableListOf<Triple<String, Map<String, String>, ByteArray>>()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val pullBody = ProtoBuf.encodeToByteArray(
            DeltaPullResponse.serializer(),
            DeltaPullResponse(cursor = "c9", hasMore = false, mangas = listOf(DeltaManga(backupManga("/r", version = 2)))),
        )
        server.createContext("/sync/deltas") { ex ->
            val headers = ex.requestHeaders.entries.associate { it.key.lowercase() to it.value.first() }
            received += Triple(ex.requestMethod + " " + ex.requestURI.toString(), headers, ex.requestBody.readBytes())
            if (ex.requestMethod == "POST") {
                val fail = received.count { it.first.startsWith("POST") } == 1
                ex.sendResponseHeaders(if (fail) 503 else 204, -1)
            } else {
                ex.sendResponseHeaders(200, pullBody.size.toLong())
                ex.responseBody.write(pullBody)
            }
            ex.close()
        }
        server.start()
        try {
            val store = MemStore()
            val prefs = DeltaSyncPreferences(store)
            prefs.endpoint.set("http://127.0.0.1:${server.address.port}/sync/")
            val service = HttpDeltaSyncService(NetworkHelper(), prefs, ProtoBuf)
            assertTrue(service.isConfigured())

            val payload = DeltaSyncPayload("dev-1", "batch-1", mangas = listOf(DeltaManga(backupManga("/m", version = 1))))
            val ex = assertFailsWith<HttpException> { service.push(payload) }
            assertEquals(503, ex.code)
            service.push(payload)

            val page = service.pull("dev-1", "c1", 50)
            assertEquals("c9", page.cursor)
            assertEquals("/r", page.mangas.single().manga.url)

            val (post, postHeaders, postBody) = received[1]
            assertEquals("POST /sync/deltas", post)
            assertEquals("dev-1", postHeaders["x-device-id"])
            assertEquals("batch-1", postHeaders["x-batch-id"])
            assertTrue(postHeaders["content-type"]!!.startsWith("application/x-protobuf"))
            assertEquals("/m", ProtoBuf.decodeFromByteArray(DeltaSyncPayload.serializer(), postBody).mangas.single().manga.url)

            val (get, getHeaders, _) = received[2]
            assertEquals("GET /sync/deltas?cursor=c1&limit=50", get)
            assertEquals("dev-1", getHeaders["x-device-id"])

            prefs.endpoint.set("not a url")
            assertEquals(false, service.isConfigured())
        } finally {
            server.stop(0)
        }
    }

    // ---- record-level rejection -------------------------------------------------------------------------------

    @Test
    fun `fake server naming one refused record still accepts the others`() = runBlocking {
        val refusing = object : DeltaSyncService {
            val pushed = mutableListOf<DeltaSyncPayload>()
            override fun isConfigured() = true
            override suspend fun push(payload: DeltaSyncPayload): DeltaPushResponse {
                pushed += payload
                val bad = payload.chapters.filter { "reject" in it.chapter.url }
                return DeltaPushResponse(bad.map { DeltaRejection(DeltaRejection.KIND_CHAPTER, it.mangaSource, it.mangaUrl, it.chapter.url, "no") })
            }
            override suspend fun pull(deviceId: String, cursor: String?, limit: Int) = DeltaPullResponse()
        }
        val e = Env(refusing)
        val m = e.addManga("/m")
        val good = e.addChapter(m, "/m/good")
        val bad = e.addChapter(m, "/m/reject-me")
        e.exec("UPDATE chapters SET read = 1")
        assertEquals(3, e.queue().size)

        assertEquals(DeltaSyncEngine.PushResult.PartiallyRejected(2, 1), e.engine.push())
        assertEquals(listOf(listOf("chapters", bad, "FAILED", 1L)), e.queue())
        assertEquals(good, good)
    }

    private fun startMockServer(vararg args: String): Pair<Process, String> {
        val script = java.io.File("../delta-sync-mock-server/mock_server.py").absolutePath
        val p = ProcessBuilder(listOf("python3", script, "--port", "0") + args).redirectErrorStream(true).start()
        val line = p.inputStream.bufferedReader().readLine() ?: error("mock server did not start")
        return p to line.substringAfter("listening on ").trim()
    }

    private fun http(method: String, url: String): String {
        val c = java.net.URL(url).openConnection() as java.net.HttpURLConnection
        c.requestMethod = method
        return if (c.responseCode in 200..299 && c.contentLength != 0) c.inputStream.readBytes().decodeToString() else ""
    }

    private fun Env.overMockServer(base: String): Env {
        val prefs = DeltaSyncPreferences(MemStore()).also { it.endpoint.set(base) }
        return Env(HttpDeltaSyncService(NetworkHelper(), prefs, ProtoBuf), prefs)
    }

    private fun scenarioWithOneBadChapter(e: Env): Pair<Long, Long> {
        val m = e.addManga("/m")
        val bad = e.addChapter(m, "/m/reject-me")
        for (i in 1..5) e.addChapter(m, "/m/ok$i")
        e.exec("UPDATE chapters SET read = 1")   // 6 chapters + the manga are now queued
        e.exec("UPDATE mangas SET favorite = 1")
        return m to bad
    }

    @Test
    fun `mock endpoint per-record mode - one refused record does not stop the rest`() = runBlocking {
        val (proc, base) = startMockServer("--mode", "per-record")
        try {
            val e = Env().overMockServer(base)
            val (_, bad) = scenarioWithOneBadChapter(e)
            assertEquals(7, e.queue().size)

            val r = e.engine.push()
            assertEquals(DeltaSyncEngine.PushResult.PartiallyRejected(6, 1), r)

            assertEquals(listOf(listOf("chapters", bad, "FAILED", 1L)), e.queue())
            val state = http("GET", base.substringBefore("/sync/v1") + "/_admin/state")
            assertEquals(1, Regex(""""kind": 1""").findAll(state).count(), state)
            assertEquals(5, Regex(""""kind": 2""").findAll(state).count(), state)
            assertTrue("reject-me" !in state.substringBefore("\"log\""), state)
            assertEquals(1, Regex(""""status": 200""").findAll(state).count(), "one request for the whole batch")
        } finally { proc.destroy() }
    }

    @Test
    fun `mock endpoint whole-batch mode - batch is split until the refused record stands alone`() = runBlocking {
        val (proc, base) = startMockServer("--mode", "whole-batch")
        try {
            val e = Env().overMockServer(base)
            val (_, bad) = scenarioWithOneBadChapter(e)

            val r = e.engine.push()
            assertEquals(DeltaSyncEngine.PushResult.PartiallyRejected(6, 1), r)
            assertEquals(listOf(listOf("chapters", bad, "FAILED", 1L)), e.queue())

            val state = http("GET", base.substringBefore("/sync/v1") + "/_admin/state")
            assertEquals(1, Regex(""""kind": 1""").findAll(state.substringBefore("\"log\"")).count(), state)
            assertEquals(5, Regex(""""kind": 2""").findAll(state.substringBefore("\"log\"")).count(), state)
            assertTrue(Regex(""""status": 422""").findAll(state).count() >= 2, "bisection should have hit the refusal more than once")
        } finally { proc.destroy() }
    }

    @Test
    fun `mock endpoint - a network error mid-split retries only what was not delivered`() = runBlocking {
        val (proc, base) = startMockServer("--mode", "whole-batch")
        try {
            val e = Env().overMockServer(base)
            scenarioWithOneBadChapter(e)
            // first request is refused with 422 (bad record in the batch), the next one (first half) dies with 503
            val first = e.engine.push() // establishes baseline: everything delivered except the bad one
            assertEquals(DeltaSyncEngine.PushResult.PartiallyRejected(6, 1), first)

            val m = e.value("SELECT _id FROM mangas")!!
            val c = e.addChapter(m, "/m/later")
            e.exec("UPDATE chapters SET bookmark = 1 WHERE _id = $c")
            http("POST", base.substringBefore("/sync/v1") + "/_admin/fail?next=1&code=503")
            assertIs<DeltaSyncEngine.PushResult.Retry>(e.engine.push())
            assertEquals(listOf("PENDING", "PENDING"), e.queue().filter { it[0] == "chapters" && it[1] == c || it[0] == "mangas" }.map { it[2] })

            assertEquals(DeltaSyncEngine.PushResult.Drained(2), e.engine.push())
            assertEquals(listOf("FAILED"), e.queue().map { it[2] })
        } finally { proc.destroy() }
    }

    @Test
    fun `mock endpoint - another device pulls what was accepted`() = runBlocking {
        val (proc, base) = startMockServer("--mode", "per-record")
        try {
            val a = Env().overMockServer(base)
            scenarioWithOneBadChapter(a)
            a.engine.push()

            val b = Env().overMockServer(base)
            b.addManga("/m")
            val pulled = b.engine.pull()
            assertIs<DeltaSyncEngine.PullResult.Pulled>(pulled)
            assertEquals(1, pulled.applied.mangasApplied)
            assertEquals(5, pulled.applied.chaptersApplied)
            assertEquals(0L, b.value("SELECT count(*) FROM chapters WHERE url LIKE '%reject%'"))
            assertEquals(1L, b.value("SELECT favorite FROM mangas"))
            assertTrue(b.queue().isEmpty())
            assertEquals(DeltaSyncEngine.PushResult.Idle, b.engine.push())

            // the pushing device does not get its own changes back
            val own = a.engine.pull()
            assertIs<DeltaSyncEngine.PullResult.Pulled>(own)
            assertEquals(0, own.applied.appliedCount)
        } finally { proc.destroy() }
    }

    // ---- rejection that matches nothing we sent ---------------------------------------------------------------

    @Test
    fun `a rejection naming an unknown record keeps the whole batch queued`() = runBlocking {
        val odd = object : DeltaSyncService {
            var ghost = true
            val accepted = mutableListOf<String>()
            override fun isConfigured() = true
            override suspend fun push(payload: DeltaSyncPayload): DeltaPushResponse {
                if (ghost) {
                    ghost = false
                    // one refusal that is real, one that matches nothing
                    return DeltaPushResponse(listOf(
                        DeltaRejection(DeltaRejection.KIND_CHAPTER, 1, "/m", "/m/reject-me", "bad"),
                        DeltaRejection(DeltaRejection.KIND_CHAPTER, 1, "/m", "/m/typo-url", "bad"),
                    ))
                }
                accepted += payload.chapters.map { it.chapter.url }
                return DeltaPushResponse()
            }
            override suspend fun pull(deviceId: String, cursor: String?, limit: Int) = DeltaPullResponse()
        }
        val e = Env(odd)
        val m = e.addManga("/m")
        e.addChapter(m, "/m/a"); e.addChapter(m, "/m/b"); e.addChapter(m, "/m/reject-me")
        e.exec("UPDATE chapters SET read = 1")
        assertEquals(4, e.queue().size)

        assertIs<DeltaSyncEngine.PushResult.Retry>(e.engine.push())
        // the refusal we can attribute is parked; the rest was not acknowledged, so it is still queued
        assertEquals(listOf("FAILED", "PENDING", "PENDING", "PENDING"), e.queue().map { it[2] as String }.sorted())
        assertTrue(odd.accepted.isEmpty())

        // next attempt: the server behaves and the two good chapters (and the manga) go through
        e.engine.push()
        assertEquals(listOf("/m/a", "/m/b"), odd.accepted.sorted())
        assertEquals(listOf("FAILED"), e.queue().map { it[2] })
    }

    @Test
    fun `mock endpoint - edit survives a bogus rejection and syncs on retry`() = runBlocking {
        val (proc, base) = startMockServer("--mode", "per-record")
        try {
            val admin = base.substringBefore("/sync/v1")
            val e = Env().overMockServer(base)
            val m = e.addManga("/m")
            val c = e.addChapter(m, "/m/c1")
            e.exec("UPDATE chapters SET read = 1, last_page_read = 12 WHERE _id = $c")
            assertEquals(2, e.queue().size)

            http("POST", "$admin/_admin/ghost?next=1")
            val first = e.engine.push()
            assertIs<DeltaSyncEngine.PushResult.Retry>(first)

            // server answered 200 but stored nothing, and our queue still holds both edits
            val afterFirst = http("GET", "$admin/_admin/state")
            assertTrue(""""records": []""" in afterFirst, afterFirst)
            assertTrue(""""ghost": true""" in afterFirst, afterFirst)
            assertEquals(listOf(listOf("chapters", c, "PENDING", 1L), listOf("mangas", m, "PENDING", 1L)), e.queue().sortedBy { it[0] as String })

            // retry: the server behaves, the edit arrives and the queue empties
            assertEquals(DeltaSyncEngine.PushResult.Drained(2), e.engine.push())
            val afterRetry = http("GET", "$admin/_admin/state")
            assertEquals(1, Regex(""""kind": 2, "manga": "/m", "chapter": "/m/c1"""").findAll(afterRetry).count(), afterRetry)
            assertTrue(e.queue().isEmpty())
        } finally { proc.destroy() }
    }
}
