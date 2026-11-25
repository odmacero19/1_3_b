package eu.kanade.tachiyomi.data.sync.delta

import eu.kanade.tachiyomi.network.GET
import eu.kanade.tachiyomi.network.HttpException
import eu.kanade.tachiyomi.network.NetworkHelper
import eu.kanade.tachiyomi.network.POST
import eu.kanade.tachiyomi.network.await
import kotlinx.serialization.SerializationException
import kotlinx.serialization.protobuf.ProtoBuf
import logcat.LogPriority
import okhttp3.CacheControl
import okhttp3.Headers
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.sync.service.DeltaSyncPreferences

/**
 * Transport for delta payloads. Implementations throw on any failure; [DeltaSyncEngine] decides whether
 * the failure is worth retrying.
 */
interface DeltaSyncService {

    /** Whether a usable server address is configured. */
    fun isConfigured(): Boolean

    /**
     * Uploads [payload]. Returns normally when the server acknowledged the request; records it refused are listed
     * in the response. Throws when the request as a whole failed.
     */
    suspend fun push(payload: DeltaSyncPayload): DeltaPushResponse

    /** Fetches the page of remote changes that follows [cursor] (null: from the beginning). */
    suspend fun pull(deviceId: String, cursor: String?, limit: Int): DeltaPullResponse
}

/**
 * Protocol, all bodies protobuf ([DeltaSyncPayload] / [DeltaPullResponse]):
 *
 *  - `POST {endpoint}/deltas`                         → any 2xx acknowledges the request. The body may be a
 *                                                        [DeltaPushResponse] naming records that were refused
 *                                                        while the rest was accepted. 400/413/422 without such a
 *                                                        body means "something in this batch is unacceptable"
 *  - `GET  {endpoint}/deltas?cursor=<c>&limit=<n>`    → 200 with a page of remote changes
 *
 * Both carry `X-Device-Id`; pushes also set `X-Batch-Id` so a redelivered batch can be recognised.
 */
class HttpDeltaSyncService(
    private val networkHelper: NetworkHelper,
    private val preferences: DeltaSyncPreferences,
    private val protoBuf: ProtoBuf,
) : DeltaSyncService {

    override fun isConfigured(): Boolean = baseUrl() != null

    override suspend fun push(payload: DeltaSyncPayload): DeltaPushResponse {
        val url = requireBaseUrl().newBuilder().addPathSegment("deltas").build()
        val body = protoBuf.encodeToByteArray(DeltaSyncPayload.serializer(), payload)
            .toRequestBody(PROTOBUF_MEDIA_TYPE)

        val headers = Headers.Builder()
            .add(HEADER_DEVICE_ID, payload.deviceId)
            .add(HEADER_BATCH_ID, payload.batchId)
            .build()

        return networkHelper.client
            .newCall(POST(url.toString(), headers, body, CacheControl.FORCE_NETWORK))
            .await()
            .use { response ->
                if (!response.isSuccessful) throw HttpException(response.code)
                response.body.bytes()
            }
            .let(::decodePushResponse)
    }

    private fun decodePushResponse(bytes: ByteArray): DeltaPushResponse {
        if (bytes.isEmpty()) return DeltaPushResponse()
        return try {
            protoBuf.decodeFromByteArray(DeltaPushResponse.serializer(), bytes)
        } catch (e: SerializationException) {
            // The request itself succeeded; an unreadable acknowledgement is treated as a plain one
            logcat(LogPriority.WARN, e) { "Ignoring unreadable push response" }
            DeltaPushResponse()
        }
    }

    override suspend fun pull(deviceId: String, cursor: String?, limit: Int): DeltaPullResponse {
        val url = requireBaseUrl().newBuilder()
            .addPathSegment("deltas")
            .apply { if (!cursor.isNullOrEmpty()) addQueryParameter("cursor", cursor) }
            .addQueryParameter("limit", limit.toString())
            .build()

        val headers = Headers.Builder()
            .add(HEADER_DEVICE_ID, deviceId)
            .add(HEADER_ACCEPT, PROTOBUF_CONTENT_TYPE)
            .build()

        val bytes = networkHelper.client
            .newCall(GET(url, headers, CacheControl.FORCE_NETWORK))
            .await()
            .use { response ->
                if (!response.isSuccessful) throw HttpException(response.code)
                response.body.bytes()
            }

        return protoBuf.decodeFromByteArray(DeltaPullResponse.serializer(), bytes)
    }

    private fun baseUrl(): HttpUrl? = preferences.endpoint.get().trim().toHttpUrlOrNull()

    private fun requireBaseUrl(): HttpUrl = requireNotNull(baseUrl()) { "Delta sync endpoint is not configured" }

    private companion object {
        const val HEADER_DEVICE_ID = "X-Device-Id"
        const val HEADER_BATCH_ID = "X-Batch-Id"
        const val HEADER_ACCEPT = "Accept"
        const val PROTOBUF_CONTENT_TYPE = "application/x-protobuf"
        val PROTOBUF_MEDIA_TYPE = PROTOBUF_CONTENT_TYPE.toMediaType()
    }
}
