package tachiyomi.domain.sync.service

import tachiyomi.core.common.preference.Preference
import tachiyomi.core.common.preference.PreferenceStore
import java.util.UUID

class DeltaSyncPreferences(
    private val preferenceStore: PreferenceStore,
) {

    val enabled: Preference<Boolean> = preferenceStore.getBoolean("delta_sync_enabled", true)

    /**
     * Base URL of the sync server. Defaults to the host loopback as seen from the Android emulator, where
     * the mock server is expected to run.
     */
    val endpoint: Preference<String> = preferenceStore.getString("delta_sync_endpoint", DEFAULT_ENDPOINT)

    private val deviceIdPref: Preference<String> = preferenceStore.getString(
        Preference.appStateKey("delta_sync_device_id"),
        "",
    )

    /** Random identifier of this installation, so the server can avoid echoing a device's own changes back. */
    fun deviceId(): String {
        deviceIdPref.get().takeIf { it.isNotEmpty() }?.let { return it }
        return synchronized(this) {
            deviceIdPref.get().takeIf { it.isNotEmpty() }
                ?: UUID.randomUUID().toString().also(deviceIdPref::set)
        }
    }

    companion object {
        const val DEFAULT_ENDPOINT = "http://10.0.2.2:8080/sync/v1"
    }
}
