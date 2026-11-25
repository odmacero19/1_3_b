package eu.kanade.tachiyomi.data.sync.delta

import android.content.Context
import eu.kanade.tachiyomi.data.backup.create.BackupCreateJob
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import logcat.LogPriority
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.sync.repository.SyncDeltaRepository
import tachiyomi.domain.sync.service.DeltaSyncPreferences
import kotlin.time.Duration.Companion.seconds

/**
 * Keeps the sync jobs scheduled and reacts to local edits: as soon as the trigger-maintained queue holds
 * something, a push is requested. Bursts of edits are debounced into one request.
 */
class DeltaSyncManager(
    private val context: Context,
    private val repository: SyncDeltaRepository,
    private val preferences: DeltaSyncPreferences,
) {

    @OptIn(FlowPreview::class)
    fun start(scope: CoroutineScope) {
        scope.launch(Dispatchers.IO) {
            try {
                // is_syncing is raised only inside the transaction that applies remote data; anything found
                // raised at startup was left behind by an interrupted write and would hide local edits.
                repository.resetSyncingFlags()
            } catch (e: Exception) {
                logcat(LogPriority.ERROR, e) { "Failed to reset is_syncing flags" }
            }
        }

        preferences.enabled.changes()
            .onEach { enabled ->
                // Full .tachibk exports are replaced by delta sync while it is on (and come back when it is
                // off). Also runs once at startup, which is what pauses them on installs that already had
                // a schedule.
                BackupCreateJob.setupTask(context)
                if (enabled) {
                    schedule()
                } else {
                    DeltaSyncPushJob.cancelTask(context)
                    DeltaSyncPullJob.cancelTask(context)
                }
            }
            .launchIn(scope)

        preferences.enabled.changes()
            .flatMapLatest { enabled -> if (enabled) repository.getPendingCountAsFlow() else emptyFlow() }
            .catch { logcat(LogPriority.ERROR, it) { "Delta queue observation failed" } }
            .filter { it > 0 }
            .debounce(PUSH_DEBOUNCE)
            .onEach { DeltaSyncPushJob.requestPush(context) }
            .launchIn(scope)
    }

    fun schedule() {
        DeltaSyncPushJob.setupTask(context)
        DeltaSyncPullJob.setupTask(context)
    }

    /** App moved to the background: flush local edits and pick up remote ones. */
    fun syncNow() {
        if (!preferences.enabled.get()) return
        DeltaSyncPushJob.requestPush(context)
        DeltaSyncPullJob.requestPull(context)
    }

    private companion object {
        val PUSH_DEBOUNCE = 3.seconds
    }
}
