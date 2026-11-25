package eu.kanade.tachiyomi.data.sync.delta

import android.content.Context
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkerParameters
import eu.kanade.tachiyomi.util.system.workManager
import logcat.LogPriority
import tachiyomi.core.common.util.system.logcat
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import java.util.concurrent.TimeUnit

/**
 * Uploads queued local changes (see `sync_delta_queue`) as incremental delta payloads.
 *
 * The work is kept "continuous" by three triggers that all end up here: a periodic safety net, a request
 * whenever the queue becomes non-empty ([DeltaSyncManager]) and a request when the app goes to the background.
 * Each run drains the queue in batches, so a burst of edits collapses into a few uploads.
 */
class DeltaSyncPushJob(context: Context, workerParams: WorkerParameters) :
    CoroutineWorker(context, workerParams) {

    private val engine: DeltaSyncEngine = Injekt.get()

    override suspend fun doWork(): Result {
        return when (val result = engine.push()) {
            DeltaSyncEngine.PushResult.Idle,
            DeltaSyncEngine.PushResult.Disabled,
            is DeltaSyncEngine.PushResult.Drained,
            -> Result.success()
            // Rejected entries are parked by the queue itself, this run did its job
            is DeltaSyncEngine.PushResult.PartiallyRejected -> Result.success()
            is DeltaSyncEngine.PushResult.Retry -> {
                logcat(LogPriority.DEBUG, result.cause) { "Delta push will be retried" }
                Result.retry()
            }
        }
    }

    companion object {
        private const val TAG_PERIODIC = "DeltaSyncPush"
        private const val TAG_NOW = "DeltaSyncPush:now"

        private val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        /** Periodic safety net, in case an on-demand request was missed. */
        fun setupTask(context: Context) {
            val request = PeriodicWorkRequestBuilder<DeltaSyncPushJob>(15, TimeUnit.MINUTES)
                .setConstraints(constraints)
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .addTag(TAG_PERIODIC)
                .build()

            context.workManager.enqueueUniquePeriodicWork(TAG_PERIODIC, ExistingPeriodicWorkPolicy.UPDATE, request)
        }

        fun cancelTask(context: Context) {
            context.workManager.cancelUniqueWork(TAG_PERIODIC)
            context.workManager.cancelUniqueWork(TAG_NOW)
        }

        /**
         * Runs a push as soon as the network allows. Requests made while one is already waiting or running are
         * dropped: the running job keeps claiming batches until the queue is empty.
         */
        fun requestPush(context: Context) {
            val request = OneTimeWorkRequestBuilder<DeltaSyncPushJob>()
                .setConstraints(constraints)
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .addTag(TAG_NOW)
                .build()

            context.workManager.enqueueUniqueWork(TAG_NOW, ExistingWorkPolicy.KEEP, request)
        }
    }
}
