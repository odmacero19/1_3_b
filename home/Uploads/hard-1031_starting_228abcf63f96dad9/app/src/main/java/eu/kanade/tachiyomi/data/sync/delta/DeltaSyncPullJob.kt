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
 * Retrieves remote deltas and applies them to the local database.
 *
 * Application is done by `SyncDeltaRepository.applyRemoteDelta`, which raises `is_syncing` on every row it
 * touches so that the version triggers ignore the write. Without that, each incoming change would be queued
 * as a local modification, pushed back, and bounce between devices forever.
 */
class DeltaSyncPullJob(context: Context, workerParams: WorkerParameters) :
    CoroutineWorker(context, workerParams) {

    private val engine: DeltaSyncEngine = Injekt.get()

    override suspend fun doWork(): Result {
        return when (val result = engine.pull()) {
            DeltaSyncEngine.PullResult.Disabled -> Result.success()
            is DeltaSyncEngine.PullResult.Pulled -> {
                logcat(LogPriority.DEBUG) {
                    "Delta pull applied ${result.applied.appliedCount} changes in ${result.pages} page(s), " +
                        "skipped ${result.applied.skipped}, orphaned ${result.applied.orphaned}"
                }
                Result.success()
            }
            is DeltaSyncEngine.PullResult.Retry -> Result.retry()
            is DeltaSyncEngine.PullResult.Failed -> Result.failure()
        }
    }

    companion object {
        private const val TAG_PERIODIC = "DeltaSyncPull"
        private const val TAG_NOW = "DeltaSyncPull:now"

        private val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        fun setupTask(context: Context) {
            val request = PeriodicWorkRequestBuilder<DeltaSyncPullJob>(15, TimeUnit.MINUTES)
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

        fun requestPull(context: Context) {
            val request = OneTimeWorkRequestBuilder<DeltaSyncPullJob>()
                .setConstraints(constraints)
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .addTag(TAG_NOW)
                .build()

            context.workManager.enqueueUniqueWork(TAG_NOW, ExistingWorkPolicy.KEEP, request)
        }
    }
}
