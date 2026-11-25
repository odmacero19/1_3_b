package logcat
enum class LogPriority { VERBOSE, DEBUG, INFO, WARN, ERROR }
fun Throwable.asLog(): String = stackTraceToString()
inline fun Any.logcat(priority: LogPriority = LogPriority.DEBUG, tag: String? = null, message: () -> String) {
    System.err.println("[$priority] ${message()}")
}
