object TimeIt {
  def executionTime[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val traces = Thread.currentThread().getStackTrace.toList
    println("Execution time: " + (t1 - t0) / 1e9d + " seconds - " + traces)
    result
  }

}
