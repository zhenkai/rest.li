package test.r2.perf.client;

/**
 * @auther Zhenkai Zhu
 */

public interface RateLimiter
{
  void init();
  boolean acquirePermit();
}
