package com.linkedin.r2.message.stream.entitystream;

/**
 * @author Zhenkai Zhu
 */
public class AbortedException extends Exception
{
  static final long serialVersionUID = 0L;

  public AbortedException() {
    super();
  }

  public AbortedException(String message) {
    super(message);
  }

  public AbortedException(String message, Throwable cause) {
    super(message, cause);
  }

  public AbortedException(Throwable cause) {
    super(cause);
  }
}
