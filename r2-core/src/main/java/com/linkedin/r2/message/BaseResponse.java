package com.linkedin.r2.message;

import java.util.List;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public abstract class BaseResponse extends BaseMessage implements Response
{
  private final int _status;

  protected BaseResponse(Map<String, String> headers, List<String> cookies, int status)
  {
    super(headers, cookies);
    _status = status;
  }

  @Override
  public int getStatus()
  {
    return _status;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
    {
      return true;
    }
    if (!(o instanceof BaseResponse))
    {
      return false;
    }
    if (!super.equals(o))
    {
      return false;
    }

    return _status == ((BaseResponse) o)._status;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + _status;
    return result;
  }
}
