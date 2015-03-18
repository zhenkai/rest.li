package com.linkedin.r2.message.rest;

import java.util.List;
import java.util.Map;

/**
 * @author Zhenkai Zhu
 */
public interface RestHead
{
  /**
   * Gets the value of the header with the given name. If there is no header with the given name
   * then this method returns {@code null}. If the header has multiple values then this method
   * returns the list joined with commas as allowed by RFC-2616, section 4.2.
   *
   * @param name name of the header
   * @return the value of the header or {@code null} if there is no header with the given name.
   */
  String getHeader(String name);

  /**
   * Treats the header with the given name as a multi-value header (see RFC 2616, section 4.2). Each
   * value for the header is a separate element in the returned list. If no header exists with the
   * supplied name then {@code null} is returned.
   *
   * @param name the name of the header
   * @return a list of values for the header or {@code null} if no values exist.
   */
  List<String> getHeaderValues(String name);

  /**
   * Gets the values of cookies as specified in the Cookie or Set-Cookies HTTP headers in the
   * HTTP request and response respectively. Each Cookie or Set-Cookie header is a separate element in
   * the returned elements. If no cookie exists then an empty list is returned.
   *
   * @return cookies specified in the Cookie or Set-Cookie HTTP headers.
   */
  List<String> getCookies();

  /**
   * Returns an unmodifiable view of the headers in this builder. Because this is a view of the
   * headers and not a copy, changes to the headers in this builder *may* be reflected in the
   * returned map.
   *
   * @return a view of the headers in this builder
   */
  Map<String, String> getHeaders();
}
