package com.revistek.util;

import java.util.Map;

/**
 * The interface for Cache data access objects. For example, it holds recently assigned CAS IDs to
 * help avoid CAS ID duplication.
 *
 * @author Chuong Ngo
 */
public interface CacheDao {
  /** Perform any necessary initialization of the DAO. */
  public default void initialize() {}

  /** Perform any necessary teardown of the DAO. */
  public default void cleanup() {}

  /**
   * Adds a string value, associating it to a string key to the cache.
   *
   * @param key the string to associate the value to.
   * @param value the value to cache.
   * @param timeoutInSecs how long the value should stay valid, in seconds.
   */
  public void set(String key, String value, int timeoutInSecs);

  /**
   * Checks if a key exists in the cache and has not timed out.
   *
   * @param key the key that is being queried.
   * @return true if the key is in the cache else false if the key is not in the cache or has timed
   *     out.
   * @throws Exception there was a problem communicating with the cache.
   */
  public boolean exists(String key) throws Exception;

  /**
   * Retrieves the value associated with the key.
   *
   * @param key the key that is being queried.
   * @return The value associated with the key.
   */
  public String get(String key);

  /**
   * Retrieves a map of key-value pairs associated with a map key.
   *
   * @param key the key that is being queried.
   * @return The map associated with the key.
   */
  public Map<String, String> getMap(String key);
}
