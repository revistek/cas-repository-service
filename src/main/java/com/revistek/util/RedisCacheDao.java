package com.revistek.util;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

/**
 * An implementation of the cache DAO for REDIS.
 *
 * @author Chuong Ngo
 */
public class RedisCacheDao implements CacheDao {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheDao.class);

  private JedisPooled clientPooled;

  public RedisCacheDao(String url) {
    clientPooled = new JedisPooled(url);

    LOGGER.trace("Initialized to url: " + url);
  }

  @Override
  public void set(String key, String value, int timeoutInSecs) {
    LOGGER.trace(
        "Get the value for the key: "
            + key
            + " to value: "
            + value
            + " with a timeout of "
            + String.valueOf(timeoutInSecs)
            + " seconds.");

    clientPooled.setex(key, timeoutInSecs, value);
  }

  @Override
  public String get(String key) {
    LOGGER.trace("Get the value for the key: " + key + ".");

    return clientPooled.get(key);
  }

  @Override
  public Map<String, String> getMap(String key) {
    LOGGER.trace("Get the map for the key: " + key + ".");

    return clientPooled.hgetAll(key);
  }

  @Override
  public boolean exists(String key) {
    LOGGER.trace("Does the key: " + key + " exists in the cache?");

    return clientPooled.exists(key);
  }

  @Override
  public void cleanup() {
    LOGGER.trace("Closing.");

    clientPooled.close();
  }
}
