package com.revistek.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPooled;
import redis.embedded.RedisServer;

public class TestRedisCacheDao {
  public static final int PORT = 12345;
  public static final String URL = "redis://localhost:" + String.valueOf(PORT);
  public static final Map<String, String> testMap =
      new HashMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {
          put("key01", "val01");
          put("key02", "val02");
          put("key03", "val03");
        }
      };
  private static RedisServer redisServer;

  @BeforeAll
  public static void setupRedis() {
    // Starting embedded server for testing.
    redisServer = new RedisServer(PORT);
    redisServer.start();

    JedisPooled client = new JedisPooled(URL);

    client.set("testkey", "testval");
    client.hset("mapkey", testMap);

    client.close();
  }

  @AfterAll
  public static void teardownRedis() {
    redisServer.stop();
  }

  @Test
  public void testSet() {
    RedisCacheDao dao = new RedisCacheDao(URL);
    dao.set("key", "val", 100);
    assertEquals("val", dao.get("key"));
    dao.cleanup();
  }

  @Test
  public void testGet() {
    RedisCacheDao dao = new RedisCacheDao(URL);
    assertEquals("testval", dao.get("testkey"));
    dao.cleanup();
  }

  @Test
  public void testExpire() throws InterruptedException {
    RedisCacheDao dao = new RedisCacheDao(URL);
    dao.set("key", "val", 10);
    assertEquals("val", dao.get("key"));
    TimeUnit.SECONDS.sleep(15);
    assertEquals(null, dao.get("key"));
    dao.cleanup();
  }

  @Test
  public void testExists() {
    RedisCacheDao dao = new RedisCacheDao(URL);
    assertTrue(dao.exists("testkey"));
    assertFalse(dao.exists("nokey"));
    dao.cleanup();
  }

  @Test
  public void testGetMaps() {
    RedisCacheDao dao = new RedisCacheDao(URL);
    assertEquals(testMap, dao.getMap("mapkey"));
    dao.cleanup();
  }
}
