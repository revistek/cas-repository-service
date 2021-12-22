package com.revistek.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestRandomNumberGenerator {
  @Test
  public void testRandomInt() {
    RandomNumberGenerator rand = new RandomNumberGenerator(1L);
    assertEquals(rand.randomInt(), -1155869325);
  }
}
