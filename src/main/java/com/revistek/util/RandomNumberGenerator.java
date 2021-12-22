package com.revistek.util;

import java.util.Random;

/**
 * Generates a random number based on a seed.
 * 
 * @author Chuong Ngo
 *
 */
public class RandomNumberGenerator {
  Random rand;

  public RandomNumberGenerator(long seed) {
    rand = new Random(seed);
  }

  public int randomInt() {
    return rand.nextInt();
  }
}
