package com.revistek.util;

import com.revistek.crs.constants.Cache;
// import com.revistek.util.constants.CacheConstants;
import com.revistek.util.constants.ErrorMessages;
import java.time.Clock;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a unique CAS ID.
 *
 * @author Chuong Ngo
 */
public class IdGenerator {
  public static final String DEFAULT_DELIMITER = "_";
  public static final String DEFAULT_ID_CACHE_KEY = "idKey";

  private static final Logger LOGGER = LoggerFactory.getLogger(IdGenerator.class);

  private CacheDao cacheDao;
  private String delimiter;
  private UUID uuid;
  private Clock clock;
  private RandomNumberGenerator randomNumberGenerator;

  private IdGenerator() {}

  /**
   * Refreshes the generator and generate a unique ID value.
   *
   * @return a unique ID value.
   */
  public String refreshAndGetUniqueId() {
    randomizeUuid();
    resetClock();
    resetRandomNumberGenerator();

    return uniqueId();
  }

  private void randomizeUuid() {
    LOGGER.trace("Randomizing the UUID.");

    uuid = UUID.randomUUID();
  }

  private void resetClock() {
    LOGGER.trace("Resetting the clock.");

    clock = Clock.systemUTC();
  }

  private void resetRandomNumberGenerator() {
    LOGGER.trace("Randomizing the random number generator.");

    Clock seedGenerator = Clock.systemUTC();
    randomNumberGenerator = new RandomNumberGenerator(seedGenerator.millis());
  }

  /**
   * Generates an ID string that is the concatenation of a time in milliseconds, a UUID, and a
   * random integer. The components are separated by a delimiter. The generated ID string is also
   * registered with the cache to help prevent ID collisions.
   *
   * <p>NOTE: This method results in an ID string that is practically unique, but uniqueness is not
   * guaranteed.
   */
  public String uniqueId() {
    LOGGER.trace("Generating a unique ID string.");

    String idString;
    boolean isDuplicate = true;

    do {
      String randIntString = String.valueOf(randomNumberGenerator.randomInt());
      String milliString = String.valueOf(clock.millis());

      idString =
          (new StringBuilder())
              .append(milliString)
              .append(delimiter)
              .append(uuid.toString())
              .append(delimiter)
              .append(randIntString)
              .toString();

      try {
        isDuplicate = cacheDao.exists(idString);
      } catch (Exception e) {
        LOGGER.trace(
            "The generated ID string is not unique. Randomizing the ID string generating components and trying again.");

        randomizeUuid();
        resetClock();
        resetRandomNumberGenerator();
      }
    } while (isDuplicate);

    LOGGER.trace("Writing the ID string too the cache with the default timeout.");
    cacheDao.set(idString, idString, Cache.DEFAULT_TIMEOUT_IN_SECS);
    return idString;
  }

  /**
   * The builder class for {@link com.revistek.util.IdGenerator IdGenerator}.
   *
   * @author Chuong Ngo
   */
  public static class Builder {
    private CacheDao cacheDao;
    private String delimiter;
    private UUID uuid;
    private Clock clock;
    private RandomNumberGenerator randomNumberGenerator;

    public Builder cacheDao(CacheDao cacheDao) {
      if (cacheDao == null) {
        throw new IllegalArgumentException(ErrorMessages.REQUIRED_ARGUMENT_IS_NULL);
      }

      this.cacheDao = cacheDao;
      return this;
    }

    public Builder randomGenerator(RandomNumberGenerator randomNumberGenerator) {
      this.randomNumberGenerator = randomNumberGenerator;
      return this;
    }

    public Builder uuid(UUID uuid) {
      this.uuid = uuid;
      return this;
    }

    public Builder clock(Clock clock) {
      this.clock = clock;
      return this;
    }

    public Builder delimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public IdGenerator build() {
      IdGenerator idGenerator = new IdGenerator();
      idGenerator.cacheDao = cacheDao;
      idGenerator.cacheDao.initialize();

      if (uuid != null) {
        idGenerator.uuid = uuid;
      } else {
        idGenerator.randomizeUuid();
      }

      if (clock != null) {
        idGenerator.clock = clock;
      } else {
        idGenerator.resetClock();
      }

      if (randomNumberGenerator != null) {
        idGenerator.randomNumberGenerator = randomNumberGenerator;
      } else {
        idGenerator.resetRandomNumberGenerator();
      }

      if (!StringUtils.isEmpty(delimiter)) {
        idGenerator.delimiter = delimiter;
      } else {
        idGenerator.delimiter = IdGenerator.DEFAULT_DELIMITER;
      }

      return idGenerator;
    }
  }

  /**
   * Returns a new builder.
   *
   * @return Returns a new {@link com.revistek.util.IdGenerator.Builder builder}.
   */
  public static Builder newBuilder() {
    Builder builder = new Builder();
    return builder;
  }
}
