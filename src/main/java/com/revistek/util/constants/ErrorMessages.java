package com.revistek.util.constants;

/**
 * The error messages used by the "com.revistek.util" package.
 *
 * @author Chuong Ngo
 */
public final class ErrorMessages {
  public static final String REPOSITORY_NOT_CONNECTED =
      "A connection to the repository has not been established.";
  public static final String METADATA_STORE_NOT_CONNECTED =
      "A connection to the metadata store has not been established.";
  public static final String CACHE_COMMAND_FAILED =
      "Failed to execute the command against the cache. Please verify that the cache is running and accessible.";
  public static final String CHECKSUM_CHECK_FAILED =
      "The Cas failed its checksum check, document id: %s";
  public static final String CASID_NOT_UNIQUE = "The Cas ID is not unique.";
  public static final String REQUIRED_ARGUMENT_IS_NULL = "A required argument is null.";
  public static final String INVALID_CONFIGURATION_FILE_CACHEURL =
      "The cache URL in the configuration file is invalid.";
  public static final String INVALID_REPOSITORY_URL = "The repository URL is invalid.";
  public static final String INVALID_REPOSITORY_IDGENERATOR = "The IdGenerator is invalid.";
  public static final String INVALID_REPOSITORY_ENTRY = "The repository entry is invalid.";
  public static final String INVALID_PROTOBUF_CAS = "The protobuf CAS is invalid.";
  public static final String INVALID_MONGODB_REPOSITORY_DATABASE =
      "The MongoDB repository database is invalid.";
  public static final String INVALID_MONGODB_REPOSITORY_COLLECTION =
      "The MongoDB repository collection is invalid.";
  public static final String INVALID_METADATA_STORE_QUERY_REGISTER =
      "The query is invalid and cannot be registered.";
  public static final String INVALID_METADATA_STORE_QUERY =
      "The %s query was not found or is invalid.";
  public static final String INVALID_CAS_ID = "Invalid CAS ID: %s";

  private ErrorMessages() {}

  public static String getChecksumFailedMessage(String docId) {
    return String.format(CHECKSUM_CHECK_FAILED, docId);
  }

  public static String getInvalidQueryMessage(String queryId) {
    return String.format(INVALID_METADATA_STORE_QUERY, queryId);
  }
}
