package com.revistek.exceptions;

/**
 * An exception for when the metadata store is in an illegal state (e.g., a CAS ID is not unique).
 */
public class IllegalMetadataStoreStateException extends Exception {
  public static final long serialVersionUID = 1L;

  public IllegalMetadataStoreStateException(String message) {
    super(message);
  }
}
