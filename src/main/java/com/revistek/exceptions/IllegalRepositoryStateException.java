package com.revistek.exceptions;

/** An exception for when the repository is in an illegal state (e.g., a CAS ID is not unique). */
public class IllegalRepositoryStateException extends Exception {
  public static final long serialVersionUID = 1L;

  public IllegalRepositoryStateException(String message) {
    super(message);
  }
}
