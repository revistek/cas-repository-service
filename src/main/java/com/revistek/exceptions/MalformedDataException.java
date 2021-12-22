package com.revistek.exceptions;

/** An exception for data errors, like those arising from erroneous transmission. */
public class MalformedDataException extends Exception {
  public static final long serialVersionUID = 1L;

  public MalformedDataException(String message) {
    super(message);
  }
}
