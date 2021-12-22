package com.revistek.util;

/**
 * The interface for metadata store data access objects. A metadata store holds the metadata for the
 * CASes to be processed. For example, it writes the CAS IDs into different tables depending on the
 * processing needs of the CAS.
 *
 * @author Chuong Ngo
 */
public interface MetadataStoreDao {
  /** Perform any necessary teardown of the DAO. */
  public default void cleanup() {}

  /**
   * Adds a Cas ID to the metadata store.
   *
   * @param queryKey - the metadata query to add the Cas ID with.
   * @param casId - the Cas ID to add.
   * @throws Exception There was a problem with this operation.
   */
  public void addCasId(String queryKey, String casId) throws Exception;

  /**
   * Deletes a Cas ID from the metadata store with a specific query.
   *
   * @param queryKey - the metadata query to add the Cas ID with.
   * @param casId - the Cas ID to add.
   * @throws Exception There was a problem with this operation.
   */
  public void deleteCasId(String queryKey, String casId) throws Exception;

  /**
   * Deletes a Cas ID from the metadata store with every specific query.
   *
   * @param queryKey - the metadata query to add the Cas ID with.
   * @param casId - the Cas ID to add.
   * @throws Exception There was a problem with this operation.
   */
  public void deleteAllCasId(String casId) throws Exception;
}
