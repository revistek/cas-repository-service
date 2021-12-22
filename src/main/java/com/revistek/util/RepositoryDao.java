package com.revistek.util;

import com.revistek.crs.protos.Cas;
import com.revistek.exceptions.MalformedDataException;

/**
 * The interface for repository data access objects. A repository holds the actual CAS data.
 *
 * @author Chuong Ngo
 */
public interface RepositoryDao {
  /** Perform any necessary initialization of the DAO. */
  public default void initialize() {}
  
  /** Perform any necessary teardown of the DAO. */
  public default void cleanup() {}

  /**
   * Retrieves a {@link com.revistek.protos.Cas Cas} from the repository using its CAS ID.
   *
   * @param casId the CAS ID of the {@link com.revistek.protos.Cas Cas} to retrieve.
   * @return the retrieved {@link com.revistek.protos.Cas Cas}.
   * @throws Exception The repository is in an invalid state (e.g., there are multiple {@link
   *     com.revistek.protos.Cas Cas} objects for the specified CAS ID), or there is no connection
   *     to the repository. or the CAS ID is invalid, or there is no connection to the repository..
   */
  public Cas getCasId(String casId) throws Exception;

  /**
   * Stores a {@link com.revistek.protos.Cas Cas} into the repository.
   *
   * @param cas the {@link com.revistek.protos.Cas Cas} to store.
   * @return The CAS Id of the CAS that wwas stored.
   * @throws MalformedDataException the {@link com.revistek.protos.Cas Cas} is malformed (e.g., the
   *     binary blob failed its checksum check.
   * @throws Exception
   */
  public String store(Cas cas) throws Exception;

  /**
   * Deletes a {@link com.revistek.protos.Cas Cas} from the repository.
   *
   * @param casId the CASID of the {@link com.revistek.protos.Cas Cas} to delete.
   * @throws Exception The repository is in an invalid state (e.g., there are multiple {@link
   *     com.revistek.protos.Cas Cas} objects for the specified CAS ID), or the CAS ID is invalid,
   *     or there is no connection to the repository.
   */
  public void deleteCasId(String casId) throws Exception;

  /**
   * Checks if there is already a {@link com.revistek.protos.Cas Cas} with the specified CAS ID in
   * the repository.
   *
   * @param casId the CAS ID to look for.
   * @return true if there exists one or more entries with the specified CAS ID, else false.
   * @throws Exception There is no connection to the repository.
   */
  public boolean existsCasId(String casId) throws Exception;
}
