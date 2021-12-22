package com.revistek.util;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.revistek.exceptions.IllegalMetadataStoreStateException;
import com.revistek.util.constants.ErrorMessages;
import com.revistek.util.constants.MongoDbDocument;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a Metadata Store DAO for MongoDB.
 *
 * @author Chuong Ngo
 */
public class MongoDbMetadataStoreDao implements MetadataStoreDao {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStoreDao.class);

  private MongoClient client;
  private Map<String, Pair<String, String>> queryMap;

  public MongoDbMetadataStoreDao(String url) {
    client = MongoClients.create(url);
    queryMap = new HashMap<String, Pair<String, String>>();

    LOGGER.trace("Initialized.");
  }

  /**
   * Adds the database, collection pairs to be used when querying the metadata store.
   *
   * @param key the query key to associate the database, collection pair to.
   * @param database the database to query against.
   * @param collection the collection to query or write to.
   */
  public void registerQuery(String key, String database, String collection) {
    LOGGER.trace(
        "Loading the query: " + key + ", database:" + database + ", collection: " + collection);

    if (StringUtils.isAnyEmpty(key, database, collection)) {
      LOGGER.trace("Cannot register this query. It is invalid.");

      throw new IllegalArgumentException(ErrorMessages.INVALID_METADATA_STORE_QUERY_REGISTER);
    }

    Pair<String, String> query = ImmutablePair.of(database, collection);
    queryMap.put(key, query);
  }

  @Override
  public void addCasId(String queryKey, String casId) throws Exception {
    LOGGER.trace("Writing Cas ID " + casId + "from the metadata store " + queryKey + ".");

    if (client == null) {
      LOGGER.trace("There is no connection to the MongoDB server.");

      throw new NullPointerException(ErrorMessages.METADATA_STORE_NOT_CONNECTED);
    } else if (StringUtils.isEmpty(casId)) {
      LOGGER.trace("Invalid Cas ID specified.");

      throw new IllegalArgumentException(ErrorMessages.INVALID_CAS_ID);
    }

    Pair<String, String> query = queryMap.get(queryKey);
    if (query == null) {
      LOGGER.trace("Invalid metadata store query specified.");

      throw new IllegalArgumentException(ErrorMessages.getInvalidQueryMessage(queryKey));
    }

    MongoCollection<Document> collection =
        client.getDatabase(query.getKey()).getCollection(query.getValue());

    if (collection.countDocuments(Filters.eq(MongoDbDocument.CASID_FIELD_KEY, casId)) > 0) {
      LOGGER.trace("An entry for this Cas ID already exists: " + casId + ".");

      throw new IllegalMetadataStoreStateException(ErrorMessages.CASID_NOT_UNIQUE);
    }

    Document doc = new Document();
    doc.append(MongoDbDocument.CASID_FIELD_KEY, casId);
    collection.insertOne(doc);
  }

  @Override
  public void deleteCasId(String queryKey, String casId) throws Exception {
    LOGGER.trace("Deleting Cas ID " + casId + "from the metadata store " + queryKey + ".");

    if (client == null) {
      LOGGER.trace("There is no connection to the MongoDB server.");

      throw new NullPointerException(ErrorMessages.METADATA_STORE_NOT_CONNECTED);
    } else if (StringUtils.isEmpty(casId)) {
      LOGGER.trace("Invalid Cas ID specified.");

      throw new IllegalArgumentException(ErrorMessages.INVALID_CAS_ID);
    }

    Pair<String, String> query = queryMap.get(queryKey);

    if (query == null) {
      LOGGER.trace("Invalid metadata store query specified.");

      throw new IllegalArgumentException(ErrorMessages.getInvalidQueryMessage(queryKey));
    }

    MongoCollection<Document> collection =
        client.getDatabase(query.getKey()).getCollection(query.getValue());

    Bson filter = Filters.all(MongoDbDocument.CASID_FIELD_KEY, casId);
    if (collection.countDocuments(filter) > 1) {
      LOGGER.trace("There are two or more entries for this Cas ID: " + casId + ".");

      throw new IllegalMetadataStoreStateException(ErrorMessages.CASID_NOT_UNIQUE);
    }
    collection.deleteOne(filter);
  }

  @Override
  public void deleteAllCasId(String casId) throws Exception {
    LOGGER.trace("Deleting Cas ID " + casId + "from all metadata stores.");

    for (String key : queryMap.keySet()) {
      deleteCasId(key, casId);
    }
  }

  @Override
  public void cleanup() {
    LOGGER.trace("Closing.");

    client.close();
  }

  public MongoClient getClient() {
    return client;
  }
}
