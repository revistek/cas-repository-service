package com.revistek.util;

import com.google.protobuf.ByteString;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.revistek.crs.protos.Cas;
import com.revistek.exceptions.IllegalRepositoryStateException;
import com.revistek.exceptions.MalformedDataException;
import com.revistek.util.constants.ErrorMessages;
import com.revistek.util.constants.MongoDbDocument;
import java.util.ArrayList;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a repository store DAO for MongoDB.
 *
 * @author Chuong Ngo
 */
public class MongoDbRepositoryDao implements RepositoryDao {
  private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryDao.class);

  private MongoClient client;
  private IdGenerator idGenerator;
  private String database;
  private String collection;

  private MongoDbRepositoryDao() {}

  public void createClient(String url) {
    client = MongoClients.create(url);

    LOGGER.trace("Initialized.");
  }

  @Override
  public Cas getCasId(String casId) throws Exception {
    LOGGER.trace("Getting the Cas: " + casId + ".");

    if (StringUtils.isEmpty(casId)) {
        LOGGER.trace("Invalid Cas ID specified.");

      throw new IllegalArgumentException(ErrorMessages.INVALID_CAS_ID);
    }

    Bson filter = Filters.all(MongoDbDocument.CASID_FIELD_KEY, casId);
    return get(filter);
  }

  /**
   * Gets a {@link com.revistek.crs.protos.Cas Cas} from the database.
   * 
   * @param filter - The MongoDB filter to use to search for the Cas.
   * @return The retrieved and reconstituted {@link com.revistek.crs.protos.Cas Cas}.
   * 
   * @throws Exception There was a problem with this operation.
   */
  public Cas get(Bson filter) throws Exception {
    if (client == null) {
        LOGGER.trace("There is no connection to the MongoDB server.");

      throw new NullPointerException(ErrorMessages.REPOSITORY_NOT_CONNECTED);
    }

    MongoCollection<Document> mongoCollection =
        client.getDatabase(database).getCollection(collection);

    if (mongoCollection.countDocuments(filter) > 1) {
      throw new IllegalRepositoryStateException(ErrorMessages.CASID_NOT_UNIQUE);
    }

    FindIterable<Document> iter = mongoCollection.find(filter);
    Document doc = iter.first();
    return documentToCas(doc);
  }

  @Override
  public String store(Cas cas) throws Exception {
    String docId = cas.getDocumentId();
    ByteString data = cas.getCasData();

    if (StringUtils.isEmpty(docId)) {
        LOGGER.trace("Invalid document ID specified.");

      throw new MalformedDataException(ErrorMessages.INVALID_PROTOBUF_CAS);
    }

    byte[] dataBytes = data.toByteArray();

    if (dataBytes.length == 0) {
        LOGGER.trace("There is no UIMA Cas/JCas data to store.");

      throw new MalformedDataException(ErrorMessages.INVALID_PROTOBUF_CAS);
    }

    Checksum crc32 = new CRC32();
    crc32.update(dataBytes, 0, dataBytes.length);

    if (crc32.getValue() != cas.getCrc32Checksum()) {
        LOGGER.trace("The UIMA Cas/JCas failed its checksum check.");
        
      throw new MalformedDataException(ErrorMessages.getChecksumFailedMessage(docId));
    }

    String casId = idGenerator.refreshAndGetUniqueId();
    LOGGER.trace("Storing the Cas: " + casId + " for document: " + docId + ".");
    
    if (client == null) {
        LOGGER.trace("There is no connection to the MongoDB server.");

      throw new NullPointerException(ErrorMessages.REPOSITORY_NOT_CONNECTED);
    }

    Cas newCas =
        Cas.newBuilder()
            .setCasId(casId)
            .setDocumentId(docId)
            .setCrc32Checksum(crc32.getValue())
            .setCasData(data)
            .build();
    Document doc = MongoDbRepositoryDao.casToDocument(newCas);
    MongoDatabase mongoDb = client.getDatabase(database);
    MongoCollection<Document> mongoCollection = mongoDb.getCollection(collection);
    mongoCollection.insertOne(doc);
    
    return casId;
  }

  @Override
  public void deleteCasId(String casId) throws Exception {
    LOGGER.trace("Deleting the Cas: " + casId + ".");

    if (StringUtils.isEmpty(casId)) {
        LOGGER.trace("Invalid Cas ID specified.");

      throw new IllegalArgumentException(ErrorMessages.INVALID_CAS_ID);
    }

    Bson filter = Filters.all(MongoDbDocument.CASID_FIELD_KEY, casId);
    this.delete(filter);
  }

  /**
   * Deletes a {@link com.revistek.crs.protos.Cas Cas} from the database.
   * 
   * @param filter - The MongoDB filter to use to search for the Cas.
   * 
   * @throws Exception There was a problem with this operation.
   */
  public void delete(Bson filter) throws Exception {
    if (client == null) {
        LOGGER.trace("There is no connection to the MongoDB server.");

      throw new NullPointerException(ErrorMessages.REPOSITORY_NOT_CONNECTED);
    }

    MongoCollection<Document> mongoCollection =
        client.getDatabase(database).getCollection(collection);

    if (mongoCollection.countDocuments(filter) > 1) {
        LOGGER.trace("There are two or more entries for the specified Cas.");
        
      throw new IllegalRepositoryStateException(ErrorMessages.CASID_NOT_UNIQUE);
    }

    mongoCollection.deleteOne(filter);
  }

  @Override
  public boolean existsCasId(String casId) throws Exception {
    if (StringUtils.isEmpty(casId)) {
        LOGGER.trace("Invalid Cas ID specified.");

      throw new IllegalArgumentException(ErrorMessages.INVALID_CAS_ID);
    }

    Bson filter = Filters.all(MongoDbDocument.CASID_FIELD_KEY, casId);
    return exists(filter);
  }

  public boolean exists(Bson filter) throws Exception {
    if (client == null) {
        LOGGER.trace("There is no connection to the MongoDB server.");

      throw new NullPointerException(ErrorMessages.REPOSITORY_NOT_CONNECTED);
    }

    MongoCollection<Document> mongoCollection =
        client.getDatabase(database).getCollection(collection);
    return (mongoCollection.countDocuments(filter) > 0);
  }

  @Override
  public void cleanup() {
    LOGGER.trace("Closing.");

    client.close();
  }

  public MongoClient getClient() {
    return client;
  }

  public IdGenerator getIdGenerator() {
    return idGenerator;
  }

  public String getDatabase() {
    return database;
  }

  public String getCollection() {
    return collection;
  }

  /**
   * The Builder for {@link com.revistek.util.MongoDbRepositoryDao MongoDbRepositoryDao}.
   * 
   * @author Chuuong Ngo
   */
  public static class Builder {
    private IdGenerator idGenerator;
    private String url;
    private String database;
    private String collection;

    public Builder idGenerator(IdGenerator idGenerator) {
      this.idGenerator = idGenerator;
      return this;
    }

    public Builder url(String url) {
      this.url = url;
      return this;
    }

    public Builder database(String database) {
      this.database = database;
      return this;
    }

    public Builder collection(String collection) {
      this.collection = collection;
      return this;
    }

    public MongoDbRepositoryDao build() throws IllegalArgumentException {
      if (StringUtils.isEmpty(url)) {
        throw new IllegalArgumentException(ErrorMessages.INVALID_REPOSITORY_URL);
      }

      if (idGenerator == null) {
        throw new IllegalArgumentException(ErrorMessages.INVALID_REPOSITORY_IDGENERATOR);
      }

      if (StringUtils.isEmpty(database)) {
        throw new IllegalArgumentException(ErrorMessages.INVALID_MONGODB_REPOSITORY_DATABASE);
      }

      if (StringUtils.isEmpty(collection)) {
        throw new IllegalArgumentException(ErrorMessages.INVALID_MONGODB_REPOSITORY_COLLECTION);
      }

      MongoDbRepositoryDao dao = new MongoDbRepositoryDao();
      dao.client = MongoClients.create(url);
      dao.idGenerator = idGenerator;
      dao.database = database;
      dao.collection = collection;

      return dao;
    }
  }

  /**
   * Returns a new builder.
   *
   * @return Returns a new {@link com.revistek.util.MongoDbRepositoryDao.Builder builder}.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Converts a {@link com.revistek.protos.Cas Cas} into a MongoDB {@link org.bson.Document
   * Document}.
   *
   * @param cas the {@link com.revistek.protos.Cas Cas} to convert.
   * @return the {@link org.bson.Document Document} equivalent of the CAS.
   */
  public static Document casToDocument(Cas cas) throws Exception {
    LOGGER.trace("Converting a Cas to a MongoDB document.");

    if (cas == null) {
    	LOGGER.trace("The Cas is invalid.");
    	
      throw new IllegalArgumentException(ErrorMessages.INVALID_PROTOBUF_CAS);
    }

    Document doc = new Document();

    doc.append(MongoDbDocument.CASID_FIELD_KEY, cas.getCasId())
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, cas.getDocumentId())
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, cas.getCrc32Checksum())
        .append(MongoDbDocument.CASDATA_FIELD_KEY, cas.getCasData());

    return doc;
  }

  /**
   * Converts a MongoDB {@link org.bson.Document Document} into a {@link com.revistek.protos.Cas
   * Cas}.
   *
   * @param doc the {@link org.bson.Document Document} to convert.
   * @return the {@link com.revistek.protos.Cas Cas} equivalent of the {@link org.bson.Document
   *     Document}.
   */
  public static Cas documentToCas(Document doc) throws Exception {
    LOGGER.trace("Converting a MongoDB document to a Cas.");

    if (doc == null) {
    	LOGGER.trace("The Document is invalid.");
    	
      throw new IllegalArgumentException(ErrorMessages.INVALID_REPOSITORY_ENTRY);
    }

    String casId = doc.getString(MongoDbDocument.CASID_FIELD_KEY);
    String docId = doc.getString(MongoDbDocument.DOCUMENTID_FIELD_KEY);
    Long checksum = doc.getLong(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY);

    @SuppressWarnings("unchecked")
    ArrayList<Integer> bytes = (ArrayList<Integer>) doc.get(MongoDbDocument.CASDATA_FIELD_KEY);

    if ((casId == null) || (docId == null) || (checksum == null) || (bytes == null)) {
    	LOGGER.trace("The Document is missing data required for the Cas.");
    	
      throw new IllegalArgumentException(ErrorMessages.INVALID_REPOSITORY_ENTRY);
    }

    Cas cas =
        Cas.newBuilder()
            .setCasId(casId)
            .setDocumentId(docId)
            .setCrc32Checksum(checksum)
            .setCasData(
                ByteString.copyFrom(
                    ArrayUtils.toPrimitive(
                        bytes.stream().map(entry -> entry.byteValue()).toArray(Byte[]::new))))
            .build();

    return cas;
  }
}
