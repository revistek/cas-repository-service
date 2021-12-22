package com.revistek.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.revistek.crs.constants.MetadataStoreQueries;
import com.revistek.exceptions.IllegalMetadataStoreStateException;
import com.revistek.util.constants.ErrorMessages;
import com.revistek.util.constants.MongoDbDocument;
import de.svenkubiak.embeddedmongodb.EmbeddedMongoDB;
import java.util.Arrays;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestMongoDbMetadataStoreDao {
  public static int PORT = 12345;
  public static String URL = "mongodb://localhost:" + PORT;
  public static String DATABASE = "testdatabase";
  public static String COLLECTION_PREPROCESSOR = MetadataStoreQueries.QUERY_KEY_PREPROCESSOR;
  public static String COLLECTION_PIPELINE_A = MetadataStoreQueries.QUERY_KEY_PIPELINEA;

  private static EmbeddedMongoDB mongoDbServer;

  private MongoDbMetadataStoreDao dao;

  @BeforeAll
  public static void setupMongoDb() {
    mongoDbServer = EmbeddedMongoDB.create().withPort(PORT).start();

    MongoClient client = MongoClients.create(URL);
    Document doc = new Document();
    doc.append("_id", 1);
    Bson filter = Filters.eq("_id", 1);

    MongoCollection<Document> collection1 =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PREPROCESSOR);
    collection1.insertOne(doc);
    collection1.deleteMany(filter);

    MongoCollection<Document> collection2 =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PIPELINE_A);
    collection2.insertOne(doc);
    collection2.deleteMany(filter);

    client.close();
  }

  @AfterAll
  public static void teardownMongoDb() {
    MongoClient client = MongoClients.create(URL);
    client.getDatabase(DATABASE).drop();
    client.close();

    mongoDbServer.stop();
  }

  @BeforeEach
  public void setup() {
    dao = new MongoDbMetadataStoreDao(URL);
    dao.registerQuery(
        MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, DATABASE, COLLECTION_PREPROCESSOR);
    dao.registerQuery(MetadataStoreQueries.QUERY_KEY_PIPELINEA, DATABASE, COLLECTION_PIPELINE_A);
  }

  @Test
  public void testCreate() {
    try (MockedStatic<MongoClients> mockMongoClients = Mockito.mockStatic(MongoClients.class)) {
      MongoClient mockMongoClient = Mockito.mock(MongoClient.class);
      mockMongoClients.when(() -> MongoClients.create("url")).thenReturn(mockMongoClient);

      MongoDbMetadataStoreDao dao = new MongoDbMetadataStoreDao("url");
      assertEquals(mockMongoClient, dao.getClient());
    }
  }

  @Test
  public void testRegisterQueryInvalidQuery() throws Exception {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> dao.registerQuery("", "database", "collection"));
    assertEquals(ErrorMessages.INVALID_METADATA_STORE_QUERY_REGISTER, exception.getMessage());

    exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> dao.registerQuery(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, "", "collection"));
    assertEquals(ErrorMessages.INVALID_METADATA_STORE_QUERY_REGISTER, exception.getMessage());

    exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> dao.registerQuery(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, "database", ""));
    assertEquals(ErrorMessages.INVALID_METADATA_STORE_QUERY_REGISTER, exception.getMessage());
  }

  @Test
  public void testAddCasIdNullClient() {
    try (MockedStatic<MongoClients> mockMongoClients = Mockito.mockStatic(MongoClients.class)) {
      mockMongoClients.when(() -> MongoClients.create("url")).thenReturn(null);

      MongoDbMetadataStoreDao dao = new MongoDbMetadataStoreDao("url");
      assertEquals(null, dao.getClient());

      NullPointerException exception =
          assertThrows(
              NullPointerException.class,
              () -> dao.addCasId(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, "casId"));
      assertEquals(ErrorMessages.METADATA_STORE_NOT_CONNECTED, exception.getMessage());
    }
  }

  @Test
  public void testAddCasIdNoCasId() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> dao.addCasId(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, ""));
    assertEquals(ErrorMessages.INVALID_CAS_ID, exception.getMessage());
  }

  @Test
  public void testAddCasIdInvalidQuery() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> dao.addCasId("noQuery", "casId"));
    assertEquals(ErrorMessages.getInvalidQueryMessage("noQuery"), exception.getMessage());
  }

  @Test
  public void testAddCasIdDuplicateCasId() {
    MongoClient client = MongoClients.create(URL);
    MongoCollection<Document> collection =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PREPROCESSOR);
    assertEquals(0L, collection.countDocuments());

    Document doc = new Document();
    doc.append(MongoDbDocument.CASID_FIELD_KEY, "casId");
    collection.insertOne(doc);
    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertEquals(1L, collection.countDocuments(filter));

    IllegalMetadataStoreStateException exception =
        assertThrows(
            IllegalMetadataStoreStateException.class,
            () -> dao.addCasId(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, "casId"));
    assertEquals(ErrorMessages.CASID_NOT_UNIQUE, exception.getMessage());
    collection.deleteMany(filter);
  }

  @Test
  public void testAddCasId() throws Exception {
    MongoClient client = MongoClients.create(URL);
    MongoCollection<Document> collection1 =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PREPROCESSOR);
    MongoCollection<Document> collection2 =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PIPELINE_A);
    assertEquals(0L, collection1.countDocuments());
    assertEquals(0L, collection2.countDocuments());

    dao.addCasId(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, "casId");
    dao.addCasId(MetadataStoreQueries.QUERY_KEY_PIPELINEA, "casId");

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertEquals(1L, collection1.countDocuments());
    assertEquals(1L, collection2.countDocuments());
    collection1.deleteMany(filter);
    collection2.deleteMany(filter);
  }

  @Test
  public void testDeleteCasIdNullClient() {
    try (MockedStatic<MongoClients> mockMongoClients = Mockito.mockStatic(MongoClients.class)) {
      mockMongoClients.when(() -> MongoClients.create("url")).thenReturn(null);

      MongoDbMetadataStoreDao dao = new MongoDbMetadataStoreDao("url");
      assertEquals(null, dao.getClient());

      NullPointerException exception =
          assertThrows(
              NullPointerException.class,
              () -> dao.deleteCasId(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, "casId"));
      assertEquals(ErrorMessages.METADATA_STORE_NOT_CONNECTED, exception.getMessage());
    }
  }

  @Test
  public void testDeleteCasIdNoCasId() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> dao.deleteCasId(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, ""));
    assertEquals(ErrorMessages.INVALID_CAS_ID, exception.getMessage());
  }

  @Test
  public void testDeleteCasIdInvalidQuery() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> dao.deleteCasId("noQuery", "casId"));
    assertEquals(ErrorMessages.getInvalidQueryMessage("noQuery"), exception.getMessage());
  }

  @Test
  public void testDeleteCasIdDuplicateCasId() {
    MongoClient client = MongoClients.create(URL);
    MongoCollection<Document> collection =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PREPROCESSOR);
    assertEquals(0L, collection.countDocuments());

    Document doc1 = new Document();
    doc1.append(MongoDbDocument.CASID_FIELD_KEY, "casId");
    Document doc2 = new Document();
    doc2.append(MongoDbDocument.CASID_FIELD_KEY, "casId");
    collection.insertMany(Arrays.asList(doc1, doc2));
    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertEquals(2L, collection.countDocuments(filter));

    IllegalMetadataStoreStateException exception =
        assertThrows(
            IllegalMetadataStoreStateException.class,
            () -> dao.deleteCasId(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, "casId"));
    assertEquals(ErrorMessages.CASID_NOT_UNIQUE, exception.getMessage());
    collection.deleteMany(filter);
  }

  @Test
  public void testDeleteCasId() throws Exception {
    MongoClient client = MongoClients.create(URL);
    MongoCollection<Document> collection1 =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PREPROCESSOR);
    MongoCollection<Document> collection2 =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PIPELINE_A);
    assertEquals(0L, collection1.countDocuments());
    assertEquals(0L, collection2.countDocuments());

    Document doc = new Document();
    doc.append(MongoDbDocument.CASID_FIELD_KEY, "casId");
    collection1.insertOne(doc);
    collection2.insertOne(doc);
    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertEquals(1L, collection1.countDocuments(filter));
    assertEquals(1L, collection2.countDocuments(filter));

    dao.deleteCasId(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR, "casId");
    assertEquals(0L, collection1.countDocuments(filter));
    assertEquals(1L, collection2.countDocuments(filter));

    dao.deleteCasId(MetadataStoreQueries.QUERY_KEY_PIPELINEA, "casId");
    assertEquals(0L, collection1.countDocuments(filter));
    assertEquals(0L, collection2.countDocuments(filter));
  }

  @Test
  public void testDeleteAllCasId() throws Exception {
    MongoClient client = MongoClients.create(URL);
    MongoCollection<Document> collection1 =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PREPROCESSOR);
    MongoCollection<Document> collection2 =
        client.getDatabase(DATABASE).getCollection(COLLECTION_PIPELINE_A);
    assertEquals(0L, collection1.countDocuments());
    assertEquals(0L, collection2.countDocuments());

    Document doc = new Document();
    doc.append(MongoDbDocument.CASID_FIELD_KEY, "casId");
    collection1.insertOne(doc);
    collection2.insertOne(doc);
    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertEquals(1L, collection1.countDocuments(filter));
    assertEquals(1L, collection2.countDocuments(filter));

    dao.deleteAllCasId("casId");
    assertEquals(0L, collection1.countDocuments(filter));
    assertEquals(0L, collection2.countDocuments(filter));
  }
}
