package com.revistek.util;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.revistek.crs.protos.Cas;
import com.revistek.exceptions.IllegalRepositoryStateException;
import com.revistek.exceptions.MalformedDataException;
import com.revistek.util.constants.ErrorMessages;
import com.revistek.util.constants.MongoDbDocument;
import de.svenkubiak.embeddedmongodb.EmbeddedMongoDB;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestMongoDbRepositoryDao {
  public static int PORT = 12345;
  public static String URL = "mongodb://localhost:" + PORT;
  public static String DATABASE = "testdatabase";
  public static String COLLECTION = "testcollection";

  private static EmbeddedMongoDB mongoDbServer;

  private final IdGenerator mockIdGenerator = Mockito.mock(IdGenerator.class);
  private MongoDbRepositoryDao mockDao;

  @BeforeAll
  public static void setupMongoDb() {
    mongoDbServer = EmbeddedMongoDB.create().withPort(PORT).start();

    MongoClient client = MongoClients.create(URL);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    Document doc = new Document();
    doc.append("_id", 1);

    collection.insertOne(doc);
    collection.deleteMany(Filters.eq("_id", 1));
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
    mockDao =
        MongoDbRepositoryDao.newBuilder()
            .url(URL)
            .idGenerator(mockIdGenerator)
            .database(DATABASE)
            .collection(COLLECTION)
            .build();
  }

  @Test
  public void testCreate() {
    try (MockedStatic<MongoClients> mockMongoClients = Mockito.mockStatic(MongoClients.class)) {
      IdGenerator mockIdGenerator = Mockito.mock(IdGenerator.class);
      MongoClient mockMongoClient = Mockito.mock(MongoClient.class);
      mockMongoClients.when(() -> MongoClients.create("url")).thenReturn(mockMongoClient);

      MongoDbRepositoryDao dao =
          MongoDbRepositoryDao.newBuilder()
              .url("url")
              .idGenerator(mockIdGenerator)
              .database("database")
              .collection("collection")
              .build();
      assertEquals(mockMongoClient, dao.getClient());
      assertEquals(mockIdGenerator, dao.getIdGenerator());
      assertEquals("database", dao.getDatabase());
      assertEquals("collection", dao.getCollection());
    }
  }

  @Test
  public void testGetNullCient() {
    try (MockedStatic<MongoClients> mockMongoClients = Mockito.mockStatic(MongoClients.class)) {
      IdGenerator mockIdGenerator = Mockito.mock(IdGenerator.class);
      mockMongoClients.when(() -> MongoClients.create("url")).thenReturn(null);

      MongoDbRepositoryDao dao =
          MongoDbRepositoryDao.newBuilder()
              .url("url")
              .idGenerator(mockIdGenerator)
              .database("database")
              .collection("collection")
              .build();
      assertNull(dao.getClient());

      NullPointerException exception =
          assertThrows(
              NullPointerException.class,
              () -> dao.get(Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId")));
      assertEquals(ErrorMessages.REPOSITORY_NOT_CONNECTED, exception.getMessage());
    }
  }

  @Test
  public void testGetMultipleCasIds() {
    MongoClient client = MongoClients.create(URL);
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    assertEquals(0L, collection.countDocuments());

    Document doc1 = new Document();
    doc1.append("_id", 1)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc1);

    Document doc2 = new Document();
    doc2.append("_id", 2)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc2);

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertEquals(2L, collection.countDocuments(filter));

    IllegalRepositoryStateException exception =
        assertThrows(IllegalRepositoryStateException.class, () -> mockDao.get(filter));
    assertEquals(ErrorMessages.CASID_NOT_UNIQUE, exception.getMessage());

    collection.deleteMany(filter);
    client.close();
  }

  @Test
  public void testGet() throws Exception {
    MongoClient client = MongoClients.create(URL);
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    assertEquals(0L, collection.countDocuments());

    Document doc = new Document();
    doc.append("_id", 1)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc);

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    Cas cas = mockDao.get(filter);
    assertEquals("casId", cas.getCasId());
    assertEquals("documentId", cas.getDocumentId());
    assertEquals(9L, cas.getCrc32Checksum());
    assertEquals(data, cas.getCasData());

    collection.deleteMany(filter);
    client.close();
  }

  @Test
  public void testGetCasId() throws Exception {
    MongoClient client = MongoClients.create(URL);
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    assertEquals(0L, collection.countDocuments());

    Document doc = new Document();
    doc.append("_id", 1)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc);

    Cas cas = mockDao.getCasId("casId");
    assertEquals("casId", cas.getCasId());
    assertEquals("documentId", cas.getDocumentId());
    assertEquals(9L, cas.getCrc32Checksum());
    assertEquals(data, cas.getCasData());

    collection.deleteMany(Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId"));
    client.close();
  }

  @Test
  public void testGetCasIdInvalidCasId() {
    Exception exception = assertThrows(Exception.class, () -> mockDao.getCasId(""));
    assertEquals(ErrorMessages.INVALID_CAS_ID, exception.getMessage());
  }

  @Test
  public void testStoreInvalidCas() {
    final Cas cas1 = Cas.newBuilder().build();
    MalformedDataException exception1 =
        assertThrows(MalformedDataException.class, () -> mockDao.store(cas1));
    assertEquals(ErrorMessages.INVALID_PROTOBUF_CAS, exception1.getMessage());

    final Cas cas2 = Cas.newBuilder().setDocumentId("docId").build();
    MalformedDataException exception2 =
        assertThrows(MalformedDataException.class, () -> mockDao.store(cas2));
    assertEquals(ErrorMessages.INVALID_PROTOBUF_CAS, exception2.getMessage());

    ByteString data = ByteString.copyFrom("", StandardCharsets.UTF_8);
    final Cas cas3 = Cas.newBuilder().setDocumentId("docId").setCasData(data).build();
    MalformedDataException exception3 =
        assertThrows(MalformedDataException.class, () -> mockDao.store(cas3));
    assertEquals(ErrorMessages.INVALID_PROTOBUF_CAS, exception3.getMessage());
  }

  @Test
  public void testStoreChecksumFailed() {
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    byte[] dataBytes = data.toByteArray();
    Checksum crc32 = new CRC32();
    crc32.update(dataBytes, 0, dataBytes.length);
    assertNotEquals(9L, crc32.getValue());

    Cas cas = Cas.newBuilder().setDocumentId("docId").setCrc32Checksum(9).setCasData(data).build();
    MalformedDataException exception =
        assertThrows(MalformedDataException.class, () -> mockDao.store(cas));
    assertEquals(ErrorMessages.getChecksumFailedMessage("docId"), exception.getMessage());
  }

  @Test
  public void testStore() throws Exception {
    MongoClient client = MongoClients.create(URL);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    assertEquals(0L, collection.countDocuments());
    Mockito.when(mockIdGenerator.refreshAndGetUniqueId()).thenReturn("casId");

    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    byte[] dataBytes = data.toByteArray();
    Checksum crc32 = new CRC32();
    crc32.update(dataBytes, 0, dataBytes.length);

    Cas cas =
        Cas.newBuilder()
            .setDocumentId("docId")
            .setCrc32Checksum(crc32.getValue())
            .setCasData(data)
            .build();
    mockDao.store(cas);

    assertEquals(1L, collection.countDocuments());
    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");

    FindIterable<Document> iter = collection.find(filter);
    Document doc = iter.first();
    assertEquals("casId", doc.getString(MongoDbDocument.CASID_FIELD_KEY));
    assertEquals("docId", doc.getString(MongoDbDocument.DOCUMENTID_FIELD_KEY));

    collection.deleteMany(filter);
  }

  @Test
  public void testDeleteNullCient() throws Exception {
    try (MockedStatic<MongoClients> mockMongoClients = Mockito.mockStatic(MongoClients.class)) {
      IdGenerator mockIdGenerator = Mockito.mock(IdGenerator.class);
      mockMongoClients.when(() -> MongoClients.create("url")).thenReturn(null);

      MongoDbRepositoryDao dao =
          MongoDbRepositoryDao.newBuilder()
              .url("url")
              .idGenerator(mockIdGenerator)
              .database("database")
              .collection("collection")
              .build();
      assertNull(dao.getClient());

      NullPointerException exception =
          assertThrows(
              NullPointerException.class,
              () -> dao.delete(Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId")));
      assertEquals(ErrorMessages.REPOSITORY_NOT_CONNECTED, exception.getMessage());
    }
  }

  @Test
  public void testDeleteDuplicateCasIds() {
    MongoClient client = MongoClients.create(URL);
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    assertEquals(0L, collection.countDocuments());

    Document doc1 = new Document();
    doc1.append("_id", 1)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc1);

    Document doc2 = new Document();
    doc2.append("_id", 2)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc2);

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertEquals(2L, collection.countDocuments(filter));

    IllegalRepositoryStateException exception =
        assertThrows(IllegalRepositoryStateException.class, () -> mockDao.delete(filter));
    assertEquals(ErrorMessages.CASID_NOT_UNIQUE, exception.getMessage());

    collection.deleteMany(filter);
    client.close();
  }

  @Test
  public void testDelete() throws Exception {
    MongoClient client = MongoClients.create(URL);
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    assertEquals(0L, collection.countDocuments());

    Document doc = new Document();
    doc.append("_id", 1)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc);

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertEquals(1L, collection.countDocuments(filter));

    mockDao.delete(filter);
    assertEquals(0L, collection.countDocuments(filter));
  }

  @Test
  public void testDeleteCasId() throws Exception {
    MongoClient client = MongoClients.create(URL);
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    assertEquals(0L, collection.countDocuments());

    Document doc = new Document();
    doc.append("_id", 1)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc);

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertEquals(1L, collection.countDocuments(filter));

    mockDao.deleteCasId("casId");
    assertEquals(0L, collection.countDocuments(filter));
  }

  @Test
  public void testDeleteCasIdInvalidCasId() throws Exception {
    Exception exception = assertThrows(Exception.class, () -> mockDao.deleteCasId(""));
    assertEquals(ErrorMessages.INVALID_CAS_ID, exception.getMessage());
  }

  @Test
  public void testCastoDocumentNullCas() throws Exception {
    Exception exception =
        assertThrows(Exception.class, () -> MongoDbRepositoryDao.casToDocument(null));
    assertEquals(ErrorMessages.INVALID_PROTOBUF_CAS, exception.getMessage());
  }

  @Test
  public void testCastoDocument() throws Exception {
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    Cas cas =
        Cas.newBuilder()
            .setCasId("casId")
            .setDocumentId("docId")
            .setCrc32Checksum(9)
            .setCasData(data)
            .build();
    Document doc = MongoDbRepositoryDao.casToDocument(cas);
    assertEquals("casId", doc.get(MongoDbDocument.CASID_FIELD_KEY));
    assertEquals("docId", doc.get(MongoDbDocument.DOCUMENTID_FIELD_KEY));
    assertEquals(9L, doc.get(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY));
    assertEquals(data, doc.get(MongoDbDocument.CASDATA_FIELD_KEY));
  }

  @Test
  public void testDocumentToCasNullDocument() throws Exception {
    Exception exception =
        assertThrows(Exception.class, () -> MongoDbRepositoryDao.documentToCas(null));
    assertEquals(ErrorMessages.INVALID_REPOSITORY_ENTRY, exception.getMessage());
  }

  @Test
  public void testDocumentToCasIncompleteDocument() throws Exception {
    Document doc = new Document();
    doc.append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L);

    Exception exception =
        assertThrows(Exception.class, () -> MongoDbRepositoryDao.documentToCas(doc));
    assertEquals(ErrorMessages.INVALID_REPOSITORY_ENTRY, exception.getMessage());
  }

  @Test
  public void testDocumentToCas() throws Exception {
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    byte[] bytes = data.toByteArray();
    ArrayList<Integer> bytesAsIntegers = new ArrayList<Integer>();
    for (int i = 0; i < bytes.length; i++) {
      int val = bytes[i];
      bytesAsIntegers.add(Integer.valueOf(val));
    }

    Document doc = new Document();
    doc.append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, bytesAsIntegers);

    Cas cas = MongoDbRepositoryDao.documentToCas(doc);
    assertEquals("casId", cas.getCasId());
    assertEquals("documentId", cas.getDocumentId());
    assertEquals(9L, cas.getCrc32Checksum());
    assertEquals(data, cas.getCasData());
  }

  @Test
  public void testExistsNullCient() throws Exception {
    try (MockedStatic<MongoClients> mockMongoClients = Mockito.mockStatic(MongoClients.class)) {
      IdGenerator mockIdGenerator = Mockito.mock(IdGenerator.class);
      mockMongoClients.when(() -> MongoClients.create("url")).thenReturn(null);

      MongoDbRepositoryDao dao =
          MongoDbRepositoryDao.newBuilder()
              .url("url")
              .idGenerator(mockIdGenerator)
              .database("database")
              .collection("collection")
              .build();
      assertNull(dao.getClient());

      NullPointerException exception =
          assertThrows(
              NullPointerException.class,
              () -> dao.exists(Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId")));
      assertEquals(ErrorMessages.REPOSITORY_NOT_CONNECTED, exception.getMessage());
    }
  }

  @Test
  public void testExists() throws Exception {
    MongoClient client = MongoClients.create(URL);
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    assertEquals(0L, collection.countDocuments());

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId");
    assertFalse(mockDao.exists(filter));

    Document doc = new Document();
    doc.append("_id", 1)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc);

    assertTrue(mockDao.exists(filter));

    collection.deleteMany(filter);
    client.close();
  }

  @Test
  public void testExistsCasId() throws Exception {
    MongoClient client = MongoClients.create(URL);
    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    MongoCollection<Document> collection = client.getDatabase(DATABASE).getCollection(COLLECTION);
    assertEquals(0L, collection.countDocuments());

    Document doc = new Document();
    doc.append("_id", 1)
        .append(MongoDbDocument.CASID_FIELD_KEY, "casId")
        .append(MongoDbDocument.DOCUMENTID_FIELD_KEY, "documentId")
        .append(MongoDbDocument.CRC32CHECKSUM_FIELD_KEY, 9L)
        .append(MongoDbDocument.CASDATA_FIELD_KEY, data);
    collection.insertOne(doc);

    assertTrue(mockDao.existsCasId("casId"));

    collection.deleteMany(Filters.eq(MongoDbDocument.CASID_FIELD_KEY, "casId"));
    client.close();
  }

  @Test
  public void testExistsCasIdInvalidCasId() throws Exception {
    Exception exception = assertThrows(Exception.class, () -> mockDao.existsCasId(""));
    assertEquals(ErrorMessages.INVALID_CAS_ID, exception.getMessage());
  }
}
