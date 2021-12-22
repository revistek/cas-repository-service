package com.revistek.web.resources;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.revistek.crs.constants.Cache;
import com.revistek.crs.constants.MetadataStoreQueries;
import com.revistek.crs.protos.Cas;
import com.revistek.crs.protos.Message;
import com.revistek.crs.protos.constants.MessageCodes;
import com.revistek.crs.protos.constants.MessageExceptions;
import com.revistek.crs.protos.constants.ProtobufMessages;
import com.revistek.crs.protos.net.jersey.ProtobufMessageBodyHandler;
import com.revistek.net.constants.MediaTypes;
import com.revistek.net.constants.StatusCodes;
import com.revistek.util.constants.MongoDbDocument;
import de.svenkubiak.embeddedmongodb.EmbeddedMongoDB;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPooled;
import redis.embedded.RedisServer;

public class TestCasRepositoryServiceResource extends RestTest {
  public static final int REDIS_PORT = 12345;
  public static final String REDIS_URL = "redis://localhost:" + String.valueOf(REDIS_PORT);

  public static final int MONGODB_PORT = 12346;
  public static final String MONGODB_URL = "mongodb://localhost:" + String.valueOf(MONGODB_PORT);
  public static final String REPO_DATABASE = "testrepo";
  public static final String REPO_COLLECTION = "testcollection";
  public static final String METADATA_DATABASE = "testmetadata";
  public static final String METADATA_COLLECTION_PREPROCESSOR =
      MetadataStoreQueries.QUERY_KEY_PREPROCESSOR;
  public static final String METADATA_COLLECTION_PIPELINE_A =
      MetadataStoreQueries.QUERY_KEY_PIPELINEA;
  public static final Map<String, String> queriesMap =
      new HashMap<String, String>() {
        private static final long serialVersionUID = 1L;

        {
          put(
              METADATA_COLLECTION_PREPROCESSOR,
              METADATA_DATABASE + "," + METADATA_COLLECTION_PREPROCESSOR);
          put(
              METADATA_COLLECTION_PIPELINE_A,
              METADATA_DATABASE + "," + METADATA_COLLECTION_PIPELINE_A);
        }
      };

  private static RedisServer redisServer;
  private static EmbeddedMongoDB mongoDbServer;

  @BeforeAll
  public static void setupMongoDb() {
    mongoDbServer = EmbeddedMongoDB.create().withPort(MONGODB_PORT).start();

    MongoClient client = MongoClients.create(MONGODB_URL);

    Document doc = new Document();
    doc.append("_id", 1);
    Bson filter = Filters.eq("_id", 1);

    MongoCollection<Document> collection =
        client.getDatabase(REPO_DATABASE).getCollection(REPO_COLLECTION);
    collection.insertOne(doc);
    collection.deleteMany(filter);

    MongoCollection<Document> collection1 =
        client.getDatabase(METADATA_DATABASE).getCollection(METADATA_COLLECTION_PREPROCESSOR);
    collection1.insertOne(doc);
    collection1.deleteMany(filter);

    MongoCollection<Document> collection2 =
        client.getDatabase(METADATA_DATABASE).getCollection(METADATA_COLLECTION_PIPELINE_A);
    collection2.insertOne(doc);
    collection2.deleteMany(filter);

    client.close();
  }

  @AfterAll
  public static void teardownMongoDb() {
    MongoClient client = MongoClients.create(MONGODB_URL);
    client.getDatabase(REPO_DATABASE).drop();
    client.getDatabase(METADATA_DATABASE).drop();
    client.close();

    mongoDbServer.stop();
  }

  @BeforeAll
  public static void setupRedis() {
    // Starting embedded server for testing.
    redisServer = new RedisServer(REDIS_PORT);
    redisServer.start();

    JedisPooled client = new JedisPooled(REDIS_URL);

    client.set(Cache.KEY_REPOSITORY_URL, MONGODB_URL);
    client.set(Cache.KEY_MONGODB_REPOSITORY_DATABASE, REPO_DATABASE);
    client.set(Cache.KEY_MONGODB_REPOSITORY_COLLECTION, REPO_COLLECTION);
    client.set(Cache.KEY_METDATA_STORE_URL, MONGODB_URL);
    client.hset(Cache.KEY_METDATA_STORE_QUERIES, queriesMap);

    client.close();
  }

  @AfterAll
  public static void teardownRedis() {
    redisServer.stop();
  }

  @Override
  protected ResourceConfig configure() {
    ResourceConfig config = new ResourceConfig(CasRepositoryServiceResource.class);
    config.register(ProtobufMessageBodyHandler.class);

    return config;
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(ProtobufMessageBodyHandler.class);
  }

  @Test
  public void testStoreNoCas() {
    Message outMessage = Message.newBuilder().setMessage("HI").build();
    Response response =
        target("/rest/store")
            .request()
            .post(Entity.entity(outMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, response.getStatus());

    Message returnMessage = response.readEntity(Message.class);
    assertEquals(MessageCodes.INVALID_MESSAGE, returnMessage.getStatusCode());
    assertEquals(MessageExceptions.ILLEGAL_ARGUMENT, returnMessage.getExceptionType());
    assertEquals(ProtobufMessages.INVALID_MESSAGE, returnMessage.getMessage());
  }

  @Test
  public void testStoreNoDocIdQueryString() {
    Cas cas = Cas.newBuilder().setDocumentId("docId").build();
    Message outMessage = Message.newBuilder().setCas(cas).build();
    Response response =
        target("/rest/store")
            .request()
            .post(Entity.entity(outMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, response.getStatus());

    Message returnMessage = response.readEntity(Message.class);
    assertEquals(MessageCodes.INVALID_MESSAGE, returnMessage.getStatusCode());
    assertEquals(MessageExceptions.ILLEGAL_ARGUMENT, returnMessage.getExceptionType());
    assertEquals(ProtobufMessages.INVALID_MESSAGE, returnMessage.getMessage());

    cas = Cas.newBuilder().build();
    outMessage = Message.newBuilder().setCas(cas).setMetadataQueryKey("query").build();
    response =
        target("/rest/store")
            .request()
            .post(Entity.entity(outMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, response.getStatus());

    returnMessage = response.readEntity(Message.class);
    assertEquals(MessageCodes.INVALID_MESSAGE, returnMessage.getStatusCode());
    assertEquals(MessageExceptions.ILLEGAL_ARGUMENT, returnMessage.getExceptionType());
    assertEquals(ProtobufMessages.INVALID_MESSAGE, returnMessage.getMessage());
  }

  @Test
  public void testStore() {
    MongoClient client = MongoClients.create(MONGODB_URL);
    MongoCollection<Document> repoCollection =
        client.getDatabase(REPO_DATABASE).getCollection(REPO_COLLECTION);
    MongoCollection<Document> metadataCollection =
        client.getDatabase(METADATA_DATABASE).getCollection(METADATA_COLLECTION_PREPROCESSOR);
    assertEquals(0L, repoCollection.countDocuments());
    assertEquals(0L, metadataCollection.countDocuments());

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
    Message outMessage =
        Message.newBuilder()
            .setCas(cas)
            .setMetadataQueryKey(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR)
            .build();
    Response response =
        target("/rest/store")
            .request()
            .post(Entity.entity(outMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, response.getStatus());

    Message returnMessage = response.readEntity(Message.class);
    assertEquals(MessageCodes.OK, returnMessage.getStatusCode());
    assertTrue(StringUtils.isEmpty(returnMessage.getExceptionType()));
    assertEquals(ProtobufMessages.CAS_STORE_SUCCESS, returnMessage.getMessage());

    Cas returnCas = returnMessage.getCas();

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, returnCas.getCasId());
    assertEquals(1L, repoCollection.countDocuments(filter));
    assertEquals(1L, metadataCollection.countDocuments(filter));

    repoCollection.deleteMany(filter);
    metadataCollection.deleteMany(filter);
  }

  @Test
  public void testDeleteNoCas() {
    Message outMessage = Message.newBuilder().setMessage("HI").build();
    Response response =
        target("/rest/delete")
            .request()
            .post(Entity.entity(outMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, response.getStatus());

    Message returnMessage = response.readEntity(Message.class);
    assertEquals(MessageCodes.INVALID_MESSAGE, returnMessage.getStatusCode());
    assertEquals(MessageExceptions.ILLEGAL_ARGUMENT, returnMessage.getExceptionType());
    assertEquals(ProtobufMessages.INVALID_MESSAGE, returnMessage.getMessage());
  }

  @Test
  public void testDelete() {
    MongoClient client = MongoClients.create(MONGODB_URL);
    MongoCollection<Document> repoCollection =
        client.getDatabase(REPO_DATABASE).getCollection(REPO_COLLECTION);
    MongoCollection<Document> metadataCollection =
        client.getDatabase(METADATA_DATABASE).getCollection(METADATA_COLLECTION_PREPROCESSOR);
    assertEquals(0L, repoCollection.countDocuments());
    assertEquals(0L, metadataCollection.countDocuments());

    ByteString data = ByteString.copyFrom("test", StandardCharsets.UTF_8);
    byte[] dataBytes = data.toByteArray();
    Checksum crc32 = new CRC32();
    crc32.update(dataBytes, 0, dataBytes.length);

    final Cas cas =
        Cas.newBuilder()
            .setDocumentId("docId")
            .setCrc32Checksum(crc32.getValue())
            .setCasData(data)
            .build();
    final Message outMessage =
        Message.newBuilder()
            .setCas(cas)
            .setMetadataQueryKey(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR)
            .build();
    Response response =
        target("/rest/store")
            .request()
            .post(Entity.entity(outMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, response.getStatus());

    final Message returnMessage = response.readEntity(Message.class);
    final Cas returnCas = returnMessage.getCas();

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, returnCas.getCasId());
    assertEquals(1L, repoCollection.countDocuments(filter));
    assertEquals(1L, metadataCollection.countDocuments(filter));

    final Cas deleteCas = Cas.newBuilder().setCasId(returnCas.getCasId()).build();
    final Message deleteMessage = Message.newBuilder().setCas(deleteCas).build();
    Response deleteResponse =
        target("/rest/delete")
            .request()
            .post(Entity.entity(deleteMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, deleteResponse.getStatus());

    final Message returnDeleteMessage = deleteResponse.readEntity(Message.class);
    assertEquals(MessageCodes.OK, returnDeleteMessage.getStatusCode());
    assertTrue(StringUtils.isEmpty(returnDeleteMessage.getExceptionType()));
    assertEquals(ProtobufMessages.CAS_DELETE_SUCCESS, returnDeleteMessage.getMessage());

    assertEquals(0L, repoCollection.countDocuments(filter));
    assertEquals(0L, metadataCollection.countDocuments(filter));
  }

  @Test
  public void testGetNoCas() {
    Message outMessage = Message.newBuilder().setMessage("HI").build();
    Response response =
        target("/rest/get")
            .request()
            .post(Entity.entity(outMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, response.getStatus());

    Message returnMessage = response.readEntity(Message.class);
    assertEquals(MessageCodes.INVALID_MESSAGE, returnMessage.getStatusCode());
    assertEquals(MessageExceptions.ILLEGAL_ARGUMENT, returnMessage.getExceptionType());
    assertEquals(ProtobufMessages.INVALID_MESSAGE, returnMessage.getMessage());
  }

  @Test
  public void testGet() {
    MongoClient client = MongoClients.create(MONGODB_URL);
    MongoCollection<Document> repoCollection =
        client.getDatabase(REPO_DATABASE).getCollection(REPO_COLLECTION);
    MongoCollection<Document> metadataCollection =
        client.getDatabase(METADATA_DATABASE).getCollection(METADATA_COLLECTION_PREPROCESSOR);
    assertEquals(0L, repoCollection.countDocuments());
    assertEquals(0L, metadataCollection.countDocuments());

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
    Message outMessage =
        Message.newBuilder()
            .setCas(cas)
            .setMetadataQueryKey(MetadataStoreQueries.QUERY_KEY_PREPROCESSOR)
            .build();
    Response response =
        target("/rest/store")
            .request()
            .post(Entity.entity(outMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, response.getStatus());

    Message returnMessage = response.readEntity(Message.class);
    assertEquals(MessageCodes.OK, returnMessage.getStatusCode());
    assertTrue(StringUtils.isEmpty(returnMessage.getExceptionType()));
    assertEquals(ProtobufMessages.CAS_STORE_SUCCESS, returnMessage.getMessage());

    Cas returnCas = returnMessage.getCas();

    Bson filter = Filters.eq(MongoDbDocument.CASID_FIELD_KEY, returnCas.getCasId());
    assertEquals(1L, repoCollection.countDocuments(filter));
    assertEquals(1L, metadataCollection.countDocuments(filter));

    final Cas getCas = Cas.newBuilder().setCasId(returnCas.getCasId()).build();
    final Message getMessage = Message.newBuilder().setCas(getCas).build();
    Response getResponse =
        target("/rest/get")
            .request()
            .post(Entity.entity(getMessage, MediaTypes.APPLICATION_XPROTOBUF));
    assertEquals(StatusCodes.OK, getResponse.getStatus());

    final Message returnGetMessage = getResponse.readEntity(Message.class);
    assertEquals(MessageCodes.OK, returnGetMessage.getStatusCode());
    assertTrue(StringUtils.isEmpty(returnGetMessage.getExceptionType()));
    assertEquals(ProtobufMessages.CAS_GET_SUCCESS, returnGetMessage.getMessage());
    String retrievedCasString =
        returnGetMessage.getCas().getCasData().toString(StandardCharsets.UTF_8);
    assertEquals("test", retrievedCasString);

    repoCollection.deleteMany(filter);
    metadataCollection.deleteMany(filter);
  }
}
