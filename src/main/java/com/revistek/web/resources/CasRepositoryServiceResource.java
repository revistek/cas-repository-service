package com.revistek.web.resources;

import com.revistek.crs.protos.Cas;
import com.revistek.crs.protos.Message;
import com.revistek.crs.protos.constants.MessageCodes;
import com.revistek.crs.protos.constants.MessageExceptions;
import com.revistek.crs.protos.constants.ProtobufMessages;
import com.revistek.net.constants.MediaTypes;
import com.revistek.net.constants.StatusCodes;
import com.revistek.util.MetadataStoreDao;
import com.revistek.util.RepositoryDao;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The REST endpoints for the Cas Repository Service.
 * 
 * @author Chuong Ngo
 */
@Path("/rest")
public class CasRepositoryServiceResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(CasRepositoryServiceResource.class);

  /**
   * Endpoint to store a UIMA Cas/JCas to the repository and add an entry for it to the metadata
   * store.
   *
   * @param cxt - The {@link jakarta.servlet.ServletContext ServletContext} that holds the singleton
   *     objects need to interact with the repository and metadata store.
   * @param message - The {@link com.revistek.crs.protos.Message Message} object that holds the Cas
   *     and other needed information.
   * @return A {@link com.revistek.crs.protos.Message Message} object with information about the
   *     transaction.
   */
  @POST
  @Path("/store")
  @Produces(MediaTypes.APPLICATION_XPROTOBUF)
  public Response store(@Context ServletContext cxt, Message message) {
    if (message == null) {
      LOGGER.error("The protobuf message failed to properly parse.");

      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INVALID_MESSAGE)
              .setExceptionType(MessageExceptions.ILLEGAL_ARGUMENT)
              .setMessage(ProtobufMessages.INVALID_MESSAGE)
              .build();
      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    MetadataStoreDao metaDao =
        (MetadataStoreDao) cxt.getAttribute(MetadataStoreDao.class.getName());
    RepositoryDao repoDao = (RepositoryDao) cxt.getAttribute(RepositoryDao.class.getName());

    if (!message.hasCas()) {
      LOGGER.error(
          "A Cas protobuf object with a valid Cas ID is needed to store a Cas to the repository.");

      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INVALID_MESSAGE)
              .setExceptionType(MessageExceptions.ILLEGAL_ARGUMENT)
              .setMessage(ProtobufMessages.INVALID_MESSAGE)
              .build();
      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    final Cas cas = message.getCas();
    String docId = cas.getDocumentId();
    String queryId = message.getMetadataQueryKey();

    if (StringUtils.isAnyEmpty(docId, queryId)) {
      LOGGER.error("Valid document and query IDs are needed to store a Cas to the repository.");

      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INVALID_MESSAGE)
              .setExceptionType(MessageExceptions.ILLEGAL_ARGUMENT)
              .setMessage(ProtobufMessages.INVALID_MESSAGE)
              .build();
      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    String casId = "";

    try {
      casId = repoDao.store(cas);
    } catch (Exception e) {
      LOGGER.error("There was an error writing the Cas " + casId + " to the repository.");
      e.printStackTrace();

      final Cas returnCas = Cas.newBuilder().setCasId(casId).build();
      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INTERNAL_SERVER_ERROR)
              .setExceptionType(MessageExceptions.GENERAL_EXCEPTION)
              .setMessage(ProtobufMessages.CAS_STORE_FAILED)
              .setCas(returnCas)
              .build();

      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    try {
      metaDao.addCasId(queryId, casId);
    } catch (Exception e) {
      LOGGER.error(
          "There was an error writing the Cas ID "
              + casId
              + " to the metadata store: "
              + queryId
              + ".");
      e.printStackTrace();

      final Cas returnCas = Cas.newBuilder().setCasId(casId).build();
      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INTERNAL_SERVER_ERROR)
              .setExceptionType(MessageExceptions.GENERAL_EXCEPTION)
              .setMessage(ProtobufMessages.CAS_STORE_FAILED)
              .setCas(returnCas)
              .build();

      try {
        repoDao.deleteCasId(casId);
      } catch (Exception e1) {
        // Ignore this exception and move on.
      }

      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    LOGGER.trace(
        "Successfully wrote the Cas "
            + casId
            + " to the repository and metadata store: "
            + queryId
            + ".");
    final Cas returnCas = Cas.newBuilder().setCasId(casId).setDocumentId(docId).build();
    final Message responseMessage =
        Message.newBuilder()
            .setStatusCode(MessageCodes.OK)
            .setMessage(ProtobufMessages.CAS_STORE_SUCCESS)
            .setCas(returnCas)
            .build();
    return Response.status(StatusCodes.OK).entity(responseMessage).build();
  }

  /**
   * Endpoint to delete a UIMA Cas/JCas to the repository and it's entries in the metadata store.
   *
   * @param cxt - The {@link jakarta.servlet.ServletContext ServletContext} that holds the singleton
   *     objects need to interact with the repository and metadata store.
   * @param message - The {@link com.revistek.crs.protos.Message Message} object that holds the
   *     needed information.
   * @return A {@link com.revistek.crs.protos.Message Message} object with information about the
   *     transaction.
   */
  @POST
  @Path("/delete")
  @Produces()
  public Response deleteCasId(@Context ServletContext cxt, Message message) {
    if (message == null) {
      LOGGER.error("The protobuf message failed to properly parse.");

      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INVALID_MESSAGE)
              .setExceptionType(MessageExceptions.ILLEGAL_ARGUMENT)
              .setMessage(ProtobufMessages.INVALID_MESSAGE)
              .build();
      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    RepositoryDao repoDao = (RepositoryDao) cxt.getAttribute(RepositoryDao.class.getName());
    MetadataStoreDao metaDao =
        (MetadataStoreDao) cxt.getAttribute(MetadataStoreDao.class.getName());

    if (!message.hasCas()) {
      LOGGER.error(
          "A Cas protobuf object with a valid Cas ID is needed to delete a Cas from the repository.");

      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INVALID_MESSAGE)
              .setExceptionType(MessageExceptions.ILLEGAL_ARGUMENT)
              .setMessage(ProtobufMessages.INVALID_MESSAGE)
              .build();
      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    String casId = message.getCas().getCasId();

    try {
      repoDao.deleteCasId(casId);
      metaDao.deleteAllCasId(casId);
    } catch (Exception e) {
      LOGGER.error("There was an error deleting the Cas " + casId + " from the repository.");
      e.printStackTrace();

      final Cas returnCas = Cas.newBuilder().setCasId(casId).build();
      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INTERNAL_SERVER_ERROR)
              .setExceptionType(MessageExceptions.GENERAL_EXCEPTION)
              .setMessage(ProtobufMessages.CAS_DELETE_FAILED)
              .setCas(returnCas)
              .build();
      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    LOGGER.trace("Successfully deleted the Cas " + casId + " from the repository.");
    final Cas returnCas = Cas.newBuilder().setCasId(casId).build();
    final Message responseMessage =
        Message.newBuilder()
            .setStatusCode(MessageCodes.OK)
            .setMessage(ProtobufMessages.CAS_DELETE_SUCCESS)
            .setCas(returnCas)
            .build();
    return Response.status(StatusCodes.OK).entity(responseMessage).build();
  }

  /**
   * Endpoint to retrieve a UIMA Cas/JCas from the repository.
   *
   * @param cxt - The {@link jakarta.servlet.ServletContext ServletContext} that holds the singleton
   *     objects need to interact with the repository and metadata store.
   * @param message - The {@link com.revistek.crs.protos.Message Message} object that holds the
   *     needed information.
   * @return A {@link com.revistek.crs.protos.Message Message} object with information about the
   *     transaction.
   */
  @POST
  @Path("/get")
  @Produces(MediaTypes.APPLICATION_XPROTOBUF)
  public Response getCasId(@Context ServletContext cxt, Message message) {
    if (message == null) {
      LOGGER.error("The protobuf message failed to properly parse.");

      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INVALID_MESSAGE)
              .setExceptionType(MessageExceptions.ILLEGAL_ARGUMENT)
              .setMessage(ProtobufMessages.INVALID_MESSAGE)
              .build();
      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    RepositoryDao dao = (RepositoryDao) cxt.getAttribute(RepositoryDao.class.getName());

    if (!message.hasCas()) {
      LOGGER.error(
          "A Cas protobuf object with a valid Cas ID is needed to retrieve a Cas from the repository.");
      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INVALID_MESSAGE)
              .setExceptionType(MessageExceptions.ILLEGAL_ARGUMENT)
              .setMessage(ProtobufMessages.INVALID_MESSAGE)
              .build();
      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    String casId = message.getCas().getCasId();
    Cas retrievedCas;

    try {
      retrievedCas = dao.getCasId(casId);
    } catch (Exception e) {
      LOGGER.error("There was an error retrieving the Cas " + casId + " from the repository.");
      e.printStackTrace();

      final Message responseMessage =
          Message.newBuilder()
              .setStatusCode(MessageCodes.INTERNAL_SERVER_ERROR)
              .setExceptionType(MessageExceptions.GENERAL_EXCEPTION)
              .setMessage(ProtobufMessages.CAS_GET_FAILED)
              .build();
      return Response.status(StatusCodes.OK).entity(responseMessage).build();
    }

    LOGGER.trace("Successfully retrieved the Cas " + casId + " from the repository.");
    final Message responseMessage =
        Message.newBuilder()
            .setStatusCode(MessageCodes.OK)
            .setMessage(ProtobufMessages.CAS_GET_SUCCESS)
            .setCas(retrievedCas)
            .build();
    return Response.status(StatusCodes.OK).entity(responseMessage).build();
  }
}
