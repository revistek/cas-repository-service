package com.revistek.web;

import com.google.common.io.CharSource;
import com.google.common.io.Resources;
import com.revistek.crs.constants.Cache;
import com.revistek.crs.constants.ConfigurationValues;
import com.revistek.util.CacheDao;
import com.revistek.util.IdGenerator;
import com.revistek.util.MetadataStoreDao;
import com.revistek.util.MongoDbMetadataStoreDao;
import com.revistek.util.MongoDbRepositoryDao;
import com.revistek.util.RedisCacheDao;
import com.revistek.util.RepositoryDao;
import com.revistek.util.constants.ErrorMessages;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and registers the singletons needed for the operation of the CRS endpoints.
 *
 * @author Chuong Ngo
 */
@WebListener
public class CasRepositoryServiceServletContextListener implements ServletContextListener {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(CasRepositoryServiceServletContextListener.class);

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    LOGGER.info("The ServletContextListener is starting.");

    // Load properties.
    Properties prop = new Properties();
    try {
      URL propUrl = Resources.getResource(ConfigurationValues.FILENAME);
      LOGGER.trace("Using properties file: " + propUrl);

      CharSource source = Resources.asCharSource(propUrl, ConfigurationValues.FILE_ENCODING);
      BufferedReader reader = source.openBufferedStream();
      prop.load(reader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String cacheUrl = prop.getProperty(ConfigurationValues.CACHE_URL);
    if (StringUtils.isEmpty(cacheUrl)) {
      throw new RuntimeException(ErrorMessages.INVALID_CONFIGURATION_FILE_CACHEURL);
    }

    CacheDao cacheDao = new RedisCacheDao(cacheUrl);
    cacheDao.initialize();

    IdGenerator idGenerator = IdGenerator.newBuilder().cacheDao(cacheDao).build();
    RepositoryDao repositoryDao;

    MongoDbMetadataStoreDao metadataDao =
        new MongoDbMetadataStoreDao(cacheDao.get(Cache.KEY_METDATA_STORE_URL));
    Map<String, String> queries = cacheDao.getMap(Cache.KEY_METDATA_STORE_QUERIES);
    LOGGER.trace("Loading the metadata store queries.");

    for (Map.Entry<String, String> query : queries.entrySet()) {
      String values = query.getValue();
      LOGGER.trace("Loading query, key: " + query.getKey() + " value: " + values);

      // For a MongoDB Metadata Store, the values should be a comma-delimited string of database,
      // collection.
      String[] splitValues = values.split(",", 2);

      try {
        metadataDao.registerQuery(query.getKey(), splitValues[0], splitValues[1]);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      repositoryDao =
          MongoDbRepositoryDao.newBuilder()
              .url(cacheDao.get(Cache.KEY_REPOSITORY_URL))
              .idGenerator(idGenerator)
              .database(cacheDao.get(Cache.KEY_MONGODB_REPOSITORY_DATABASE))
              .collection(cacheDao.get(Cache.KEY_MONGODB_REPOSITORY_COLLECTION))
              .build();
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    }

    ServletContext context = sce.getServletContext();

    context.setAttribute(MetadataStoreDao.class.getName(), (MetadataStoreDao) metadataDao);
    context.setAttribute(RepositoryDao.class.getName(), repositoryDao);
    context.setAttribute(CacheDao.class.getName(), cacheDao);
    LOGGER.info("The ServletContextListener has finished loading.");
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    ServletContext context = sce.getServletContext();

    MetadataStoreDao metaDao =
        (MetadataStoreDao) context.getAttribute(MetadataStoreDao.class.getName());
    RepositoryDao repoDao = (RepositoryDao) context.getAttribute(RepositoryDao.class.getName());
    CacheDao cacheDao = (CacheDao) context.getAttribute(CacheDao.class.getName());

    metaDao.cleanup();
    repoDao.cleanup();
    cacheDao.cleanup();

    LOGGER.trace("The ServletContextListener has finished cleaning up.");
  }
}
