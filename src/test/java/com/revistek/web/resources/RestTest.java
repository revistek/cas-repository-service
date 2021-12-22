package com.revistek.web.resources;

import com.revistek.web.CasRepositoryServiceServletContextListener;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;

public abstract class RestTest extends JerseyTest {
  @Override
  protected abstract ResourceConfig configure();

  @Override
  protected abstract void configureClient(ClientConfig config);

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return new GrizzlyWebTestContainerFactory();
  }

  @Override
  protected DeploymentContext configureDeployment() {
    ResourceConfig app = (ResourceConfig) configure();
    //    app.register(
    //        new Feature() {
    //          @Context ServletContext servletContext;
    //
    //          @Override
    //          public boolean configure(FeatureContext context) {
    //            AppName appName = new AppName();
    //
    //            MetadataStoreDao mockMetedataStoreDao =
    // Mockito.mock(MongoDbMetadataStoreDao.class);
    //            RepositoryDao mockRepositoryDao = Mockito.mock(MongoDbRepositoryDao.class);
    //            servletContext.setAttribute(AppName.class.getName(), appName);
    //            servletContext.setAttribute(MetadataStoreDao.class.getName(),
    // mockMetedataStoreDao);
    //            servletContext.setAttribute(RepositoryDao.class.getName(), mockRepositoryDao);
    //            return true;
    //          }
    //        });

    return ServletDeploymentContext.forServlet(new ServletContainer(app))
        .addListener(CasRepositoryServiceServletContextListener.class)
        .build();
  }
}
