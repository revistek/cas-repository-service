package com.revistek.web;

import jakarta.ws.rs.ApplicationPath;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Configures the Jersey web app.
 *
 * @author Chuong Ngo
 */
@ApplicationPath("/")
public class CasRepositoryServiceResourceConfig extends ResourceConfig {
  public CasRepositoryServiceResourceConfig() {
    packages("com.revistek.web");
  }
}
