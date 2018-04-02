package uk.co.flax.luwak.server.resources;

import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.DocumentBatch;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.QueryMatch;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.MatcherFactory;
import uk.co.flax.luwak.matchers.ParallelMatcher;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.QueryParam;
import java.io.IOException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Tom.Ridd on 20/01/2017.
 */
@Path("/match")
public class MatchResource {
  private final Monitor monitor;
  
  public static final Logger logger = LoggerFactory.getLogger(MatchResource.class);

  public MatchResource(Monitor monitor) {
    this.monitor = monitor;
  }

  @POST
  @Path("/doc/")
  @Produces(MediaType.APPLICATION_JSON)
  public Matches<QueryMatch> getLuwakMatches(@QueryParam("thd") int thd, InputDocument inputDocument) throws IOException {
    MatcherFactory matcher = SimpleMatcher.FACTORY;
    if (thd>1) {
      ExecutorService executor = Executors.newFixedThreadPool(thd);
      matcher = ParallelMatcher.factory(executor, SimpleMatcher.FACTORY, thd);
    }

    logger.info("doc thd={} {}", thd, matcher);

    return monitor.match(inputDocument, matcher);
  }

  @POST
  @Path("/docs/")
  @Produces(MediaType.APPLICATION_JSON)
  public Matches<QueryMatch> getLuwakMatchesDocs(@QueryParam("thd") int thd, DocumentBatch docs) throws IOException {
    MatcherFactory matcher = SimpleMatcher.FACTORY;
    if (thd>1) {
      ExecutorService executor = Executors.newFixedThreadPool(thd);
      matcher = ParallelMatcher.factory(executor, SimpleMatcher.FACTORY, thd);
    }

    logger.info("doc thd={} {}", thd, matcher);
    
    return monitor.match(docs, matcher);
  }
}
