package uk.co.flax.luwak.server.resources;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.QueryError;
import uk.co.flax.luwak.UpdateException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tom.Ridd on 20/01/2017.
 */
@Path("/update")
public class UpdateResource {
    private final Monitor monitor;
    public static final Logger logger = LoggerFactory.getLogger(UpdateResource.class);

    public UpdateResource(Monitor monitor) {
        this.monitor = monitor;
    }

    @POST
    @Path("/single")
    public Response postLuwakUpdate(MonitorQuery monitorQuery) throws IOException, UpdateException {
        logger.info("q1 {}", monitorQuery);
        monitor.update(monitorQuery);
        return Response.ok("{\"result\":\"created\"}", MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Path("/multi")
    public Response addMultiple(List<MonitorQuery> queries) throws IOException {
        try {
            logger.info("q {}", queries);
            monitor.update(queries);
        } catch (UpdateException e) {
            logger.info("err ", e);
            StringBuilder sb = new StringBuilder();
            for (QueryError error : e.errors) {
                sb.append(error.toString()).append("\n");
            }
            throw new WebApplicationException(sb.toString());
        }
        return Response.ok("{\"result\":\"created\"}", MediaType.APPLICATION_JSON).build();
    }
}
