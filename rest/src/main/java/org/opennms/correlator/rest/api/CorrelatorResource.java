package org.opennms.correlator.rest.api;

import java.util.Collection;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("/correlator")
@Produces(MediaType.APPLICATION_JSON)
public interface CorrelatorResource {

    @GET
    @Path("correlate")
    public Collection<MetricAndCoefficientDTO> correlate(@QueryParam("resource") String resource, @QueryParam("metric") String metric, @QueryParam("from") Long from, @QueryParam("to") Long to, @QueryParam("resolution") Long resolution, @DefaultValue("10") @QueryParam("top") Integer topN);

    @POST
    @Path("correlate")
    @Consumes(MediaType.APPLICATION_JSON)
    public Collection<MetricAndCoefficientDTO> correlate(@QueryParam("resource") String resource, @QueryParam("metric") String metric, @QueryParam("from") Long from, @QueryParam("to") Long to, @QueryParam("resolution") Long resolution, @DefaultValue("10") @QueryParam("top") Integer topN, List<MetricDTO> candidateMetrics);

}
