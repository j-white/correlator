package org.opennms.correlator.rest;

import org.opennms.correlator.rest.service.HelloWorldResource;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class CorrelatorApplication extends Application<CorrelatorConfiguration> {
    public static void main(String[] args) throws Exception {
        new CorrelatorApplication().run(args);
    }

    @Override
    public String getName() {
        return "hello-world";
    }

    @Override
    public void initialize(Bootstrap<CorrelatorConfiguration> bootstrap) {
        // nothing to do yet
    }

    @Override
    public void run(CorrelatorConfiguration configuration,
                    Environment environment) {
        final HelloWorldResource resource = new HelloWorldResource(
            configuration.getTemplate(),
            configuration.getDefaultName()
        );
        environment.jersey().register(resource);
    }

}