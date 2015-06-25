package org.opennms.correlator.rest.api;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class MetricAndCoefficientDTOTest {

    @Test
    public void marshallAndUnmarshall() throws JsonGenerationException, JsonMappingException, IOException {
        MetricAndCoefficientDTO m = new MetricAndCoefficientDTO("a", "b", 0.15);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(m);

        JsonParser parser = new JsonParser();
        JsonElement expected = parser.parse("{\"metric\":\"b\",\"coefficient\":0.15,\"resource\":\"a\"}");
        JsonElement actual = parser.parse(json);
        assertEquals(expected, actual);

        MetricAndCoefficientDTO mm = mapper.readValue(json, MetricAndCoefficientDTO.class);
        assertEquals(m, mm);
    }
}
