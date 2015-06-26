package org.opennms.correlator.newts;

import org.junit.runner.RunWith;
import org.opennms.correlator.api.MetricCorrelator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
	"classpath:/applicationContext-newts.xml"
})
public class NewtsMetricCorrelatorTest extends AbstractCorrelatorTest {

	@Autowired
	private NewtsMetricCorrelator m_correlator;

    @Override
    public MetricCorrelator getCorrelator() {
        return m_correlator;
    }
}
