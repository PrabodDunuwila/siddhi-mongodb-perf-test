import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceTest {

    private static final Logger log = Logger.getLogger(JoinMongoTableTest.class);
    private AtomicInteger inEventCount;
    private boolean eventArrived;
    private List<Object[]> inEventsList;
    private static String uri = MongoTableTestUtils.resolveBaseUri();
    private int waitTime = 2000;
    private int timeout = 30000;

    @BeforeClass
    public void init() {
        log.info("== Mongo Table query tests started ==");
        inEventCount = new AtomicInteger();
        eventArrived = false;
        inEventsList = new ArrayList<>();
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table query tests completed ==");
    }

    @BeforeMethod
    public void testInit() {
        inEventCount.set(0);
    }

    @AfterMethod
    public void testEnd() {
        inEventsList.clear();
    }

    @Test
    public void testMongoTableQuery1() throws InterruptedException {
        log.info("testMongoTableQuery1 : PerformanceTest");

        MongoTableTestUtils.dropCollection(uri, "Purchase");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream PurchaseStream (purchaseId string, dateOfPurchase long, customerId string, " +
                "country string, totalAmount int); " +
                "define stream TriggerStream (startTimestamp long, country string ); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"purchaseId\") " +
                "define table Purchase (purchaseId string, dateOfPurchase long, customerId string, country string, " +
                "totalAmount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from PurchaseStream " +
                "insert into Purchase ;" +
                "" +
                "@info(name = 'query2') " +
                "from TriggerStream as T join Purchase as P " +
                "on P.country == T.country " +
                "select T.startTimestamp, P.customerId as customerID, sum(P.totalAmount) as totalAmount " +
                "group by P.customerId " +
                "order by totalAmount desc " +
                "limit 5 " +
                "insert into OutputStream ; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    log.info(System.currentTimeMillis() - Long.parseLong(String.valueOf(inEvents[inEvents.length - 1]
                            .getData()[0])));
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler purchaseStream = siddhiAppRuntime.getInputHandler("PurchaseStream");
        InputHandler triggerStream = siddhiAppRuntime.getInputHandler("TriggerStream");
        siddhiAppRuntime.start();

        int customerIdNumber = 0;
        for (int i = 0; i < 100000; i++) {
            if (customerIdNumber < 5) {
                purchaseStream.send(new Object[]{"purchaseId" + i, i, "customerId" + customerIdNumber, "country_x", i});
                customerIdNumber++;
            } else {
                customerIdNumber = 0;
            }
        }

        triggerStream.send(new Object[]{System.currentTimeMillis(), "country_x"});

        SiddhiTestHelper.waitForEvents(waitTime, 5, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        //Add expected results to this list
        List<Object[]> expected = new ArrayList<>();

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 5, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }
}
