//Case 2 : Sending stream events and get a stable value after running the query for 10000 events.

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.log4j.Logger;

public class PerformanceTestCase2 {

    static int counter = 0;
    static long timeDifference = 0;

    public static void main(String[] args) throws InterruptedException {

        String uri = MongoTableTestUtils.resolveBaseUri();
        final Logger log = Logger.getLogger(JoinMongoTableTest.class);

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
                    counter++;
                    if (counter == 1000) {
                        log.info(timeDifference / 1000);
                        counter = 0;
                        timeDifference = 0;
                    }
                    timeDifference += System.nanoTime() - Long.parseLong(String.valueOf(inEvents[inEvents.length - 1]
                            .getData()[0]));
                }
            }
        });

        InputHandler purchaseStream = siddhiAppRuntime.getInputHandler("PurchaseStream");
        InputHandler triggerStream = siddhiAppRuntime.getInputHandler("TriggerStream");
        siddhiAppRuntime.start();

        //Insert documents to Purchase collection in MongoDB
        int customerIdNumber = 0;           //Used for groupBy
        for (int i = 0; i < 100000; i++) {
            if (customerIdNumber < 100) {
                purchaseStream.send(new Object[]{"purchaseId" + i, i, "customerId" + customerIdNumber, "country_x", i});
                customerIdNumber++;
            } else {
                customerIdNumber = 0;
            }
        }

        //Send TriggerStream events with timestamp value.
        for (int i = 0; i < 10000; i++) {
            triggerStream.send(new Object[]{System.nanoTime(), "country_x"});
        }

        siddhiAppRuntime.shutdown();
    }
}
