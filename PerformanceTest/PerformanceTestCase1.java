//Case 1 : Run another query before getting the latency for the desired query.

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.log4j.Logger;

public class PerfTestCustom {

    public static void main(String[] args) throws InterruptedException {

        String uri = MongoTableTestUtils.resolveBaseUri();
        final Logger log = Logger.getLogger(JoinMongoTableTest.class);

        MongoTableTestUtils.dropCollection(uri, "Purchase");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream PurchaseStream (purchaseId string, dateOfPurchase long, customerId string, " +
                "country string, totalAmount int); " +
                "define stream TriggerStream (startTimestamp long, country string ); " +
                "define stream TriggerStream2 (startTimestamp long, country string ); " +
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
                "from TriggerStream2 as T join Purchase as P " +
                "on P.country == T.country " +
                "select P.customerId as customerID " +
                "limit 5 " +
                "insert into OutputStream1 ; "+
                ""+
                "@info(name = 'query3') " +
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
                    log.info("other query");
                }
            }
        });
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    log.info((System.nanoTime() - Long.parseLong(String.valueOf(inEvents[inEvents.length - 1]
                            .getData()[0])))/1000000);
                }
            }
        });

        InputHandler purchaseStream = siddhiAppRuntime.getInputHandler("PurchaseStream");
        InputHandler triggerStream = siddhiAppRuntime.getInputHandler("TriggerStream");
        InputHandler triggerStream2 = siddhiAppRuntime.getInputHandler("TriggerStream2");
        siddhiAppRuntime.start();

        //Insert documents to Purchase collection in MongoDB
        int customerIdNumber = 0;       //Used for groupBy
        for (int i = 0; i < 100000; i++) {
            if (customerIdNumber < 100) {
                purchaseStream.send(new Object[]{"purchaseId" + i, i, "customerId" + customerIdNumber, "country_x", i});
                customerIdNumber++;
            } else {
                customerIdNumber = 0;
            }
        }

        //Send events to run another query
        for(int i=0;i<10;i++){
            triggerStream2.send(new Object[]{System.nanoTime(), "country_x"});
        }

        //Send event to TriggerStream to get the latency
        triggerStream.send(new Object[]{System.nanoTime(), "country_x"});
        siddhiAppRuntime.shutdown();
    }
}
