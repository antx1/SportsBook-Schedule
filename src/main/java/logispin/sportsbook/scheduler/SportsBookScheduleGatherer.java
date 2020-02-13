package logispin.sportsbook.scheduler;

import java.io.*;

import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import java.util.Map.Entry;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@RestController
public class SportsBookScheduleGatherer {
    final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    final TableId tableId = TableId.of("vision_nigeria_production", "sportsbook_events_schedule");


    public static void main(String[] args) throws ParserConfigurationException, SAXException {
        //System.out.println(new SOAPConnector().getDate());

        try {
            new SportsBookScheduleGatherer().SOAPClient();
            //System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void writeToFile(String fn, String s)
            throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fn));
        writer.write(s);

        writer.close();
    }

    public String getDate() {
        LocalDateTime myDateObj = LocalDateTime.now();
        System.out.println("Before formatting: " + myDateObj.atZone(ZoneId.of("Europe/Rome")));
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
        String formattedDate = myDateObj.format(myFormatObj);
        //System.out.println("After formatting: " + formattedDate);


        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

//Here you say to java the initial timezone. This is the secret
        sdf.setTimeZone(TimeZone.getTimeZone("CET"));
//Will print on your default Timezone
        return sdf.format(calendar.getTime()).replace(" ", "T");
    }

    @RequestMapping(value = "/getFullList", method = {RequestMethod.GET, RequestMethod.POST}, headers = "Accept=application/json")
    public void SOAPClient() throws IOException, ParserConfigurationException, SAXException {
        String lastUpdate = getDate();
        RequestConfig requestConfig = RequestConfig.custom()
                // Determines the timeout in milliseconds until a connection is established.
                .setConnectTimeout(1000)
                // Defines the socket timeout in milliseconds,
                // which is the timeout for waiting for data or, put differently,
                // a maximum period inactivity between two consecutive data packets).
                .setSocketTimeout(6000 * 20)
                // Returns the timeout in milliseconds used when requesting a connection
                // from the connection manager.
                .setConnectionRequestTimeout(6000 * 20)
                .build();

        HttpPost post = new HttpPost("http://integrationapi.bet9ja.com/Latest/EventsProgram.svc");
        post.addHeader("content-type", "text/xml");
        post.addHeader("SOAPAction", "http://wcf.isolutions.it/ISBets.API.EventProgram/1.0/IEventProgram/GetEventsProgramV6");
        post.addHeader("Keep-Alive", "timeout=1");

        post.setConfig(requestConfig);

        StringBuilder entity = new StringBuilder();
        entity.append("<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ns=\"http://wcf.isolutions.it/ISBets.API.EventProgram/1.0/\" xmlns:isb=\"http://schemas.datacontract.org/2004/07/ISBets.API.Messages.Requests\" xmlns:isb1=\"http://schemas.datacontract.org/2004/07/ISBets.API.EventProgram.Messages.Requests\">\n" +
                "   <soapenv:Header/>\n" +
                "   <soapenv:Body>\n" +
                "      <ns:GetEventsProgramV6>\n" +
                "         <!--Optional:-->\n" +
                "         <ns:objRequest>\n" +
                "            <isb:_APIAccount>NovaFutur</isb:_APIAccount>\n" +
                "            <isb:_APIPassword>kL92:lp0_GGhm!</isb:_APIPassword>\n" +
                "            <isb:_IDBookmaker>75</isb:_IDBookmaker>\n" +
                "            <isb1:_DateLastModify>2020-02-13T14:09:53</isb1:_DateLastModify>\n" +
                "            <isb1:_EventsProgramCode>Pal01B9ja</isb1:_EventsProgramCode>\n" +
                "            <isb1:_LanguageID>2</isb1:_LanguageID>\n" +
                "         </ns:objRequest>\n" +
                "      </ns:GetEventsProgramV6>\n" +
                "   </soapenv:Body>\n" +
                "</soapenv:Envelope>");

        post.setEntity(new StringEntity(entity.toString()));
        StringBuilder sb1 = new StringBuilder();

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(post)) {


            InputStream inputStream = response.getEntity().getContent();
            //creating an InputStreamReader object
            InputStreamReader isReader = new InputStreamReader(inputStream);

            Scanner sc = new Scanner(inputStream);

            while (sc.hasNext()) {
                sb1.append(sc.nextLine());
            }
        }
        writeToFile("out.xml", sb1.toString());

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse("out.xml");
        doc.getDocumentElement().normalize();
        //System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
        NodeList nList = doc.getElementsByTagName("b:Sport");
        String lastServerDate = doc.getElementsByTagName("a:_LastServerDate").item(0).getTextContent();
        System.out.println(lastServerDate);
        //System.out.println("Sports Number " + nList.getLength());
//        StringBuffer sb = new StringBuffer();
//        sb.append("[")
        JSONArray jsonArray = new JSONArray();
        ;
        for (int i = 0; i < nList.getLength(); i++) {

            //System.out.println("Sport: " + i);

            Node nNodeSport = nList.item(i);
            Element eElementSport = (Element) nNodeSport;
//            String lastServerDate = eElementSport.
            if (eElementSport.getElementsByTagName("b:Sport").item(0) != null) {
                String sportName = (eElementSport.getElementsByTagName("b:Sport").item(0).getTextContent());
                int sportId = Integer.parseInt(eElementSport.getElementsByTagName("b:SportID").item(0).getTextContent());
                int sportStatus = Integer.parseInt(eElementSport.getElementsByTagName("b:Status").item(0).getTextContent());

                Node nNodeGroups = eElementSport.getElementsByTagName("b:Groups").item(0);

                for (int j = 0; j < nNodeGroups.getChildNodes().getLength(); j++) {
                    Element eElementGroup = (Element) nNodeGroups.getChildNodes().item(j);
                    String groupName = (eElementGroup.getElementsByTagName("b:Group").item(0).getTextContent());
                    int groupId = Integer.parseInt(eElementGroup.getElementsByTagName("b:GroupID").item(0).getTextContent());
                    int groupStatus = Integer.parseInt(eElementGroup.getElementsByTagName("b:Status").item(0).getTextContent());
                    Node nNodeEvents = eElementGroup.getElementsByTagName("b:Events").item(0);

                    for (int x = 0; x < nNodeEvents.getChildNodes().getLength(); x++) {
                        Element eElementEvent = (Element) nNodeEvents.getChildNodes().item(x);
                        String eventName = (eElementEvent.getElementsByTagName("b:Event").item(0).getTextContent());
                        int eventId = Integer.parseInt(eElementEvent.getElementsByTagName("b:EventID").item(0).getTextContent());
                        int eventTypeId = Integer.parseInt(eElementEvent.getElementsByTagName("b:EventTypeID").item(0).getTextContent());
                        int eventStatus = Integer.parseInt(eElementEvent.getElementsByTagName("b:Status").item(0).getTextContent());

                        Node nNodeSubEvents = eElementGroup.getElementsByTagName("b:SubEvents").item(0);

                        for (int y = 0; y < nNodeSubEvents.getChildNodes().getLength(); y++) {
                            Element eElementSubEvent = (Element) nNodeSubEvents.getChildNodes().item(y);
                            String subEventStartDate = (eElementSubEvent.getElementsByTagName("b:StartDate").item(0).getTextContent());
                            String subEventName = (eElementSubEvent.getElementsByTagName("b:SubEvent").item(0).getTextContent());
                            int subEventStatus = Integer.parseInt(eElementSubEvent.getElementsByTagName("b:Status").item(0).getTextContent());
                            long subEventId = Long.parseLong(eElementSubEvent.getElementsByTagName("b:SubEventID").item(0).getTextContent());
                            int SubEventPubblicationCode = Integer.parseInt(eElementSubEvent.getElementsByTagName("b:SubEventPubblicationCode").item(0).getTextContent());

                            Node nNodeOdds = eElementGroup.getElementsByTagName("b:Odds").item(0);

                            for (int z = 0; z < nNodeOdds.getChildNodes().getLength(); z++) {
                                Element eElementOdds = (Element) nNodeOdds.getChildNodes().item(z);
                                int oddCombinability = Integer.parseInt(eElementOdds.getElementsByTagName("b:Combinability").item(0).getTextContent().strip());
                                String oddEndData = (eElementOdds.getElementsByTagName("b:EndDate").item(0).getTextContent());
                                double HND = Double.parseDouble(eElementOdds.getElementsByTagName("b:HND").item(0).getTextContent());
                                double odd = Double.parseDouble(eElementOdds.getElementsByTagName("b:Odd").item(0).getTextContent());
                                String oddClass = (eElementOdds.getElementsByTagName("b:OddClass").item(0).getTextContent());
                                String oddClassCode = (eElementOdds.getElementsByTagName("b:OddClassCode").item(0).getTextContent());
                                Long oddID = Long.parseLong(eElementOdds.getElementsByTagName("b:OddID").item(0).getTextContent());
                                String oddType = (eElementOdds.getElementsByTagName("b:OddType").item(0).getTextContent());
                                String oddTypeCode = (eElementOdds.getElementsByTagName("b:OddTypeCode").item(0).getTextContent());
                                int oddTypeID = Integer.parseInt(eElementOdds.getElementsByTagName("b:OddTypeID").item(0).getTextContent());
                                String oddStartDate = (eElementOdds.getElementsByTagName("b:StartDate").item(0).getTextContent());
                                int oddStatus = Integer.parseInt(eElementOdds.getElementsByTagName("b:Status").item(0).getTextContent());


                                JSONObject jsonObj = new JSONObject();
                                if (oddStatus == 1 && oddID != 1) {
                                    jsonObj.put("lastUpdate", lastUpdate);
                                    jsonObj.put("sportName", sportName.replace('\n', ' ').replace(',', ' ').strip());
                                    jsonObj.put("sportId", sportId);
                                    jsonObj.put("sportStatus", sportStatus);
                                    jsonObj.put("groupName", groupName.replace('\n', ' ').replace(',', ' '));
                                    jsonObj.put("groupId", groupId);
                                    jsonObj.put("groupStatus", groupStatus);
                                    jsonObj.put("eventName", eventName.replace('\n', ' ').replace(',', ' '));
                                    jsonObj.put("eventId", eventId);
                                    jsonObj.put("eventTypeId", eventTypeId);
                                    jsonObj.put("eventStatus", eventStatus);
                                    //"\"BetRadarMatchID", BetRadarMatchID);
                                    jsonObj.put("subEventStartDate", subEventStartDate);
                                    jsonObj.put("subEventName", subEventName.replace('\n', ' ').replace(',', ' '));
                                    jsonObj.put("subEventStatus", subEventStatus);
                                    jsonObj.put("subEventId", subEventId);
                                    jsonObj.put("subEventPubblicationCode", SubEventPubblicationCode);
                                    jsonObj.put("oddCombinability", oddCombinability);
                                    jsonObj.put("oddEndData", oddEndData);
                                    jsonObj.put("HND", HND);
                                    jsonObj.put("odd", odd);
                                    jsonObj.put("oddClass", oddClass.replace('\n', ' ').replace(',', ' '));
                                    jsonObj.put("oddClassCode", oddClassCode.replace('\n', ' ').replace(',', ' '));
                                    jsonObj.put("oddID", oddID);
                                    jsonObj.put("oddType", oddType.replace('\n', ' ').replace(',', ' '));
                                    jsonObj.put("oddTypeCode", oddTypeCode.replace('\n', ' ').replace(',', ' '));
                                    jsonObj.put("oddTypeID", oddTypeID);
                                    jsonObj.put("oddStartDate", oddStartDate.replace('\n', ' ').replace(',', ' '));
                                    jsonObj.put("oddStatus", oddStatus);
                                    jsonArray.put(jsonObj);
                                }
                            }
                        }
                    }
                }
            }

        }
        //jsonArray.toList().parallelStream().forEach((JSONObject)a -> System.out.println(a));
        new BigQInsert(bigquery, tableId, jsonArray.toList()).start();
        writeToFile("palinsesto.json", jsonArray.toString());


    }


}

class BigQInsert extends Thread {
    BigQuery bigquery;
    TableId tableId;
    //Map map;
    List<Map<String, Object>> list;

    public BigQInsert(BigQuery bigquery, TableId tableId, List list) {
        this.bigquery = bigquery;
        this.tableId = tableId;
        this.list = list;
    }

    public void run() {

        List<Map<String, Object>> subList = new ArrayList<>();
        System.out.println("liat size " + list.size());
        for (int i = 0; i < list.size(); i++) {
            subList.add(list.get(i));
            if (i % 9999 == 0 || i == list.size() - 1) {
                System.out.println("Insert " + i);
                InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
                subList.forEach(map -> builder.addRow(map));
                bigquery.insertAll(builder.build());
                subList = new ArrayList<>();
            }
        }

    }
}
