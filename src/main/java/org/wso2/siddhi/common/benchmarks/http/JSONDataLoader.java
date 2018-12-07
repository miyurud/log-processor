/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.common.benchmarks.http;
import com.google.common.base.Splitter;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.InputHandler;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Data loader for EDGAR log files.
 */

public class JSONDataLoader extends Thread {
    private long events = 0;
    private long startTime;
    private Splitter splitter = Splitter.on(',');
    private InputHandler inputHandler;
    long startTime2;
    private int temp = 0;
    JSONDataLoader jsLoader = null;
    int i = 0;

    Long timestamp = 1000000000000L;

    private static final Logger log = Logger.getLogger(JSONDataLoader.class);

    public static void main(String[] args) {

        BasicConfigurator.configure();

        log.info("Welcome to kafka message sender");

        JSONDataLoader loader1 = new JSONDataLoader();
        JSONDataLoader loader2 = new JSONDataLoader(loader1);

        loader2.start();
    }

    public JSONDataLoader() {
        jsLoader = this;
    }

    public JSONDataLoader(JSONDataLoader js) {
        jsLoader = js;
    }

    public void dataRate() {
        jsLoader.temp++;
            if (jsLoader.temp == 1) {
                jsLoader.startTime2 = System.currentTimeMillis();
            }
            long diff = System.currentTimeMillis() - jsLoader.startTime2;
            jsLoader.timestamp = jsLoader.timestamp + 7000000;
            if (jsLoader.i == 11) {
               jsLoader.i = -1;
            }
            jsLoader.i++;
            log.info("Timestamp is : " + jsLoader.timestamp +
                    " using " + Thread.currentThread().getName());
            long datarate = (jsLoader.temp * 1000  / diff);

            log.info(Thread.currentThread().getName() + " spent : "
                    + diff + " for the event count : " + jsLoader.temp
                    + " with the  Data rate : " + datarate);
    }



    public JSONDataLoader(InputHandler inputHandler) {
        super("Data Loader");
        this.inputHandler = inputHandler;
    }
    public void run() {
        BufferedReader br = null;

        try {
            Locale locale = new Locale("en", "US");
            ResourceBundle bundle2 = ResourceBundle.getBundle("config", locale);

            String inputFilePath = bundle2.getString("input");
            br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilePath),
                    Charset.forName("UTF-8")));
            String line = br.readLine();
            line = br.readLine(); //We need to ignore the first line which has the headers.
            startTime = System.currentTimeMillis();
            while (line != null) {
                try {
                    log.info(Thread.currentThread().getName() + " is  sleeping");
                    Thread.currentThread().sleep(100);

                } catch (InterruptedException e) {
                    log.info("Error: " + e.getMessage());
                }


                //We make an assumption here that we do not get empty strings due to missing values that may present
                // in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String ipAddress = dataStrIterator.next(); //This variable provides the first three octets of the IP
                // address with the fourth octet obfuscated with a 3 character string that preserves the uniqueness of
                // the last octet without revealing the full identity of the IP (###.###.###.xxx)
                String date = dataStrIterator.next(); //yyyy-mm-dd
                String time = dataStrIterator.next(); //hh:mm:ss
                String zone = dataStrIterator.next(); //Zone is Apache log file zone. The time zone associated with the
                // server that completed processing the request.
                String cik = dataStrIterator.next(); //SEC Central Index Key (CIK) associated with the document
                // requested
                String accession = dataStrIterator.next(); //SEC document accession number associated with the
                // document requested
                String doc = dataStrIterator.next(); //This variable provides the filename of the file requested
                // including the document extension
                String code = dataStrIterator.next(); //Apache log file status code for the request
                String size = dataStrIterator.next(); //document file size
                size = size.equals("") ? "0.0" : size;


                String idx = dataStrIterator.next(); //takes on a value of 1 if the requester landed on the index page
                // of a set of documents (e.g., index.htm), and zero otherwise
                String norefer = dataStrIterator.next(); //takes on a value of one if the Apache log file referrer
                // field is empty, and zero otherwise
                String noagent = dataStrIterator.next(); //takes on a value of one if the Apache log file user agent
                // field is empty, and zero otherwise
                String find = dataStrIterator.next(); //numeric values from 0 to 10, that correspond to whether the
                // following character strings /[$string]/were found in the referrer field – this could indicate how
                // the document requester arrived at the document link (e.g., internal EDGAR search)
                String crawler = dataStrIterator.next(); //This variable takes on a value of one if the user agent
                // self-identifies as one of the following webcrawlers or has a user code of 404.
                String browser = dataStrIterator.next(); //This variable is a three character string that identifies
                // potential browser type by analyzing whether
                // the user agent field contained the following /[text]/
                //browser = browser.equals("") ? "-":browser;

                StringBuilder jsonDataItem = new StringBuilder();
                jsonDataItem.append("{ \"event\": { ");
                jsonDataItem.append("\"iij_timestamp\"");
                jsonDataItem.append(":");
                jsonDataItem.append(System.currentTimeMillis());
                jsonDataItem.append(",");

                jsonDataItem.append("\"ip\"");
                jsonDataItem.append(":\"");
                jsonDataItem.append(ipAddress);
                jsonDataItem.append("\",");


                jsonDataItem.append("\"timestamp\"");
                jsonDataItem.append(":");
                jsonDataItem.append(jsLoader.timestamp);
                jsonDataItem.append(",");

                jsonDataItem.append("\"zone\"");
                jsonDataItem.append(":");
                jsonDataItem.append(zone);
                jsonDataItem.append(",");

                jsonDataItem.append("\"cik\"");
                jsonDataItem.append(":");
                jsonDataItem.append(cik);
                jsonDataItem.append(",");

                jsonDataItem.append("\"accession\"");
                jsonDataItem.append(":\"");
                jsonDataItem.append(accession);
                jsonDataItem.append("\",");

                jsonDataItem.append("\"doc\"");
                jsonDataItem.append(":\"");
                jsonDataItem.append(doc);
                jsonDataItem.append("\",");

                jsonDataItem.append("\"code\"");
                jsonDataItem.append(":");
                jsonDataItem.append(code);
                jsonDataItem.append(",");

                jsonDataItem.append("\"size\"");
                jsonDataItem.append(":");
                jsonDataItem.append(size);
                jsonDataItem.append(",");

                jsonDataItem.append("\"idx\"");
                jsonDataItem.append(":");
                jsonDataItem.append(idx);
                jsonDataItem.append(",");

                jsonDataItem.append("\"norefer\"");
                jsonDataItem.append(":");
                jsonDataItem.append(norefer);
                jsonDataItem.append(",");

                jsonDataItem.append("\"noagent\"");
                jsonDataItem.append(":");
                jsonDataItem.append(noagent);
                jsonDataItem.append(",");

                jsonDataItem.append("\"find\"");
                jsonDataItem.append(":");
                jsonDataItem.append(find);
                jsonDataItem.append(",");

                jsonDataItem.append("\"crawler\"");
                jsonDataItem.append(":");
                jsonDataItem.append(crawler);
                jsonDataItem.append(",");


                jsonDataItem.append("\"browser\"");
                jsonDataItem.append(":\"");
                jsonDataItem.append(browser);
                jsonDataItem.append("\" } }");

                try {
                    KafkaMessageSender.runProducer(jsonDataItem.toString());
                    dataRate();
                } catch (InterruptedException e) {
                    log.error("Error sending an event to Input Handler, " + e.getMessage(), e);
                } catch (Exception e) {
                    log.error("Error: " + e.getMessage(), e);
                }

                line = br.readLine();
            }
        } catch (FileNotFoundException e) {
            log.error("Error in accessing the input file. " + e.getMessage(), e);
        } catch (IOException e2) {
            log.error("Error in accessing the input file. " + e2.getMessage(), e2);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    log.error("Error in accessing the input file. " + e.getMessage(), e);
                }
            }
        }
    }
}
