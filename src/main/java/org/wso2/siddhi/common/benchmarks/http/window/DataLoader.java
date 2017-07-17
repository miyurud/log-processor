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

package org.wso2.siddhi.common.benchmarks.http.window;

import com.google.common.base.Splitter;
import org.apache.log4j.Logger;
import org.joda.time.format.ISODateTimeFormat;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.ResourceBundle;

/**
 * Data loader for EDGAR log files.
 */

public class DataLoader extends Thread {
    private long events = 0;
    private long startTime;
    private Splitter splitter = Splitter.on(',');
    private InputHandler inputHandler;
    private static final Logger log = Logger.getLogger(DataLoader.class);

    public DataLoader(InputHandler inputHandler) {
        super("Data Loader");
        this.inputHandler = inputHandler;
    }

    public void run() {
        BufferedReader br = null;

        try {
            ResourceBundle bundle = ResourceBundle.getBundle("config");

            String inputFilePath = bundle.getString("input");
            br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilePath),
                                                          Charset.forName("UTF-8")));
            String line = br.readLine();
            line = br.readLine(); //We need to ignore the first line which has the headers.
            startTime = System.currentTimeMillis();
            while (line != null) {

                events++;

                //We make an assumption here that we do not get empty strings due to missing values that may present
                // in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String ipAddress = dataStrIterator.next(); //
                String date = dataStrIterator.next(); //yyyy-mm-dd
                String time = dataStrIterator.next(); //hh:mm:ss
                String zone = dataStrIterator.next(); //Zone is Apache log file zone
                String cik = dataStrIterator.next(); //SEC Central Index Key (CIK) associated with the document
                // requested
                String accession = dataStrIterator.next(); //SEC document accession number associated with the
                // document requested
                String extention = dataStrIterator.next();
                String code = dataStrIterator.next();
                String size = dataStrIterator.next();
                String idx = dataStrIterator.next();
                String norefer = dataStrIterator.next();
                String noagent = dataStrIterator.next();
                String find = dataStrIterator.next();
                String crawler = dataStrIterator.next();
                String browser = dataStrIterator.next();
                long timestamp = ISODateTimeFormat.dateTime().parseDateTime(date + "T" + time + ".000+0000")
                        .getMillis();

                Object[] dataItem = new Object[]{System.currentTimeMillis(), ipAddress, timestamp, zone, cik,
                                                 accession, extention, code, size,
                                                 idx, norefer, noagent, find, crawler, browser};

                try {
                    inputHandler.send(dataItem);
                } catch (InterruptedException e) {
                    log.error("Error sending an event to Input Handler, " + e.getMessage(), e);
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
