/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.learnkafka.mqttsubscriberclient;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.json.JSONObject;

/**
 *
 * @author bbiadmin
 */
public class SubscriberApp {

    private static String LANDING_PATH = "E://mqtt/downloads/";
    private static String topic = "/iot/data/logs";
    private static String host = "tcp://localhost:1883";

    public static void main(String[] args) {
        try {
            MqttClient client = new MqttClient(host, MqttClient.generateClientId());
            client.setCallback(new SimpleSubscriber());
            client.connect();
            client.subscribe(topic, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void handleMessageFromTopic(JSONObject data) {
        System.out.println("Data type = " + data);
        int dataType = Integer.parseInt(data.get("dataType").toString());
        System.out.println("Data Type = " + dataType);
        if (dataType == 1) {
            System.out.println("Header received");
            System.out.println("Creating a file with the name " + data.get("fileName").toString());
            boolean createFileStatus = createFile(LANDING_PATH + data.get("fileName").toString());
            System.out.println("Creating file status " + createFileStatus);
        } else if (dataType == 2) {
            System.out.println("file content received");
            System.out.println("Writing the data to file ");
            boolean writeFileStatus = writeDataToFile(data.get("data").toString(), LANDING_PATH + data.get("fileName").toString());
            System.out.println("Writing data to file status " + writeFileStatus);
        } else if (dataType == 3) {
            System.out.println("Footer Received");
            System.out.println("Closing the file!");
        }
    }

    private static boolean createFile(String path) {
        try {
            Path newFilePath = Paths.get(path);
            Files.createFile(newFilePath);
            return true;
        } catch (IOException e) {
            System.out.println("File creation exception");
            return false;
        }
    }

    private static boolean writeDataToFile(String data, String filePath) {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(filePath, true));
            out.write("\n"+data);
            out.close();
            return true;
        } catch (Exception e) {
            System.out.println("Problem writing to file");
            return false;
        }
    }
}
