/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.learnkafka.mqttsubscriberclient;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;

/**
 *
 * @author bbiadmin
 */
public class SimpleSubscriber implements MqttCallback {

    @Override
    public void connectionLost(Throwable thrwbl) {
        System.out.println("Connection to MQTT broker lost!");
    }

    @Override
    public void messageArrived(String string, MqttMessage mm) throws Exception {
//        System.out.println("Message received:\n\t" + new String(mm.getPayload()));
        String receivedMessage = (new String(mm.getPayload())).toString();
        JSONObject receivedData = new JSONObject(receivedMessage);
        SubscriberApp.handleMessageFromTopic(receivedData);

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken imdt) {
        System.out.println("Delivery is completed");
    }

}
