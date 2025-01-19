package dev.mark.demos.kafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onComment(String arg0) throws Exception {
        // nothing here
        
    }

    @Override
    public void onError(Throwable t)  {
        log.error("Error in Stream Reading", t);
        
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        // asynchronous
        kafkaProducer.send(new ProducerRecord<String,String>(topic, messageEvent.getData()));
    }

    @Override
    public void onOpen() throws Exception {
        // nothing here
    }

    
    
}
