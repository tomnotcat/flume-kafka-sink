package com.tedwon.flume.sink;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import kafka.message.Message;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka Flume Sink Class.
 * <p/>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 1.0
 */
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

    private String zkConnect;
    private String topic;
    private String serializerClass;

    private Properties props;

    private Producer<String, Message> producer;

    @Override
    public void configure(Context context) {
        this.zkConnect = context.getString("zkConnect", "localhost:2181");
        this.topic = context.getString("topic", "test");
        this.serializerClass = context.getString("serializerClass", "kafka.serializer.DefaultEncoder");
    }

    @Override
    public synchronized void start() {
        super.start();

        props = new Properties();
        props.put("zk.connect", this.zkConnect);
        props.put("serializer.class", this.serializerClass);

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, Message>(config);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        producer.close();
    }

    @Override
    public Status process() throws EventDeliveryException {

        Status status = Status.READY;

        Channel channel = getChannel();
        Transaction tx = null;
        try {
            tx = channel.getTransaction();
            tx.begin();

            Event event = channel.take();

            if (event != null) {
//                String line = EventHelper.dumpEvent(event);
//                logger.debug(line);

                byte[] body = event.getBody();

                // Send a single messae
                // The message is sent to a randomly selected partition registered in ZK
                Message msg = new Message (body);
                ProducerData<String, Message> kafkaData = new ProducerData<String, Message>(this.topic, msg);
                producer.send(kafkaData);

            } else {
                status = Status.BACKOFF;
            }

            tx.commit();
        } catch (Exception e) {
            logger.error("can't process events, drop it!", e);
            if (tx != null) {
                tx.commit();// commit to drop bad event, otherwise it will enter dead loop.
            }

            throw new EventDeliveryException(e);
        } finally {
            if (tx != null) {
                tx.close();
            }
        }
        return status;
    }
}
