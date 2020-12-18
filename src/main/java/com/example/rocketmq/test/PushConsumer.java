package com.example.rocketmq.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class PushConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("myConsumer");
        defaultMQPushConsumer.setNamesrvAddr("localhost:1234");
        //第二个参数表示消息过滤，*表示不作消息过滤
        defaultMQPushConsumer.subscribe("test","*");
        //defaultMQPushConsumer.subscribe("test","TAG"); //只接受发送端设置为TAG的消息
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                try {
                    // 遍历消息
                    for (int i = 0; i < list.size(); i++) {
                        MessageExt messageExt = list.get(i);
                        byte[] body = messageExt.getBody();
                        System.out.println(new String(body));
                    }
                }catch (Exception e){
                    //ack：如果该消息一直出现RECONSUME_LATER默认16次之后，就会投递到死信队列，这时一般需要人工干预处理（可直接插入数据库或者记录日志）
                    // 如果出现异常，如果是数据库操作，一般要进行回滚，然后需要服务端再次发送该数据回来，那么这里可以这样设置
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                // 消费成功的话
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        defaultMQPushConsumer.start();
    }
}
