package com.example.rocketmq.test;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

public class Producer {
    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("myProducer");
        defaultMQProducer.setNamesrvAddr("localhost:1234");
        defaultMQProducer.start();
        syncSend(defaultMQProducer);
        //手动关闭（异步发送时不能关闭）
        defaultMQProducer.shutdown();
    }

    /**
     * 同步发送
     * @param defaultMQProducer
     * @throws InterruptedException
     * @throws RemotingException
     * @throws MQClientException
     * @throws MQBrokerException
     */
    public static void syncSend(DefaultMQProducer defaultMQProducer) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        //发送失败后重试次数，默认2
        defaultMQProducer.setRetryTimesWhenSendFailed(1);
        //发送失败后是否像其他broker发送
        defaultMQProducer.setRetryAnotherBrokerWhenNotStoreOK(true);
        //同步发送，返回结果
        SendResult result = defaultMQProducer.send(new Message("test", "sdf".getBytes()));
        //查看发送结果，详情
        System.out.println(result.toString());
    }

    /**
     * 同步批量发送
     * 注意：topic必须一致
     * 注意：发送的消息大小不超过1M，如果有必要可拆分多个小的list发送。
     * @param defaultMQProducer
     * @throws InterruptedException
     * @throws RemotingException
     * @throws MQClientException
     * @throws MQBrokerException
     */
    public static void syncSendList(DefaultMQProducer defaultMQProducer) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        //同步发送批量发送，topic必须一致
        Message m0 = new Message("test", "sdf".getBytes());
        Message m1 = new Message("test", "sdf1".getBytes());
        List<Message> list =new ArrayList<>();
        list.add(m0);
        list.add(m1);
        SendResult result = defaultMQProducer.send(list);
        //查看发送结果，详情
        System.out.println(result.toString());
    }

    /**
     * 异步发送，时间监听
     * @param defaultMQProducer
     */
    public static void asyncSend(DefaultMQProducer defaultMQProducer) throws RemotingException, MQClientException, InterruptedException {
        defaultMQProducer.send(new Message("test", "sdfs".getBytes()), new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println(sendResult.toString());
            }

            @Override
            public void onException(Throwable e) {
                System.out.println("异步发送消息异常");
                //是否继续尝试发送？
            }
        });
    }

    /**
     *消息tag
     * @param defaultMQProducer
     */
    public static void tagSend(DefaultMQProducer defaultMQProducer) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        SendResult send = defaultMQProducer.send(new Message("test", "TAG", "sdfsg".getBytes()));
    }
    public static void keySend(DefaultMQProducer defaultMQProducer) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        SendResult send = defaultMQProducer.send(new Message("test", "TAG", "key","sdfsg".getBytes()));

    }
    public static void selectQueueSend( DefaultMQProducer defaultMQProducer) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        defaultMQProducer.send(new Message("test", "dsfdf".getBytes()), new MessageQueueSelector() {
            /**
             *
             * @param mqs topic下的队列
             * @param msg 你发送的消息
             * @param arg 传递的参数，和此处send（）最后一个参数一致
             * @return
             */
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                System.out.println(arg.toString());//flag1
                return mqs.get(0);
            }
        },"flag1");
    }
}
