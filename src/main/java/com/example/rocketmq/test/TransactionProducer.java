package com.example.rocketmq.test;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class TransactionProducer {
    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        TransactionMQProducer producer = new TransactionMQProducer("transaction");
        producer.setNamesrvAddr("localhost:1234");
        producer.setTransactionListener(new TransactionListener() {
            // 执行本地事务
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                /**
                 * 执行事务操作：
                 * a();
                 * b();
                 * c();
                 * 假设有异常或者if判断需要回滚等等，如：
                 * if(!c()){
                 *     return LocalTransactionState.ROLLBACK_MESSAGE;
                 * }
                 * 例如：
                 *                 try {
                 *
                 *                 }catch (Exception e){
                 *                     return LocalTransactionState.ROLLBACK_MESSAGE;
                 *                 }
                 * 当返回值为UNKOWN时，RocketMQ会每隔一段时间调用一次checkLocalTransaction，这个方法的返回值决定着这个消息的最终归宿。
                 */
                return LocalTransactionState.COMMIT_MESSAGE;
            }
            // broker回调检查,只有在executeLocalTransaction方法或本身这个方法返回结果为UNKOWN时调用。调用的周期和次数可在broker设置
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                String keys = msg.getKeys();
                String transactionId = msg.getTransactionId();
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();
        //手动关闭（异步发送时不能关闭）
        //producer.shutdown();
    }
}
