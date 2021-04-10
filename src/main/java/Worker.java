import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class Worker implements AutoCloseable{

    private Connection connection;
    private Channel canal;
    private String NOME_FILA = "Adriano4";
    String mensagem = " Adriano Ney Nascimento do Amaral ";

    public Worker() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        connection = connectionFactory.newConnection();
        canal = connection.createChannel();
    }

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        try (Worker fibonacciRpc = new Worker()) {
            for (int i = 0; i < 32; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                String response = fibonacciRpc.call(i_str);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

        public String call(String message) throws IOException, InterruptedException {
            final String corrId = UUID.randomUUID().toString();

            String replyQueueName = canal.queueDeclare().getQueue();
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(corrId)
                    .replyTo(replyQueueName)
                    .build();

            canal.basicPublish("", NOME_FILA, props, message.getBytes("UTF-8"));
            canal.basicPublish ("", NOME_FILA, MessageProperties.PERSISTENT_TEXT_PLAIN, mensagem.getBytes ());
            canal.basicPublish ("", NOME_FILA, MessageProperties.PERSISTENT_TEXT_PLAIN, "Não".getBytes ());
            canal.basicPublish ("", NOME_FILA, MessageProperties.PERSISTENT_TEXT_PLAIN, "é".getBytes ());
            canal.basicPublish ("", NOME_FILA, MessageProperties.PERSISTENT_TEXT_PLAIN, "que".getBytes ());
            canal.basicPublish ("", NOME_FILA, MessageProperties.PERSISTENT_TEXT_PLAIN, "funciona".getBytes ());

            System.out.println ("[x] Enviado '" + mensagem + "'");

            final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

            String ctag = canal.basicConsume(replyQueueName, false, (consumerTag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                    response.offer(new String(delivery.getBody(), "UTF-8"));
                }
            }, consumerTag -> {
            });

            String result = response.take();
            canal.basicCancel(ctag);
            return result;
        }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
