import com.rabbitmq.client.*;

public class NewTask2 {

    private static final String NOME_FILA = "Adriano4";

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] args) throws Exception{
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection conexao = connectionFactory.newConnection();
        Channel canal = conexao.createChannel();

        canal.queueDeclare(NOME_FILA, false, false, false, null);
        System.out.println ("[*] Aguardando mensagens. Para sair, pressione CTRL + C");

        Object monitor = new Object();
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String response = "";
            int prefetchCount = 1;
            canal.basicQos(prefetchCount);
            System.out.println(" [x] Awaiting RPC requests");

            try {
                String mensagem = new String (delivery.getBody (), "UTF-8");
                int n = Integer.parseInt(mensagem);
                System.out.println(" [.] fib(" + mensagem + ")");
                response += fib(n);
                System.out.println ("[x] Recebido '" + mensagem + "'");
            } catch (RuntimeException e) {
                System.out.println(" [.] " + e.toString());
            } finally {
                System.out.println ("[x] Feito");
                canal.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                canal.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                synchronized (monitor) {
                    monitor.notify();
                }
            }
        };

        boolean autoAck = false; // ack é feito aqui. Como está autoAck, enviará automaticamente
        canal.basicConsume(NOME_FILA, autoAck, deliverCallback, (consumerTag -> {
            System.out.println("Cancelaram a fila: " + NOME_FILA);
        }));
        while (true) {
            synchronized (monitor) {
                try {
                    monitor.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
