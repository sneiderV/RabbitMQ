package workQueues_RabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Worker {
	private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        
    	//se crea la conexion con el servidor
    	ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        
        //se declara la cola que se va a usar dado que puede empezar primero el Recv que el Send
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        //permite la recepción de unicamente # mensajes antes de seguir recibiendo
        //si esta ocupado la cola debe de mantener el 
        int prefetchCount = 1;
        channel.basicQos(prefetchCount); 
        
        //recive los mensajes de la cola asincronamente
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            
        	String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            
            //simulación de espera con el mensaje que recive
            try {
					doWork(message);
			} 
            catch (InterruptedException e) {
				e.printStackTrace();
			} 
            finally {
				System.out.println(" [x] Done");
				//esta linea asegura que no se pierda ningun mensaje si un consumidor se cae 
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
        };
        
        //boolean autoAck = true;
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }
    
    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }
}
