package workQueues_RabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class NewTask {
	private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        
    	//se crea la conexion con el servidor
    	ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel())
        {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            
            String message = String.join(" ", argv);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
