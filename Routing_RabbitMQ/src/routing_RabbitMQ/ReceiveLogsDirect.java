package routing_RabbitMQ;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback; 

public class ReceiveLogsDirect {
	
	private static final String EXCHANGE_NAME = "direct_logs";

	  public static void main(String[] argv) throws Exception {
	    
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();

	    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
	    String queueName = channel.queueDeclare().getQueue();

	    if (argv.length < 1) {
	        System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error] || use cualquier tag");
	        System.exit(1);
	    }

	    for (String severity : argv) {
	    	// creo el Binding con cada uno de los parametros que use al ejecutar la app en la misma cola 
	        channel.queueBind(queueName, EXCHANGE_NAME, severity);
	    }
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

	    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
	        String message = new String(delivery.getBody(), "UTF-8");
	        System.out.println(" [x] Received '" +
	            delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
	    };
	    channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
	  }
	 
}
