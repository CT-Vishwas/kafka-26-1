import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;



@Configuration
public class KafkaListenerConfig {

		   @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
            kafkaListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory,
                OrderRebalanceListener rebalanceListener) {

        var factory =
            new ConcurrentKafkaListenerContainerFactory<String, String>();

        factory.setConsumerFactory(consumerFactory);

        factory.getContainerProperties()
               .setConsumerRebalanceListener(rebalanceListener);

        return factory;
    }

}