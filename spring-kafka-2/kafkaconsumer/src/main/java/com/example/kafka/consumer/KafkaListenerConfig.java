import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

@Configuration
public class KafkaListenerConfig {

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
						ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
						ConsumerFactory<Object, Object> consumerFactory) {
				ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
								new ConcurrentKafkaListenerContainerFactory<>();
				configurer.configure(factory, consumerFactory);

				// Up to 3 concurrent consumers per listener (bounded by partition count)
				factory.setConcurrency(3);
				return factory;
		}
}