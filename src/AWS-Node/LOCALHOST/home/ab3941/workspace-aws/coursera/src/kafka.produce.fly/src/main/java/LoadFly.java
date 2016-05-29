import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class LoadFly {
	public static void main(String args[]) throws InterruptedException, ExecutionException {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put("request.required.acks", "1");
		props.put("acks", "1");

        // how many times to retry when produce request fails?
//		props.put("retries", "3");
//		props.put("linger.ms", 5);
		props.put("retries", "5");
		props.put("linger.ms", 20);
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		boolean sync = false;
		String topic = args[0];
		String key = "fly_short";
	//	String value = "myvalue";

	//	Charset charset = Charset.forName("US-ASCII");
		try (BufferedReader reader = new BufferedReader(new FileReader(args[1]))) {
	//	try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
			
			String line = null;
			long readlines = 0;
			long validlines =0;
			while ((line = reader.readLine()) != null) {
readlines+=1;
				String[] fields = line.toString().replace("\"", "").split((","));

				if (fields.length > 45) { // this field indicate a fly been
											// canceled
					ProducerRecord<String, String> producerRecord = null;

					if (!fields[0].equals("Year")) {// skip the first line of
													// each file as contain
													// header
						// if (Integer.parseInt(field[48])==0 )
						if ((!fields[43].isEmpty() && Float.parseFloat(fields[43]) == 0.00) && (!fields[45].isEmpty() && Float.parseFloat(fields[45]) == 0.00)) {
validlines+=1;
							String fly =fields[4]+","+fields[5]+","+fields[6]+","+fields[10]+","+fields[11]+","+fields[18]+","+fields[26]+","+fields[27]+","+fields[28]+","+fields[29]+","+fields[37]+","+fields[38]+","+fields[39]+","+fields[40];
							producerRecord = new ProducerRecord<String, String>(topic, key, fly);

							if (sync) {
								producer.send(producerRecord).get();
							} else {
								producer.send(producerRecord);
							}
						}

					}

//			System.out.println(line);
	
				}
			}
			System.out.println(String.valueOf(validlines)+"/"+String.valueOf(readlines));
		} catch (IOException x) {
			producer.close();
			System.err.format("IOException: %s%n", x);
		}

		producer.close();
	}
}
