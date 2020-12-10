package section2;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

//PCollection - this class will insert the CustomerEntity names in a file
public class InmemoryExample {

	public static void main(String[] args) {
		
		Pipeline p = Pipeline.create();
		
		PCollection<CustomerEntity> pList = p.apply(Create.of(getCustomers()));
		
		PCollection<String> pStrList = pList.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity cust) -> cust.getName()));
		
		pStrList.apply(TextIO.write().to("/home/sabb/Documents/Beam/Section2/customer.csv").withNumShards(1).withSuffix(".csv"));
		
		p.run();
	}
	
	
	static List<CustomerEntity> getCustomers(){
		
		CustomerEntity c1 = new CustomerEntity("1001","John");
		CustomerEntity c2 = new CustomerEntity("1002","Adam");
		
		List<CustomerEntity> list = new ArrayList<CustomerEntity>();
		list.add(c1);
		list.add(c2);
		
		return list;
	}
}