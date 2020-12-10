package section2;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class LocalFileExample {

	public static void main(String[] args) {
		
		Myoptions options=PipelineOptionsFactory.fromArgs(args).withValidation().as(Myoptions.class);
		
		
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<String> output= pipeline.apply(TextIO.read().from(options.getInputFile()));
		
		output.apply(TextIO.write().to(options.getOutputFile()).withNumShards(1).withSuffix(options.getExtn()));
				
		pipeline.run();
		
	}
}
