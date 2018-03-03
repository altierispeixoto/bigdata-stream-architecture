package consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import consumer.AmazonKinesisApplicationSampleRecordProcessor;

/**
 *
 * Created by altieris on 01/03/18.
 *
 */
public class AmazonKinesisApplicationRecordProcessorFactory implements IRecordProcessorFactory {
    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new AmazonKinesisApplicationSampleRecordProcessor();
    }

}