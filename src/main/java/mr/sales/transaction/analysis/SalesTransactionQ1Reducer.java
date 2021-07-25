package mr.sales.transaction.analysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class SalesTransactionQ1Reducer
        extends Reducer<Text, FloatWritable, Text, DoubleWritable> {

    private static Logger logger = LoggerFactory.getLogger(SalesTransactionQ1Reducer.class);

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context)
            throws IOException, InterruptedException {
        Integer count = 0;
        Double sum = 0D;
        final Iterator<FloatWritable> itr = values.iterator();
        while (itr.hasNext()) {
            final String text = itr.next().toString();
            final Double value = Double.parseDouble(text);
            count++;
            sum += value;
        }

        final Double average = sum / count;
        logger.info("{} --> {} ", key, average);

        context.write(key, new DoubleWritable(average));
    }
}
