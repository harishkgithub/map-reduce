package mr.sales.transaction.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SalesTransactionQ1Mapper
        extends Mapper<LongWritable, Text, Text, FloatWritable> {

    String itemType = "-1";
    String transYear = "1970";

    private static Logger logger = LoggerFactory.getLogger(SalesTransactionQ1Mapper.class);

    enum Temperature {
        MALFORMED
    }

    private SalesTransactionRecordParser parser = new SalesTransactionRecordParser();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        itemType = conf.get("reqItemType");
        transYear = conf.get("reqYear");
        logger.info("Filtering for itemType : {} , year : {}", itemType, transYear);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            parser.parse(value);

            //For given item type in a certain year
            if (itemType.equalsIgnoreCase(parser.getItemType()) &&
                    transYear.equalsIgnoreCase(parser.getTransYear())) {
                context.write(new Text(parser.getCountry()), new FloatWritable(parser.getUnitPrice()));
            }
        } catch (NullPointerException npe) {
            System.err.println("Ignoring possibly corrupt input: " + value);
            context.getCounter(Temperature.MALFORMED).increment(1);
        }

    }
}
