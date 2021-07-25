package mr.sales.transaction.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * rge mum-nationat retail cnain nas sales orders data across regions and different sales channels large variety of item types. The business team wants to use this data to analyze various aspects Ides z e.g. top selling items in a region, regions with maximum profit in a certain item type, if is a significant difference in revenue in two item types across regions etc.
 * problem statement:
 * The data analytics team, use the sales transaction data set with about 100K records to answer e questions below —
 * 1) Average unit_price by country for a given item type in a certain year
 * 2)Total units sold by year for a given country and a given item type
 * 3)Find the max and min units sold in any order for each year by country for a given item type.
 * 4)Use a custom partitioner class instead of default hash based.
 * 5)What are the top 10 order id for a given year by the total_profit  a have to show the above analysis working on a Hadoop system using map reduce code, preferably 'ava or Python. You can do data preparation steps as required before running a MapReduce job to .wer these auestions above.
 */
public class SalesTransactionQ1Driver extends Configured implements Tool {

    /**
     * 1) Average unit_price by country for a given item type in a certain year
     * IDE run configuration params : mobile 2019 /Users/harishkumar/IdeaProjects/map-reduce/input/
     * /Users/harishkumar/IdeaProjects/map-reduce/output/
     */
    @Override
    public int run(String[] args) throws Exception {
        try {
            if (args.length != 4) {
                System.err.printf("Usage: %s [generic options] <itemType> <year> <input> <output>\n",
                        getClass().getSimpleName());
                ToolRunner.printGenericCommandUsage(System.err);
                return -1;
            }

            Configuration conf = getConf();
            conf.set("reqItemType", args[0]);
            conf.set("reqYear", args[1]);

            Job job = new Job(conf, "Average Unit price");
            job.setJarByClass(getClass());

            FileInputFormat.addInputPath(job, new Path(args[2]));
            FileOutputFormat.setOutputPath(job, new Path(args[3]));

            job.setMapperClass(SalesTransactionQ1Mapper.class);
            //job.setCombinerClass(SalesTransactionQ1Mapper.class);
            job.setReducerClass(SalesTransactionQ1Reducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception genEx) {
            genEx.printStackTrace();
            throw genEx;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SalesTransactionQ1Driver(), args);
        System.exit(exitCode);
    }

    //http://www.thecloudavenue.com/2012/10/debugging-hadoop-mapreduce-program-in.html
}
