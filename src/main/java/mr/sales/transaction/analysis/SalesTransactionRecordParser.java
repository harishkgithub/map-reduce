package mr.sales.transaction.analysis;

import org.apache.hadoop.io.Text;

import java.util.StringTokenizer;

public class SalesTransactionRecordParser {

    private long orderId;
    private String itemId;
    private String itemType;

    private float unitPrice;
    private long unitsSold;
    private String transYear;
    private String country;

    public void parse(String record) {
        //TODO Shruti: is this perfomant? or should we use regular split by column
        StringTokenizer st = new StringTokenizer(record, ",");
        if (st.hasMoreTokens()) {
            orderId = Long.parseLong(st.nextToken());
        }
        if (st.hasMoreTokens()) {
            itemId = st.nextToken();
        }
        if (st.hasMoreTokens()) {
            itemType = st.nextToken();
        }
        if (st.hasMoreTokens()) {
            unitPrice = Float.parseFloat(st.nextToken());
        }
        if (st.hasMoreTokens()) {
            unitsSold = Long.parseLong(st.nextToken());
        }
        if (st.hasMoreTokens()) {
            transYear = st.nextToken();
        }
        if (st.hasMoreTokens()) {
            country = st.nextToken();
        }
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public long getOrderId() {
        return orderId;
    }

    public String getItemId() {
        return itemId;
    }

    public String getItemType() {
        return itemType;
    }

    public float getUnitPrice() {
        return unitPrice;
    }

    public long getUnitsSold() {
        return unitsSold;
    }

    public String getTransYear() {
        return transYear;
    }

    public String getCountry() {
        return country;
    }

}
