package com.chirkunov.hive.udf.bill_hash;

import org.apache.calcite.avatica.org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


@Description(
        name = "hashed_bill",
        value = "_FUNC_(order_id, bill_raw_text) - returns hash by following rule: " +
                "If order_id is even, the md5 is applied N times iteratively to original text_bill value," +
                "where N is a number of capital letter 'A' appearances." +
                "Otherwise, sha256 is used once"
)
public class HashedBillUDF  extends GenericUDF {

    private ObjectInspectorConverters.Converter[] converters;

    private GenericUDFUtils.ReturnObjectInspectorResolver returnObjectInspectorResolver;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function hashed_bill(order_id, bill_raw_text) takes exactly 2 arguments.");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        converters[0] = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        converters[1] = ObjectInspectorConverters.getConverter(arguments[1],
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        returnObjectInspectorResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

        return returnObjectInspectorResolver.get();
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 2);

        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        long orderId = (Long) converters[0].convert(arguments[0].get());
        String billRawText = (String) converters[1].convert(arguments[1].get());

        return computeHash(orderId, billRawText);
    }

    private static String computeHash(long orderId, String billRawText) {
        String hash = billRawText;
        if(orderId % 2 == 0){
            int pos = 0;
            while((pos = billRawText.indexOf("A", pos)) != -1) {
                pos+=1;
                hash = DigestUtils.md5Hex(hash);
                if(pos >= billRawText.length())
                    break;
            }
        }
        else
            hash = DigestUtils.sha256Hex(hash);

        return hash;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 2);
        return "hashed_bill(" + children[0] + ", " + children[1] + ")";
    }

    public static void main(String[]args){
        System.out.println(computeHash(1, "AxxxxxxxA"));
        System.out.println(computeHash(0, "AxxxxxxxA"));
    }
}
