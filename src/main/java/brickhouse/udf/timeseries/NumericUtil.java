package brickhouse.udf.timeseries;

import java.math.BigDecimal;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;

/*
  In Hive 2.3.6, Double numbers get transferred as Decimal objects to UDF. To fix
  UDFS, we need to treat Decimal as a numeric catergory. The previous NumericUtil
  provided by Brickhouse did not do so. For that reason, we redid the NumericUtil
  class and added Decimal Support.
 */
public class NumericUtil {
    public static boolean isNumericCategory(PrimitiveCategory cat) {
        switch (cat) {
            case DOUBLE:
            case FLOAT:
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }
    public static double getNumericValue(PrimitiveObjectInspector objInsp, Object val) {
        switch (objInsp.getPrimitiveCategory()) {
            case DOUBLE:
                return ((DoubleObjectInspector) objInsp).get(val);
            case FLOAT:
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
                Number num = (Number) objInsp.getPrimitiveJavaObject(val);
                return num.doubleValue();
            case DECIMAL:
                return ((HiveDecimalObjectInspector) objInsp).getPrimitiveJavaObject(val).doubleValue();
            default:
                return 0.0;
        }
    }


    /**
     * Cast a double to an object required by the ObjectInspector
     * associated with the given PrimitiveCategory
     *
     * @param val
     * @param cat
     * @return
     */
    public static Object castToPrimitiveNumeric(double val, PrimitiveCategory cat) {
        switch (cat) {
            case DOUBLE:
                return new Double(val);
            case FLOAT:
                return new Float((float) val);
            case LONG:
                return new Long((long) val);
            case INT:
                return new Integer((int) val);
            case SHORT:
                return new Short((short) val);
            case BYTE:
                return new Byte((byte) val);
            case DECIMAL:
                return HiveDecimal.create(new BigDecimal(val));
            default:
                return null;
        }
    }
}
