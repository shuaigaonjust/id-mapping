import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;


public class UDTFExplodeIDs extends GenericUDTF {

	@Override
	public StructObjectInspector initialize(StructObjectInspector argOIs)
			throws UDFArgumentException {
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldsOIs = new ArrayList<ObjectInspector>();
		
		fieldNames.add("id");
		fieldsOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("product");
		fieldsOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldsOIs);
	}

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
	}

	@SuppressWarnings("unchecked")
	@Override
	public void process(Object[] arg) throws HiveException {
		// TODO Auto-generated method stub
		if (arg.length != 9) {
			throw new HiveException();
		}

		String[] product = {"imei", "mac", "imsi", "phone_number", "idfa",
				"openudid","uid","did","android_id"};

		for (int i = 0; i < arg.length; i++)
		{
			for(String s : (List<String>)arg[i]) {
				String[] result = {s, product[i]};
				forward(result);
			}
		}
	}
}
