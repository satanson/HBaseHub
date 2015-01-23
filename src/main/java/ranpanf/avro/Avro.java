package ranpanf.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.*;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import static org.apache.commons.collections4.IteratorUtils.*;

import org.apache.commons.collections4.Predicate;
import org.apache.log4j.Logger;

import ranpanf.hbasehub.HClusterDescriptor;
/**
 * Avro.
 * @author satanson
 *
 */
public class Avro {
	static Logger log = Logger.getLogger(Avro.class);
	private Class<?> target = null;
	private Schema schema = null;

	public static Avro avroize(Class<?> cls) {
		Avro a = new Avro();
		a.setTarget(cls);
		return a;
	}

	public Class<?> getTarget() {
		return target;
	}

	public void setTarget(Class<?> target) {
		this.target = target;
	}

	public Schema getSchema() {
		if (schema != null)
			return schema;

		RecordBuilder<Schema> o = SchemaBuilder.record(target.getSimpleName())
				.namespace(target.getPackage().getName());

		FieldAssembler<Schema> f = o.fields();
		for (Method m : getSetters(target)) {
			String fieldName = lowerFirst(m.getName().substring("set".length()));
			f = f.name(fieldName).type().stringType().noDefault();
		}
		schema = f.endRecord();
		return schema;
	}

	private static List<Method> getSetters(Class<?> cls) {
		return getPrefixedWith(cls, "set");
	}

	private static List<Method> getGetters(Class<?> cls) {
		return getPrefixedWith(cls, "get");
	}

	private static List<Method> getPrefixedWith(Class<?> cls,
			final String prefix) {
		List<Method> ms = Arrays.asList(cls.getDeclaredMethods());
		return toList(filteredIterator(ms.iterator(), new Predicate<Object>() {
			public boolean evaluate(Object arg) {
				Method m = (Method) arg;
				int mod = m.getModifiers();
				String name = m.getName();
				if (Modifier.isPublic(mod) && 
					!Modifier.isStatic(mod) && 
					!Modifier.isAbstract(mod) &&
					name.startsWith(prefix)) {
					return true;
				} else {
					return false;
				}
			}
		}));
	}

	boolean isValidField(Class<?> cls, String name) {
		boolean ok = false;
		try {
			Field field = cls.getField(name);
			ok = field != null && field.getType() == String.class;
		} catch (Exception ex) {
			log.fatal(ex.getMessage(), ex);
		}
		return ok;
	}

	private static String lowerFirst(String s) {
		char[] cx = s.toCharArray();
		cx[0] = Character.toLowerCase(cx[0]);
		return String.valueOf(cx);
	}

	public byte[] dehydrate(Object o) throws Exception {
		assert o.getClass() == target;
		Schema schm = getSchema();
		DatumWriter<GenericRecord> dataWriter 
			= new GenericDatumWriter<GenericRecord>(schm);
		GenericRecord record = new GenericData.Record(schm);
		ByteArrayOutputStream dataOut 
			= new ByteArrayOutputStream();
		BinaryEncoder dataEncoder 
			= EncoderFactory.get().binaryEncoder(dataOut,null);
		
		for (Method getter : getGetters(target)) {
			String fieldName=lowerFirst(getter.getName().substring("get".length()));
			//log.warn(getter.getName());
			Object fieldValue=getter.invoke(o);
			//log.warn(fieldValue.getClass().getCanonicalName());
			//log.warn(fieldName+": "+fieldValue);
			record.put(fieldName, fieldValue);
		}
		dataWriter.write(record, dataEncoder);
		dataEncoder.flush();
		return dataOut.toByteArray();
	}

	public Object hydrate(byte[] data)throws Exception {
		Schema schm=getSchema();
		ByteArrayInputStream dataIn=new ByteArrayInputStream(data);
		DatumReader<GenericRecord> dataReader = new GenericDatumReader<GenericRecord>(schm);
		BinaryDecoder dataDecoder=DecoderFactory.get().binaryDecoder(dataIn,null);
		GenericRecord record=dataReader.read(null,dataDecoder);
		Object o=target.newInstance();
		for(Method setter: getSetters(target)){
			String fieldName=lowerFirst(setter.getName().substring("set".length()));
//			log.warn(setter.getName()+":"+fieldName+":"+record.get(fieldName));
//			log.warn(o.getClass().getCanonicalName());
//			log.warn(record.get(fieldName).getClass().getCanonicalName());
			setter.invoke(o,record.get(fieldName).toString());
		}
		return o;
	}

	public static class Test {
		public static void main(String[] args) throws Exception {
			Avro avro = Avro.avroize(HClusterDescriptor.class);
			System.out.println(avro.getSchema());
			HClusterDescriptor desc=new HClusterDescriptor();
			desc.setHmaster("node1");
			desc.setPort("50070");
			long start=System.currentTimeMillis();
			int n=10000000;
			for (int i=0;i<n;++i){
				HClusterDescriptor desc2=(HClusterDescriptor)avro.hydrate(avro.dehydrate(desc));
			}
			long end=System.currentTimeMillis();
			System.out.println(n*1000.0/(end-start));
		}
	}
}
