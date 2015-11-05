package org.joolzminer.examples.spark.java;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.sql.Row;

import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

@SuppressWarnings("serial")
public class DataFrameRowPrinter extends AbstractFunction1<Row, BoxedUnit> implements Serializable {
	
	private boolean isFirstTime = true;
	private Map<String, Integer> rowFieldMap = new HashMap<>();
	
	@Override
	public BoxedUnit apply(Row row) {
		if (isFirstTime) {
			String[] fieldNames = row.schema().fieldNames();
			for (String fieldName : fieldNames) {
				rowFieldMap.put(fieldName, row.schema().fieldIndex(fieldName));
			}
		}
		
		StringBuilder sb = new StringBuilder("{");
		for (Entry<String, Integer> rowFieldMapEntry : rowFieldMap.entrySet()) {
			sb.append(rowFieldMapEntry.getKey());
			sb.append("=");
			sb.append(row.get(rowFieldMapEntry.getValue()));
			sb.append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("}");
		
		System.out.println(sb.toString());
		
		return BoxedUnit.UNIT;
	}


}
