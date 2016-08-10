package org.joolzminer.examples.spark.java.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@SuppressWarnings("serial")
public class MyDoubleAverageUDAF extends UserDefinedAggregateFunction {

	private StructType inputDataType;
	private StructType bufferSchema;
	private DataType returnDataType;
	
	public MyDoubleAverageUDAF() {
		List<StructField> inputFields = new ArrayList<>();
		inputFields.add(DataTypes.createStructField("inputDouble", DataTypes.DoubleType, true));
		inputDataType = DataTypes.createStructType(inputFields);
		
		List<StructField> bufferFields = new ArrayList<>();
		bufferFields.add(DataTypes.createStructField("bufferSum", DataTypes.DoubleType, true));
		bufferFields.add(DataTypes.createStructField("bufferCount", DataTypes.LongType, true));
		bufferSchema = DataTypes.createStructType(bufferFields);
		
		returnDataType = DataTypes.DoubleType;
	}

	@Override
	public StructType inputSchema() {
		return inputDataType;
	}
	
	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}
	
	@Override
	public DataType dataType() {
		return returnDataType;
	}
	@Override
	
	public boolean deterministic() {
		return true;
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		// initial value for the sum is null
		buffer.update(0, null); 
		
		// initial value for the count is 0
		buffer.update(1,  0L);
	}
	
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		// We only update the value when the input value is not null
		if (!input.isNullAt(0)) {
			// if the buffer value (tracking the intermediate values) is still null, we initialize it
			if (buffer.isNullAt(0)) {
				buffer.update(0, input.getDouble(0));
				buffer.update(1, 1L);
			} else {
				// update bufferSum and bufferCount
				Double newValue = input.getDouble(0) + buffer.getDouble(0);
				buffer.update(0, newValue);
				buffer.update(1, buffer.getLong(1) + 1L);
			}
		}
	}
	
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		// buffer1 and buffer2 have the same structure
		// buffer1 is only updated when buffer2 is not null
		if (!buffer2.isNullAt(0)) {
			// if the buffer value (tracking the intermediate values) is still null, we initialize it
			if (buffer1.isNullAt(0)) {				
				buffer1.update(0, buffer2.getDouble(0));
				buffer1.update(1, buffer2.getLong(1));
			} else {
				// update bufferSum and bufferCount
				Double newValue = buffer2.getDouble(0) + buffer1.getDouble(0);
				buffer1.update(0, newValue);
				buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
			}
		}
	}

	@Override
	public Object evaluate(Row buffer) {
		// if the bufferSum is still null, we return null (no result produced)
		if (buffer.isNullAt(0)) {
			return null;
		} else {
			// return the average
			return buffer.getDouble(0) / buffer.getLong(1);
		}
	}
}
