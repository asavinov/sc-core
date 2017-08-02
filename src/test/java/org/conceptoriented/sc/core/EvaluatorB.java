package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.conceptoriented.sc.core.Column;
import org.conceptoriented.sc.core.EvaluatorBase;
import org.conceptoriented.sc.core.Range;

/**
 * The evaluator class implements a user-defined function.
 * An instance of this class gets a list of column references which are resolved from the meta-information provided in the descriptor.
 * It uses then these columns to read or write the necessary data using inputs. 
 * The first input id is provided as a parameter of the evaluate method.  
 */
public class EvaluatorB extends EvaluatorBase {

	Range range;

	Column columnA;
	@Override
	public void setColumns(List<Column> columns) {
		columnA = columns.get(0);
	}

	@Override
	public void beginEvaluate() {
		// Prepare variables
		range = thisColumn.getData().getCleanRange();
		// We also can do some analysis by computing constants
	}
	
	@Override
	public void evaluate(long row) {
		// Current value can be used for accumulation
		Double currentValue = (Double)thisColumn.getData().getValue(row);
		
		// Previous values can be used for in-column aggregation by the range has to be checked on validity
		if(row-1 >= range.start) {
			Double previousValue = (Double)thisColumn.getData().getValue(row-1);
		}

		Double valueA = (Double)columnA.getData().getValue(row);
		Double result = null;
		if(valueA != null) {
			result = valueA + 2.0;
		}
		
		thisColumn.getData().setValue(row, result);

		// We can also accumulate/update the current value by using SUM
		///thisColumn.setValue(thisRow, currentValue + result);
	}

}
