package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.conceptoriented.sc.core.Column;
import org.conceptoriented.sc.core.EvaluatorBase;
import org.conceptoriented.sc.core.Range;

/**
 * A user-defined evaluator class implements a function which returns one value given some input values.
 * A user-defined evaluator knows input columns by name by declaring its dependence on these input columns (and their types).
 * Therefore, each such evaluator class is intended for certain input column names only because these names are encoded into it as dependencies.
 * Thus each class cannot be reused for other columns. It is a drawback because we cannot develop generic functions which could be applied to arbitrary columns.
 * Note that the output column name is not encoded into the class.
 * One possible solution to this problem is to use additional specification along with each user defined evaluator. This specification provides dependencies and essentially binds one instance to specific columns by name.
 * In this case, a user defined evaluator implements a generic function for certain input column types but it can be applied to different columns in a schema and this information is provided in the evaluator descriptor.
 * A descriptor (dependencies) could be hard-coded or provided as a separate file, e.g., json file with the same name as the class.
 * Alternatively, dependencies (descriptor) could be provided programmatically for each column like setDependencies. An evaluator class then implements its functions in terms of abstract column numbers.
 * A user-defined evaluator is developed as name-independent function which can be bound to different columns by name using a separate descriptor provided to the column.
 * The descriptor is used for dependency management (computing the graph) and for providing direct column data to the evaluator by using integer column identifiers.
 * An advantage is that evaluators are then reusable functions, for example, we could develop a generic evaluator for arithmetic operations.
 * 
 * The next step would be to allow for nested evaluators. This can be implemented by explicitly creating columns but this means materialization of intermediate results.
 * But what if an evaluator wants to reuse another evaluator by applying its function to the current inputs?
 * We need to take into account that any including nested evaluators can access also other records in their input columns.
 * Also, they could require some additional input columns the parent evaluator does not have. The dependency manager has to know about such uses.
 * One approach to declare in the descriptor that this (parent) evaluator will consume data from a child evaluator. 
 * And the child evaluator will consume some columns (by name). 
 * If the parent is consuming some value from a child evaluator then it is equivalent to consuming a value from a column.
 * In this case however, the parent cannot move along the evaluator by reading inputs - it can only compute one single value.
 * Alternatively, a parent could change inputs (if they are values) and evaluate the child many times.
 * Note that evaluator can change integer row identifiers and retrieve arbitrary outputs from a column where a column is viewed as a function.
 * Probably, the same could be done with child evaluators: set inputs and compute output (instead of retrieving it).
 * In fact, we have an alternative: either a function gets input values or it gets inputs of functions with the functions themselves (so that the inputs can be varied).
 * So a (child) evaluator gets either input values or functions with current inputs.
 * 
 * - Evaluator does not deal with and does not rely on column names (of course, it can get them but it is illegal)
 * - Evaluator uses column objects as (primitive or material) functions which return output for certain input (maybe we need an interface for that purpose which is similar to generic functions/evaluator concept)
 * - Since evaluator uses functions, it has to be able to get validity ranges for them by retrieving various ranges like new, old, or all (so maybe it could be part of generic function/evaluator interface)
 * - Evaluator can vary inputs of the functions it needs, for example, to compute moving average or to shift them. So it can generate new (local) iterations and loops similar to an external driver like the central evaluator which provides the main loop.  
 * - Generic evaluator/function has two alternative interpretations: computing single output for certain input values provided from outside, and computing a range of outputs for a range of inputs provided from outside. 
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
		range = thisColumn.getInput().getCleanRange();
		// We also can do some analysis by computing constants
	}
	
	@Override
	public void evaluate(long row) {
		// Current value can be used for accumulation
		Double currentValue = (Double)thisColumn.getValue(row);
		
		// Previous values can be used for in-column aggregation by the range has to be checked on validity
		if(row-1 >= range.start) {
			Double previousValue = (Double)thisColumn.getValue(row-1);
		}

		Double valueA = (Double)columnA.getValue(row);
		Double result = null;
		if(valueA != null) {
			result = valueA + 2.0;
		}
		
		thisColumn.setValue(row, result);

		// We can also accumulate/update the current value by using SUM
		///thisColumn.setValue(thisRow, currentValue + result);
	}

}
