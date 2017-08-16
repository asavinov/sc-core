package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;


//ColumnDefinitionCalc - it is a syntactic/serializable form using strings: formula(s), column/table names, Java class names etc. 
// -> translate and build ColumnEvaluatorCalc object by generating the necessary Java objects: UDEs, column/table references etc.

// UDE - it describes the logic of computing single output from multiple inputs and binding of these inputs
// It is either generated from some syntactic form (formula or class name plus bindings in descriptor), or provided by the user as an object with specific column references as binding
// In most cases, it is an intermediate object but the classes could be in a library

// column/table references, Java objects etc. (what is needed to instantiate Evaluator
//-> one or more UDE and other objects (tables, group paths etc.) are used to instantiate an evaluator

//ColumnEvaluatorCalc -> Evaluator knows how to evaluate the whole column using certain logic 
// For this specific logic, it needs the corresponding objects: UDEs, column/tables etc.
// These objects can be instantiated and provided programmatically, or they could be translated from the corresponding definition automatically.
// From the evaluator, we also get dependencies which are needed to determine the sequence of evaluations and propagations.


/**
 * This class is an object representation of a derived column. It implements the logic of computation and knows how to compute all output values for certain column kind.
 * It knows the following aspects: 
 * - Looping: the main (loop) table and other tables needed for evaluation of this column definition
 * - Reading inputs: column paths which are used to compute the output including expression parameters or group path for accumulation
 * - Writing output: how to find the output and write it to this column data
 * This class is unaware of the following aspects:
 * - Serialization and syntax of formulas. This class uses only Java objects
 * - How to parse, bind or build native computing elements (expressions) 
 */
public interface ColumnEvaluator {
	public void evaluate();
	public List<DcError> getErrors();
	// List<Column> getDependencies(); // TODO: Do we need this method for dependency graph?
}

class ColumnEvaluatorBase { // Convenience class for implementing common functions
	// Evaluate one expression for one specified table
}

/**
 * It is an implementation of evaluator for calc columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorCalc extends ColumnEvaluatorBase implements ColumnEvaluator {
	UDE ude;

	@Override
	public void evaluate() {
		// Logic of evaluation for calc (maybe use some base method

		// Get list of parameter paths
		
		// Loop over all inputs
		
		// Read all parameters for one input
		// Evaluate expression
		// Write result to the output
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	public ColumnEvaluatorCalc(UDE ude) {
		this.ude = ude;
	}
}

/**
 * It is an implementation of evaluator for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorLink extends ColumnEvaluatorBase implements ColumnEvaluator {
	List<Pair<Column,UDE>> udes;

	@Override
	public void evaluate() {
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	public ColumnEvaluatorLink(List<Pair<Column,UDE>> udes) {
		this.udes = udes;
	}
}

/**
 * It is an implementation of evaluator for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorAccu extends ColumnEvaluatorBase implements ColumnEvaluator {
	UDE ude;

	@Override
	public void evaluate() {
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	public ColumnEvaluatorAccu(UDE ude) {
		this.ude = ude;
	}
}
