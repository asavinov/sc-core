package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;


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
 * This class implements the logic of evaluation for definitions of certain kind, that is, how definitions of certain kind are interpreted to compute this column output.
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
}

/**
 * It is an implementation of evaluator for calc columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorCalc extends ColumnEvaluatorBase implements ColumnEvaluator {
	UDE ude;

	public void evaluate() {
		// Logic of evaluation for calc (maybe use some base method

		// Get list of parameter paths
		
		// Loop over all inputs
		
		// Read all parameters for one input
		// Evaluate expression
		// Write result to the output
	}

	public ColumnEvaluatorCalc(UDE ude) {
		this.ude = ude;
	}
}

class ColumnEvaluatorBase { // Convenience class for implementing common functions
	// Evaluate one expression for one specified table
}


/**
 * User defined expression. It knows how to computing one output value given several input values.
 * Normally it is implemented by a user class programmatically or produced by a translator from some syntactic representation (formula). 
 */
interface UDE {
	//public Table getMainTable(); // Do we need this? Normally it is available in the context
	//public Column getOutputColumn(); // Do we need this? Normally it is available in the context
	public List<List<Column>> getInputPaths();
	public Object evaluate(Object[] params);
}

/**
 * For test purposes to check if it useful and can be used for situations.
 * Use cases:
 * - Reusable and binding of parameters: I want to implement the logic of computation which can be applied to different inputs, that is, one class can be instantiated and used for different input paths and tables.
 * This can be implemented by the necessary binding for each new instance.
 * - Issue: each instance is hard-bound to certain column paths so if the schema changes then this binding can break.
 *   - Solution: flexibility is reached by name-based dependencies (as opposed to reference-based) but then it is necessary to translation after each change of the schema.
 * - Issue: Path names have to be stored somewhere if we want to work at the level of names.
 *   - Solution: using formulas and descriptors.
 *
 */
class UdeExample implements UDE {
	List<List<Column>> inputPaths = new ArrayList<List<Column>>();
	public List<List<Column>> getInputPaths() {
		return this.inputPaths;
	}
	public Object evaluate(Object[] params) {
		return (double)params[0] + 1;
	}
	public UdeExample(List<List<Column>> inputPaths) { // Binding of parameter
		this.inputPaths.addAll(inputPaths);
	}
}



// It is a syntactic or serializable representation of a derived column of calc kind. 
// It knows about syntax convention and assumes this column kind.
// We also can encode the logic of translation to an evaluator object by translating all formulas to user defined expresions.
class ColumnDefinitionCalc {
	String formula;
	
	// It is transformed to an object form as a column evaluator by translating all expressions from this specific syntactic format
	ColumnEvaluatorCalc translate() {
		return null;
	}

	public ColumnDefinitionCalc(String formula) {
		this.formula = formula;
	}
}


