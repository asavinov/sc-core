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

	Column column; // TODO: Need to be initialized by a child class

	protected void evaluateExpr(UserDefinedExpression expr, List<Column> accuLinkPath) {

		Table mainTable = accuLinkPath == null ? this.column.getInput() : accuLinkPath.get(0).getInput(); // Loop/scan table

		// ACCU: Currently we do full re-evaluate by resetting the accu column outputs and then making full scan through all existing facts
		// ACCU: The optimal approach is to apply negative accu function for removed elements and then positive accu function for added elements
		Range mainRange = mainTable.getIdRange();

		// Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading values
		List<List<Column>> paramPaths = this.resolveParameterPaths(expr.getParamPaths(), mainTable);
		Object[] paramValues = new Object[paramPaths.size()]; // Will store values for all params
		Object result; // Will be written to output for each input

		for(long i=mainRange.start; i<mainRange.end; i++) {
			// Find group [ACCU-specific]
			Long g = accuLinkPath == null ? i : (Long) accuLinkPath.get(0).getData().getValue(accuLinkPath, i);

			// Read all parameter values including this column output
			int paramNo = 0;
			for(List<Column> paramPath : paramPaths) {
				if(paramPath.get(0) == this.column) {
					paramValues[paramNo] = this.column.getData().getValue(g); // [ACCU-specific] [FIN-specific]
				}
				else {
					paramValues[paramNo] = paramPath.get(0).data.getValue(paramPath, i);
				}
				paramNo++;
			}

			// Evaluate
			result = expr.evaluate(paramValues);

			// Update output
			this.column.getData().setValue(g, result);
		}
	}


	protected void evaluateLink(List<Pair<Column,UserDefinedExpression>> exprs) {

		Table typeTable = this.column.getOutput();

		Table mainTable = this.column.getInput();
		// Currently we make full scan by re-evaluating all existing input ids
		Range mainRange = this.column.getData().getIdRange();

		// Each item in this lists is for one member expression 
		// We use lists and not map because want to use common index (faster) for access and not key (slower) which is important for frequent accesses in a long loop.
		List< List<List<Column>> > rhsParamPaths = new ArrayList< List<List<Column>> >();
		List< Object[] > rhsParamValues = new ArrayList< Object[] >();
		List< Object > rhsResults = new ArrayList< Object >();
		Record outRecord = new Record(); // All output values for all expressions along with column names (is used by the search)

		// Initialize items of these lists for each member expression
		for(Pair<Column,UserDefinedExpression> mmbr : exprs) {
			UserDefinedExpression eval = mmbr.getRight();
			int paramCount = eval.getParamPaths().size();

			rhsParamPaths.add( this.resolveParameterPaths(eval.getParamPaths(), mainTable) );
			rhsParamValues.add( new Object[ paramCount ] );
			rhsResults.add( null );
		}

		for(long i=mainRange.start; i<mainRange.end; i++) {
			
			outRecord.fields.clear();
			
			// Evaluate ALL child rhs expressions by producing an array of their results 
			int mmbrNo = 0;
			for(Pair<Column,UserDefinedExpression> mmbr : exprs) {

				List<List<Column>> paramPaths = rhsParamPaths.get(mmbrNo);
				Object[] paramValues = rhsParamValues.get(mmbrNo);
				
				// Read all parameter values (assuming that this column output is not used in link columns)
				int paramNo = 0;
				for(List<Column> paramPath : paramPaths) {
					paramValues[paramNo] = paramPath.get(0).data.getValue(paramPath, i);
					paramNo++;
				}

				// Evaluate this member expression
				Object result = mmbr.getRight().evaluate(paramValues);
				rhsResults.set(mmbrNo, result);
				outRecord.set(mmbr.getLeft().getName(), result);
				
				mmbrNo++; // Iterate
			}

			// Find element in the type table which corresponds to these expression results (can be null if not found and not added)
			Object out = typeTable.find(outRecord, true);
			
			// Update output
			this.getData().setValue(i, out);
		}

	}

	protected void evaluateExprDefault() {
		Range mainRange = this.column.getData().getIdRange(); // All dirty/new rows
		Object defaultValue = this.getDefaultValue();
		for(long i=mainRange.start; i<mainRange.end; i++) {
			this.column.getData().setValue(i, defaultValue);
		}
	}

	protected Object getDefaultValue() { // Depends on the column type
		Object defaultValue;
		if(this.column.getOutput().isPrimitive()) {
			defaultValue = 0.0;
		}
		else {
			defaultValue = null;
		}
		return defaultValue;
	}

}

/**
 * It is an implementation of evaluator for calc columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorCalc extends ColumnEvaluatorBase implements ColumnEvaluator {
	UserDefinedExpression ude;

	@Override
	public void evaluate() {
		// Evaluate calc expression
		if(this.ude == null) { // Default
			super.evaluateExprDefault();
		}
		else {
			super.evaluateExpr(ude, null);
		}
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	public ColumnEvaluatorCalc(UserDefinedExpression ude) {
		this.ude = ude;
	}
}

/**
 * It is an implementation of evaluator for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorLink extends ColumnEvaluatorBase implements ColumnEvaluator {
	List<Pair<Column,UserDefinedExpression>> udes;

	@Override
	public void evaluate() {
		super.evaluateLink(udes);
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	public ColumnEvaluatorLink(List<Pair<Column,UserDefinedExpression>> udes) {
		this.udes = udes;
	}
}

/**
 * It is an implementation of evaluator for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorAccu extends ColumnEvaluatorBase implements ColumnEvaluator {
	UserDefinedExpression initExpr;
	UserDefinedExpression accuExpr;
	UserDefinedExpression finExpr;
	
	List<Column> accuPathColumns;

	@Override
	public void evaluate() {
		// Initialization
		if(this.initExpr == null) { // Default
			super.evaluateExprDefault();
		}
		else {
			super.evaluateExpr(this.initExpr, null);
		}
		
		// Accumulation
		super.evaluateExpr(this.accuExpr, this.accuPathColumns);

		// Finalization
		if(this.finExpr == null) { // Default
			; // No finalization if not specified
		}
		else {
			super.evaluateExpr(this.finExpr, null);
		}
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	public ColumnEvaluatorAccu(UserDefinedExpression initExpr, UserDefinedExpression accuExpr, UserDefinedExpression finExpr, List<Column> accuPathColumns) {
		this.initExpr = initExpr;
		this.accuExpr = accuExpr;
		this.finExpr = finExpr;
		
		this.accuPathColumns = accuPathColumns;
	}
}
