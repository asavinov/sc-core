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
	public List<Column> getDependencies();
}

abstract class ColumnEvaluatorBase implements ColumnEvaluator { // Convenience class for implementing common functions

	Column column;
	
	List<DcError> errors = new ArrayList<DcError>();
	@Override
	public List<DcError> getErrors() {
		return this.errors;
	}

	protected void evaluateExpr(UserDefinedExpression expr, List<Column> accuLinkPath) {
		
		errors.clear(); // Clear state

		Table mainTable = accuLinkPath == null ? this.column.getInput() : accuLinkPath.get(0).getInput(); // Loop/scan table

		// ACCU: Currently we do full re-evaluate by resetting the accu column outputs and then making full scan through all existing facts
		// ACCU: The optimal approach is to apply negative accu function for removed elements and then positive accu function for added elements
		Range mainRange = mainTable.getIdRange();

		// Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading values
		List<List<Column>> paramPaths = expr.getResolvedParamPaths();
		Object[] paramValues = new Object[paramPaths.size()]; // Will store values for all params
		Object out; // Current output value
		Object result; // Will be written to output for each input

		for(long i=mainRange.start; i<mainRange.end; i++) {
			// Find group [ACCU-specific]
			Long g = accuLinkPath == null ? i : (Long) accuLinkPath.get(0).getData().getValue(accuLinkPath, i);

			// Read all parameter values
			int paramNo = 0;
			for(List<Column> paramPath : paramPaths) {
				paramValues[paramNo] = paramPath.get(0).data.getValue(paramPath, i);
				paramNo++;
			}
			
			// Read current out value
			out = this.column.getData().getValue(g); // [ACCU-specific] [FIN-specific]

			// Evaluate
			result = expr.evaluate(paramValues, out);
			if(expr.getEvaluateError() != null) {
				errors.add(expr.getEvaluateError());
				return;
			}

			// Update output
			this.column.getData().setValue(g, result);
		}
	}


	protected void evaluateLink(List<Pair<Column,UserDefinedExpression>> exprs) {

		errors.clear(); // Clear state

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

			rhsParamPaths.add( eval.getResolvedParamPaths() );
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
				UserDefinedExpression expr = mmbr.getRight();
				Object result = expr.evaluate(paramValues, null);
				if(expr.getEvaluateError() != null) {
					errors.add(expr.getEvaluateError());
					return;
				}

				rhsResults.set(mmbrNo, result);
				outRecord.set(mmbr.getLeft().getName(), result);
				
				mmbrNo++; // Iterate
			}

			// Find element in the type table which corresponds to these expression results (can be null if not found and not added)
			Object out = typeTable.find(outRecord, true);
			
			// Update output
			this.column.getData().setValue(i, out);
		}

	}

	protected void evaluateExprDefault() {
		Range mainRange = this.column.getData().getIdRange(); // All dirty/new rows
		Object defaultValue = this.column.getDefaultValue();
		for(long i=mainRange.start; i<mainRange.end; i++) {
			this.column.getData().setValue(i, defaultValue);
		}
	}

	protected List<Column> getExpressionDependencies(UserDefinedExpression expr) { // Get parameter paths from expression and extract (unique) columns from them
		List<Column> columns = new ArrayList<Column>();
		
		List<List<Column>> paths = expr.getResolvedParamPaths();
		for(List<Column> path : paths) {
			for(Column col : path) {
				if(!columns.contains(col) && col != this.column) {
					columns.add(col);
				}
			}
		}
		return columns;
	}

	public ColumnEvaluatorBase(Column column) {
		this.column = column;
	}

}

/**
 * It is an implementation of evaluator for calc columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorCalc extends ColumnEvaluatorBase {
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
	public List<Column> getDependencies() {
		List<Column> deps = super.getExpressionDependencies(this.ude);
		return deps;
	}

	public ColumnEvaluatorCalc(Column column, UserDefinedExpression ude) {
		super(column);
		this.ude = ude;
	}
}

/**
 * It is an implementation of evaluator for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorLink extends ColumnEvaluatorBase {
	List<Pair<Column,UserDefinedExpression>> udes = new ArrayList<Pair<Column,UserDefinedExpression>>();

	@Override
	public void evaluate() {
		super.evaluateLink(udes);
	}

	@Override
	public List<Column> getDependencies() {
		List<Column> ret = new ArrayList<Column>();
		if(udes == null) return ret;
		
		for(Pair<Column,UserDefinedExpression> pair : udes) {
			Column lhs = pair.getLeft();
			if(!ret.contains(lhs)) ret.add(lhs);

			List<Column> deps = super.getExpressionDependencies(pair.getRight());
			for(Column col : deps) {
				if(!ret.contains(col)) {
					ret.add(col);
				}
			}
		}
		return ret;
	}

	public ColumnEvaluatorLink(Column column, List<Pair<Column,UserDefinedExpression>> udes) {
		super(column);
		this.udes.addAll(udes);
	}
}

/**
 * It is an implementation of evaluator for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
class ColumnEvaluatorAccu extends ColumnEvaluatorBase {

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
	public List<Column> getDependencies() {
		List<Column> ret = new ArrayList<Column>();
		
		if(this.initExpr != null) {
			for(Column col : super.getExpressionDependencies(this.initExpr)) {
				if(!ret.contains(col)) ret.add(col);
			}
		}
		if(this.accuExpr != null) {
			for(Column col : super.getExpressionDependencies(this.accuExpr)) {
				if(!ret.contains(col)) ret.add(col);
			}
		}
		if(this.finExpr != null) {
			for(Column col : super.getExpressionDependencies(this.finExpr)) {
				if(!ret.contains(col)) ret.add(col);
			}
		}

		for(Column col : this.accuPathColumns) {
			if(!ret.contains(col)) ret.add(col);
		}

		return ret;
	}

	public ColumnEvaluatorAccu(Column column, UserDefinedExpression initExpr, UserDefinedExpression accuExpr, UserDefinedExpression finExpr, List<Column> accuPathColumns) {
		super(column);

		this.initExpr = initExpr;
		this.accuExpr = accuExpr;
		this.finExpr = finExpr;

		this.accuPathColumns = accuPathColumns;
	}
}
