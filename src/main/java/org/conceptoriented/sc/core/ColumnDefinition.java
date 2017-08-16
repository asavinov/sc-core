package org.conceptoriented.sc.core;

import java.util.List;

/**
 * It is a syntactic representation of a derived column with some specific serialization convention corresponding to certain format, for example, programming language or expression language.
 * This class knows about the syntactic conventions of the language/expression language and can also translate it the corresponding object representation.
 * This objects are supposed to be used to represent formulas edited by the user and stored in columns. The system then translates then into an executable form (evaluators).
 * This class has to directly or indirectly encode all names needed for computing certain column kind like main table name, parameter path names, and maybe additional parameters like whether to append records.
 */
public interface ColumnDefinition {
	public ColumnEvaluator translate(); // Transform syntactic representation to object representation by producing the necessary objects like Udes, tables, columns etc.
	public List<DcError> getErrors();
	// String getTable(); TODO: Do we need main table name?
	// List<String> getDependencies(); // TODO: Do we need a list of all columns used in the definition?
}

/**
 * Representation of a calc column using numeric expression libraries like exp4j.
 * The library to be used is chosen in options.
 */
class ColumnDefinitionCalc implements ColumnDefinition {
	String formula;
	
	@Override
	public ColumnEvaluator translate() {
		// Generate ColumnEvaluatorCalc object by instantiating the necessary Ude, tables, columns and other objects used during evaluation.
		return null;
	}
	@Override
	public List<DcError> getErrors() {
		return null;
	}

	public ColumnDefinitionCalc(String formula) {
		this.formula = formula;
	}
}
