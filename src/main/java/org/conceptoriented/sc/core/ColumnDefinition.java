package org.conceptoriented.sc.core;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * It is a syntactic representation of a derived column with some specific serialization convention corresponding to certain format, for example, programming language or expression language.
 * This class knows about the syntactic conventions of the language/expression language and can also translate it the corresponding object representation.
 * This objects are supposed to be used to represent formulas edited by the user and stored in columns. The system then translates then into an executable form (evaluators).
 * This class has to directly or indirectly encode all names needed for computing certain column kind like main table name, parameter path names, and maybe additional parameters like whether to append records.
 */
public interface ColumnDefinition {
	// Transform syntactic representation to object representation by producing the necessary internal objects like Udes, tables, columns etc.
	public ColumnEvaluator translate(Column column);
	public List<DcError> getErrors();
	// String getTable(); TODO: Do we need main table name?
	// List<String> getDependencies(); // TODO: Do we need a list of all columns used in the definition?

}

abstract class ColumnDefinitionBase implements ColumnDefinition {

	ColumnDefinitionKind formulaKind;

	List<DcError> errors = new ArrayList<DcError>();
	public boolean hasErrors() {
		if(this.errors.size() == 0) return false;
		else return true;
	}
	@Override
	public List<DcError> getErrors() {
		return this.errors;
	}

	public static String getDescriptorClass(String descriptor) {
		if(descriptor == null) return null;
		JSONObject jdescr = new JSONObject(descriptor);
		return jdescr.getString("class");
	}

	public static List<QName> getDescriptorParameterPaths(String descriptor) {
		List<QName> params = new ArrayList<QName>();
		if(descriptor == null || descriptor.isEmpty()) return params;

		JSONObject jdescr = new JSONObject(descriptor);
		if(jdescr == null || !jdescr.has("parameters")) return params;

		JSONArray jparams = jdescr.getJSONArray("parameters");

		QNameBuilder qnb = new QNameBuilder();
		for (int i = 0 ; i < jparams.length(); i++) {
			QName qn = qnb.buildQName(jparams.getString(i));
			params.add(qn);
		}

		return params;
	}
	
	protected UserDefinedExpression createInstance(String descriptor, ClassLoader classLoader) {

		// Parse the descriptor by getting JavaClass name and list of parameters
		String className = ColumnDefinitionCalc.getDescriptorClass(descriptor);
		List<QName> params = ColumnDefinitionCalc.getDescriptorParameterPaths(descriptor);
		if(className == null | params == null) {
			this.errors.add(new DcError(DcErrorCode.PARSE_ERROR, "Parse error.", "Cannot find class name or parameters: " + descriptor));
			return null;
		}
		
		// Create instance of Ude class
		Class clazz=null;
		try {
			clazz = classLoader.loadClass(className);
	    } catch (ClassNotFoundException e) {
			this.errors.add(new DcError(DcErrorCode.TRANSLATE_ERROR, "Translate error.", "Cannot load class: " + className));
			return null;
	    }
		
		// Create an instance of an expression class
		UserDefinedExpression expr = null;
	    try {
	    	expr = (UserDefinedExpression) clazz.newInstance();
		} catch (InstantiationException e) {
			this.errors.add(new DcError(DcErrorCode.TRANSLATE_ERROR, "Translate error.", "Cannot instantiate class: " + className));
		} catch (IllegalAccessException e) {
			this.errors.add(new DcError(DcErrorCode.TRANSLATE_ERROR, "Translate error.", "Illegal access exception. " + e.getMessage()));
		}

		// Bind its parameters to the paths specified in the descriptor
	    expr.setParamPaths(params);

	    return expr;
	}

}
