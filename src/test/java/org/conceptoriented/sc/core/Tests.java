package org.conceptoriented.sc.core;

import static org.junit.Assert.*;

import java.io.File;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.conceptoriented.sc.core.Column;
import org.conceptoriented.sc.core.EvaluatorBase;
import org.conceptoriented.sc.core.Record;
import org.conceptoriented.sc.core.Schema;
import org.conceptoriented.sc.core.Table;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Tests {

    @BeforeClass
    public static void setUpClass() {
    }

    Schema schema;

    @Before
    public void setUp() {
    }

    @Test
    public void schemaTest()
    {
    	// Create and configure: schema, tables, columns
        schema = new Schema("My Schema");
        Table table = schema.createTable("T");
        table.maxLength = 2;

        // Data column will get its data from pushed records (input column)
        Column columnA = schema.createColumn("T", "A", "Double");

        // Calculated column. It has a user-defined evaluation method (plug-in, mapping, coel etc.)
        // This column can read its own and other column values, and it knows about new/valid/old record ranges 
        // It is expected to write/update its own value
        // If necessary, it can update its type/output by pushing records to its type/output table and using the returned row id for writing into itself
        Column columnB = schema.createColumn("T", "B", "Double");
        String descr = "{ `class`:`org.conceptoriented.sc.core.EvaluatorB`, `dependencies`:[`A`] }";
        columnB.setDescriptor(descr.replace('`', '"'));

        // Add one or more records to the table
        Record record = new Record();

        record.set("A", 5.0);
        table.append(record); // The number of added/dirty records is incremented. Some records can be marked for deletion/old. 
    	
        record.set("A", 10.0);
        table.append(record); // The number of added/dirty records is incremented. Some records can be marked for deletion/old. 

        // Evaluate schema by updating its schema. Mark new records as clean and finally remove records for deletion.
        schema.evaluate();
        
        record.set("A", 20.0);
        table.append(record); 

        // Evaluate schema by updating its schema. Mark new records as clean and finally remove records for deletion.
        schema.evaluate();
        
        // Check the result

    }



    @Test
    public void evalexTest()
    {
    	BigDecimal result = null;
    	
    	com.udojava.evalex.Expression e = new com.udojava.evalex.Expression("1+1/3");
    	e.setPrecision(2);
    	e.setRoundingMode(java.math.RoundingMode.UP);
    	result = e.eval();
    	
    	e = new com.udojava.evalex.Expression("SQRT(a^2 + b^2)");
    	List<String> usedVars = e.getUsedVariables();

    	e.getExpressionTokenizer(); // Does not detect errors
    	
    	e.setVariable("a", "2.4"); // Can work with strings (representing numbers)
    	e.setVariable("b", new BigDecimal(9.253));

    	// Validate
    	try {
        	e.toRPN(); // Generates prefixed representation but can be used to check errors (variables have to be set in order to correctly determine parse errors)
    	}
    	catch(com.udojava.evalex.Expression.ExpressionException ee) {
    		System.out.println(ee);
    	}

    	result = e.eval();
		
		result = new com.udojava.evalex.Expression("random() > 0.5").eval();

		//e = new com.udojava.evalex.Expression("MAX('aaa', 'bbb')");
		// We can define custom functions but they can take only numbers (as constants). 
		// EvalEx does not have string parameters (literals). 
		// It does not recognize quotes. So maybe simply introduce string literals even if they will be converted into numbers, that is, just like string in setVariable.
		// We need to change tokenizer by adding string literals in addition to numbers and then their processing.
		
		e.eval();
    }

    @Test
    public void calcFormulaTest() 
    {
    	Schema schema = createCalcSchema();
        Column columnA = schema.getColumn("T", "A");
        Column columnB = schema.getColumn("T", "B");

        columnB.setCalcFormula("2 * [A] + 1");

        columnB.translate();
        
        // Check correctness of dependencies
        List<Column> depsB = columnB.getDependencies();
        assertTrue( depsB.contains(columnA) );

        columnB.evaluate();

        assertEquals(11.0, (Double)columnB.getData().getValue(0), 0.00001);
        assertEquals(Double.NaN, columnB.getData().getValue(1));
        assertEquals(13.0, (Double)columnB.getData().getValue(2), 0.00001);
    }

    @Test
    public void calcUdeTest() // Test custom class for calc column 
    {
    	Schema schema = createCalcSchema();
        Column columnA = schema.getColumn("T", "A");
        Column columnB = schema.getColumn("T", "B");
        
        // Create ColumnEvaluatorCalc by using a custom Java class as UserDefinedExpression
        List<List<Column>> inputPaths = Arrays.asList( Arrays.asList(columnA) ); // Bind to column objects directly (without names)
        UserDefinedExpression ude = new CustomCalcUde(inputPaths);
        ColumnEvaluatorCalc eval = new ColumnEvaluatorCalc(columnB, ude);
        columnB.setEvaluatorCalc(eval);

        columnB.translate(); // Only to extract dependencies

        // Check correctness of dependencies
        List<Column> depsB = columnB.getDependencies();
        assertTrue( depsB.contains(columnA) );

        columnB.evaluate();

        assertEquals(11.0, (Double)columnB.getData().getValue(0), 0.00001);
        assertEquals(Double.NaN, columnB.getData().getValue(1));
        assertEquals(13.0, (Double)columnB.getData().getValue(2), 0.00001);
    }
    protected Schema createCalcSchema() {
    	schema = new Schema("My Schema");
        Table table = schema.createTable("T");
        Column columnA = schema.createColumn("T", "A", "Double");
        Column columnB = schema.createColumn("T", "B", "Double");
        columnB.setKind(DcColumnKind.CALC);
        
        Record record = new Record();
        record.set("A", 5.0);
        table.append(record);
        record.set("A", null);
        table.append(record);
        record.set("A", 6);
        table.append(record);

        return schema;
    }
    class CustomCalcUde implements UserDefinedExpression {
    	
    	@Override public void setParamPaths(List<QName> paths) {}
    	@Override public List<QName> getParamPaths() { return null; }

    	List<List<Column>> inputPaths = new ArrayList<List<Column>>(); // The expression parameters are bound to these input column paths
    	@Override public List<List<Column>> getResolvedParamPaths() { return inputPaths; }

    	@Override public void translate(String formula) {}
    	@Override public List<DcError> getTranslateErrors() { return null; }

    	@Override public Object evaluate(Object[] params, Object out) { 
    		double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
    		return 2.0 * param + 1.0; // "2 * [A] + 1" 
		}
    	@Override public DcError getEvaluateError() { return null; }
    	
    	public CustomCalcUde(List<List<Column>> inputPaths) {
    		this.inputPaths.addAll(inputPaths);
    	}
    }



    @Test
    public void tupleParserTest() 
    {
    	QNameBuilder nb = new QNameBuilder();
    	QName name = nb.buildQName("[a1 1 1].b222");

    	ExprNode t = new ExprNode(); 

    	t.formula = "{ bbb = v11 + 1 * sin(bla) / bla ; [ccc]=22,2 }";
    	t.name = "aaa";
    	t.parse();

    	t.formula = "{ bbb = v11 + 1 * sin(bla) ; [ccc]= {ddd=22,2} }";
    	t.name = "aaa";
    	t.parse();
    	t = null;
    }
    @Test
    public void linkFormulaTest()
    {
    	Schema schema = createLinkSchema();

        Column c5 = schema.getColumn("T2", "C");

    	c5.setLinkFormula(" { [A] = [A]; [B] = [B] } ");

        c5.translate();

        // Check correctness of dependencies
        List<Column> depsC5 = c5.getDependencies();
        assertTrue( depsC5.contains( schema.getColumn("T2", "A") ) );
        assertTrue( depsC5.contains( schema.getColumn("T2", "B") ) );

        c5.evaluate();

        assertEquals(0L, c5.getData().getValue(0));
        assertEquals(1L, c5.getData().getValue(1));
    }
    
    @Test
    public void linkUdeTest()
    {
    	Schema schema = createLinkSchema();

        Column c5 = schema.getColumn("T2", "C");

        // Define evaluator for this formula: " { [A] = [A]; [B] = [B] } "
        Column c1 = schema.getColumn("T", "A");
        Column c2 = schema.getColumn("T", "B");
        UserDefinedExpression expr1 = new UdeJava("[A]", c1.getInput());
        UserDefinedExpression expr2 = new UdeJava("[B]", c2.getInput());

        List<Pair<Column,UserDefinedExpression>> udes = new ArrayList<Pair<Column,UserDefinedExpression>>();
        udes.add(Pair.of(schema.getColumn("T2", "A"), expr1));
        udes.add(Pair.of(schema.getColumn("T2", "B"), expr2));
        
        ColumnEvaluatorLink eval = new ColumnEvaluatorLink(c5, udes);
        c5.setEvaluatorLink(eval);

        c5.translate();

        // Check correctness of dependencies
        List<Column> depsC5 = c5.getDependencies();
        assertTrue( depsC5.contains( schema.getColumn("T2", "A") ) );
        assertTrue( depsC5.contains( schema.getColumn("T2", "B") ) );

        c5.evaluate();

        assertEquals(0L, c5.getData().getValue(0));
        assertEquals(1L, c5.getData().getValue(1));
    }
    Schema createLinkSchema() {
    	// Create and configure: schema, tables, columns
        schema = new Schema("My Schema");

        //
        // Table 1 (type table)
        //
        Table t1 = schema.createTable("T");

        Column c1 = schema.createColumn("T", "A", "Double");
        Column c2 = schema.createColumn("T", "B", "String");

        // Add one or more records to the table
        t1.append(Record.fromJson("{ A: 5.0, B: \"bbb\" }"));

        //
        // Table 2
        //
        Table t2 = schema.createTable("T2");

        Column c3 = schema.createColumn("T2", "A", "Double");
        Column c4 = schema.createColumn("T2", "B", "String");

        Column c5 = schema.createColumn("T2", "C", "T");
        c5.setKind(DcColumnKind.LINK);

        // Add one or more records to the table
        t2.append(Record.fromJson("{ A: 5.0, B: \"bbb\" }"));
        t2.append(Record.fromJson("{ A: 10.0, B: \"ccc\" }"));

        return schema;
    }



    @Test
    public void accuFormulaTest()
    {
        schema = this.createAccuSchema();

        // Link (group) formula
        Column t2g = schema.getColumn("T2", "G");
        t2g.setLinkFormula(" { [Id] = [Id] } ");
        
        // Accu formula
        Column ta = schema.getColumn("T", "A");
        ta.setInitFormula(""); // Init to default
        ta.setAccuTable("T2");
        ta.setAccuFormula(" [out] + 2.0 * [Id] ");
        ta.setAccuPath("[G]");
        
        //
        // Translate and evaluate
        //
        schema.translate();

        // Check correctness of dependencies
        List<Column> depsTa = ta.getDependencies();
        assertTrue( depsTa.contains( schema.getColumn("T2", "Id") ) ); // Used in formula
        assertTrue( depsTa.contains( schema.getColumn("T2", "G") ) ); // Group path

        schema.evaluate();

        assertEquals(20.0, ta.getData().getValue(0));
        assertEquals(20.0, ta.getData().getValue(1));
        assertEquals(0.0, ta.getData().getValue(2));
    }
    @Test
    public void accuUdeTest()
    {
        schema = this.createAccuSchema();

        // Link (group) formula
        Column t2g = schema.getColumn("T2", "G");
        t2g.setLinkFormula(" { [Id] = [Id] } ");
        
        // Accu evaluator
        Column ta = schema.getColumn("T", "A");
        
        UserDefinedExpression initUde = new UdeJava("0.0", schema.getTable("T"));

        //UserDefinedExpression accuUde = new UdeJava(" [out] + 2.0 * [Id] ", schema.getTable("T2"));;
        List<List<Column>> inputPaths = Arrays.asList( Arrays.asList( schema.getColumn("T2", "Id") ) );
        UserDefinedExpression accuUde = new CustomAccuUde(inputPaths);

        List<Column> accuPathColumns = Arrays.asList(schema.getColumn("T2", "G"));
        
        ColumnEvaluatorAccu eval = new ColumnEvaluatorAccu(ta, initUde, accuUde, null, accuPathColumns);
        ta.setEvaluatorAccu(eval);
        
        //
        // Translate and evaluate
        //
        schema.translate();

        // Check correctness of dependencies
        List<Column> depsTa = ta.getDependencies();
        assertTrue( depsTa.contains( schema.getColumn("T2", "Id") ) ); // Used in formula
        assertTrue( depsTa.contains( schema.getColumn("T2", "G") ) ); // Group path

        schema.evaluate();

        assertEquals(20.0, ta.getData().getValue(0));
        assertEquals(20.0, ta.getData().getValue(1));
        assertEquals(0.0, ta.getData().getValue(2));
    }
    protected Schema createAccuSchema() {
    	
        schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t1 = schema.createTable("T");

        Column tid = schema.createColumn("T", "Id", "Double");

        // Define accu column
        Column ta = schema.createColumn("T", "A", "Double");
        ta.setKind(DcColumnKind.ACCU);

        t1.append(Record.fromJson("{ Id: 5.0 }"));
        t1.append(Record.fromJson("{ Id: 10.0 }"));
        t1.append(Record.fromJson("{ Id: 15.0 }"));

        //
        // Table 2 (fact table)
        //
        Table t2 = schema.createTable("T2");

        Column t2id = schema.createColumn("T2", "Id", "Double");

        // Define group column
        Column t2g = schema.createColumn("T2", "G", "T");
        t2g.setKind(DcColumnKind.LINK);
        
        t2.append(Record.fromJson("{ Id: 5.0 }"));
        t2.append(Record.fromJson("{ Id: 5.0 }"));
        t2.append(Record.fromJson("{ Id: 10.0 }"));
        t2.append(Record.fromJson("{ Id: 20.0 }"));
        
        return schema;
    }
    class CustomAccuUde implements UserDefinedExpression {
    	
    	@Override public void setParamPaths(List<QName> paths) {}
    	@Override public List<QName> getParamPaths() { return null; }

    	List<List<Column>> inputPaths = new ArrayList<List<Column>>(); // The expression parameters are bound to these input column paths
    	@Override public List<List<Column>> getResolvedParamPaths() { return inputPaths; }

    	@Override public void translate(String formula) {}
    	@Override public List<DcError> getTranslateErrors() { return null; }

    	@Override public Object evaluate(Object[] params, Object out) {
    		double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
    		double outVal = out == null ? Double.NaN : ((Number)out).doubleValue();
    		return outVal + 2.0 * param; // " [out] + 2.0 * [Id] " 
		}
    	@Override public DcError getEvaluateError() { return null; }
    	
    	public CustomAccuUde(List<List<Column>> inputPaths) {
    		this.inputPaths.addAll(inputPaths);
    	}
    }



    @Test
    public void dependencyTest()
    {
    	// What do we need?
    	
    	// We want to append records asynchronously
    	// Each append increases the new interval of the table

    	// Each append result in column append and column status has to be updated
    	// User column becomes dirty and propagates deeply (its own dirty status will be resent immediately or by evaluate because it is a user column)
    	// Calc columns cannot be changed from outside but since it is a new record we do not lose anything so we can set the new value. But the column itself is marked dirty because this new value has to be computed from the formula.
    	// Link columns cannot be changed same as calc. And this column is also marked as dirty for future evaluation.
    	// Accu column also is marked dirty.
    	
    	// Evaluation means full evaluation (whole columns). But only dirty. 
    	// Evaluation starts from user columns which are marked clean without evaluation. 
    	// Then we evaluate next level as usual. And evaluated columns are marked clean.
    	// !!! Error columns are skipped and do not participate in evaluation as well as they are always dirty. 
    	// !!! Cycle columns are skipped and their status remains unchanged.
    	
    	// After evaluation, new table interval is merged with clean so we do not have new records anymore.
    	// We could add several records and then evaluate. 
    }

    @Test
    public void csvReadTest()
    {
        schema = new Schema("My Schema");

        /*
        String path = "src/test/resources/example1/Order Details Status.csv"; // Relative to project directory

        Table table = schema.createFromCsv(path, true);
        
        assertEquals("Order Details Status", table.getName());
        assertEquals("Double", schema.getColumn("Order Details Status", "Status ID").getOutput().getName());
        assertEquals("String", schema.getColumn("Order Details Status", "Status Name").getOutput().getName());
        
        assertEquals(3L, schema.getColumn("Order Details Status", "Status ID").getValue(3));
        assertEquals("Shipped", schema.getColumn("Order Details Status", "Status Name").getValue(3));
        */
    }

    @Test
    public void classLoaderTest() 
    {
    	// Create class loader for the schema
    	// UDF class have to be always in nested folders corresponding to their package: either directly in file system or in jar
    	File classDir = new File("C:/TEMP/classes/");
        URL[] classUrl = new URL[1];
		try {
			classUrl[0] = classDir.toURI().toURL();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		URLClassLoader classLoader = new URLClassLoader(classUrl);
		// Now the schema is expected to dynamically load all class definitions for evaluators by using this class loader from this dir

		
		try {
			Class classB = classLoader.loadClass("org.conceptoriented.sc.core.EvaluatorB");
			Object o = classB.newInstance();
			classB = null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    	schema = new Schema("My Schema");
		schema.setClassLoader(classLoader);

        Table table = schema.createTable("T");
        table.maxLength = 2;

        // Data column will get its data from pushed records (input column)
        Column columnA = schema.createColumn("T", "A", "Double");

        Column columnB = schema.createColumn("T", "B", "Double");
        columnB.setDescriptor("{ \"class\":\"org.conceptoriented.sc.core.EvaluatorB\" }");
    }

}
