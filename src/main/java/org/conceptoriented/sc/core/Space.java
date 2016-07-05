package org.conceptoriented.sc.core;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.json.JSONObject;

/**
 * Stream space stores the complete data state and is able to consistently update it. 
 */
public class Space {
	
	private final UUID id;
	public UUID getId() {
		return id;
	}

	private String name;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	

	//
	// Environment
	//
	private UdfClassLoader classLoader;
	public UdfClassLoader getClassLoader() {
		return classLoader;
	}
	
	//
	// Tables
	//
	
	private List<Table> tables = new ArrayList<Table>();
	public List<Table> getTables() {
		return tables;
	}
	public Table getTable(String table) {
		for(Table tab : tables) {
			if(tab.getName() == table) return tab;
		}
		return null;
	}
	public Table getTableById(String id) {
        Optional<Table> ret = tables.stream().filter(x -> x.getId().toString().equals(id)).findAny();
        if(ret.isPresent()) {
        	return ret.get();
        }
        else {
    		return null;
        }
	}

	public Table createTable(String name) {
		Table table = new Table(this, name);
		tables.add(table);
		return table;
	}
	public Table createTableFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");

		// Check validity

		boolean isValid = true;
		if(name == null || name.isEmpty()) isValid = false;

		// Create

		if(isValid) {
			return this.createTable(name);
		}
		else {
			return null;
		}
	}
	public void updateTableFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");
		Table table = getTableById(id);

		// Update the properties

		table.setName(name);
	}
	public void deleteTable(String id) {
		Table table = getTableById(id);

		// Remove input columns
		List<Column> inColumns = columns.stream().filter(x -> x.getInput().equals(table)).collect(Collectors.<Column>toList());
		columns.removeAll(inColumns);
		
		// Remove output columns
		List<Column> outColumns = columns.stream().filter(x -> x.getOutput().equals(table)).collect(Collectors.<Column>toList());
		columns.removeAll(outColumns);
		
		// Remove table itself
		tables.remove(table);
	}

	//
	// Columns
	//

	private List<Column> columns = new ArrayList<Column>();
	public List<Column> getColumns() {
		return columns;
	}
	public List<Column> getColumns(String table) {
		List<Column> res = new ArrayList<Column>();
		for(Column col : columns) {
			if(col.getInput().getName() == table) {
				res.add(col);
			}
		}
		return res;
	}
	public Column getColumn(String table, String column) {
		for(Column col : columns) {
			if(col.getName() == column) return col;
		}
		return null;
	}
	public Column getColumnById(String id) {
        Optional<Column> ret = columns.stream().filter(x -> x.getId().toString().equals(id)).findAny();
        if(ret.isPresent()) {
        	return ret.get();
        }
        else {
    		return null;
        }
	}

	public Column createColumn(String name, String input, String output) {
		Column column = new Column(this, name, input, output);
		columns.add(column);
		return column;
	}
	public Column createColumnFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");

		JSONObject input_table = obj.getJSONObject("input");
		String input_id = input_table.getString("id");
		Table input = this.getTableById(input_id);

		JSONObject output_table = obj.getJSONObject("output");
		String output_id = output_table.getString("id");
		Table output = this.getTableById(output_id);

		// Check validity

		boolean isValid = true;
		if(name == null || name.isEmpty()) isValid = false;
		if(input == null) isValid = false;
		if(output == null) isValid = false;

		// Create

		if(isValid) {
			return this.createColumn(name, input.getName(), output.getName());
		}
		else {
			return null;
		}
	}
	public void updateColumnFromJson(String json) {
		JSONObject obj = new JSONObject(json);

		// Extract all necessary parameters
		
		String id = obj.getString("id");
		String name = obj.getString("name");
		Column column = getColumnById(id);

		JSONObject input_table = obj.getJSONObject("input");
		String input_id = input_table.getString("id");
		Table input = this.getTableById(input_id);

		JSONObject output_table = obj.getJSONObject("output");
		String output_id = output_table.getString("id");
		Table output = this.getTableById(output_id);

		// Update the properties

		column.setName(name);
		column.setInput(input);
		column.setOutput(output);
	}
	public void deleteColumn(String id) {
		Column column = getColumnById(id);
		columns.remove(column);
	}

	//
	// Data (state)
	//
	
	public void evaluate() {
		
		//
		// Evaluate the space. Make again consistent (non-dirty).
		// Bring the state back to consistent state by re-computing the values which are known to be dirty.
		//

		// Build a list/graph of columns to be evaluated. Either in the sequence of creation, or using dependencies.
		// Evaluate each column individually from this structure. 
		// Any column has to provide an evaluation function which knows how to compute the output value.
		List<Column> columns = this.columns;

		// For each dirty value, evaluate it again and store the result
		for(Column column : columns) {
			if(column.getEvaluator() == null) continue;
			column.evaluate();
		}
		
		//
		// Update ranges of all tables
		//

		for(Table table : tables) {
			if(table.isPrimitive()) continue;
			table.addNewRange(); // Mark dirty as clean
			table.removeDelRange(); // Really remove old records
		}
		
	}
	
	public String toJson() {
		// Trick to avoid backslashing double quotes: use backticks and then replace it at the end 
		String jid = "`id`: `" + this.getId() + "`";
		String jname = "`name`: `" + this.getName() + "`";
		
		String json = jid + ", " + jname;

		return ("{" + json + "}").replace('`', '"');
	}
	
	@Override
	public String toString() {
		return "[" + name + "]";
	}
	
	public Space(String name) {
		this.id = UUID.randomUUID();
		this.name = name;
		
		// Create class loader
		classLoader = new UdfClassLoader();

		// Create primitive tables
		Table doubleType = createTable("Double");
		Table integerType = createTable("Integer");
		Table stringType = createTable("String");
	}

}


/**
 * Criteria:
 * - Load class dynamically by using the default class loader and the default class path. So the task of the user/system to put all jars to the class path.
 *   - The user jars can be uploaded dynamically to the location known to this default class loader or otherwise define in the class path, e.g., in the app config like plug-in-dir
 *   - It is important that there is one location for all external jars/classes but each space loads its evaluator classes individually and dynamically
 *   - The mechanism has to work when we manually copy/upload a jar with udf to the common system class path (maybe a special location for udfs specified in the configuration)
 * - What about dependencies and resolve?
 * - In future, we assume that columns in different spaces may have the same class name for evaluator 
 * - Also, evaluator classes from different space have to be somehow isolated   
 **/

// http://www.javaworld.com/article/2077260/learn-java/learn-java-the-basics-of-java-class-loaders.html !!!
// https://examples.javacodegeeks.com/core-java/dynamic-class-loading-example/ - loading automatically
class UdfClassLoader extends ClassLoader {
	
	String spaceJarName = "";
	String spaceJarUrl = "";

	@Override
	public Class loadClass(String name) throws ClassNotFoundException {
		return loadClassDefault(name);
	}

	protected Class loadClassDefault(String name) throws ClassNotFoundException {
		Class clazz = null;
		
		// First check if the class is already loaded
	    clazz = findLoadedClass(name);
	    if (clazz != null) return clazz;
		
		// The parent classloader
		ClassLoader defaultLoader = Space.class.getClassLoader(); // Or EvaluatorBase loader

		clazz = defaultLoader.loadClass(name);
		
		return clazz;
	}

	// http://www.javaworld.com/article/2071777/design-patterns/add-dynamic-java-code-to-your-application.html
	protected Class loadClassFromUrl(String name) throws ClassNotFoundException, IllegalAccessException, InstantiationException, MalformedURLException {

		// The dir contains the compiled classes.
		File classesDir = new File("/temp/dynacode_classes/");

		// The parent classloader
		ClassLoader parentLoader = Space.class.getClassLoader();
		//ClassLoader parentLoader = EvaluatorBase.class.getClassLoader();

		// Load class "sample.PostmanImpl" with our own classloader.
		URLClassLoader loader1 = new URLClassLoader(new URL[] { classesDir.toURL() }, parentLoader);
		Class cls1 = loader1.loadClass("sample.PostmanImpl");

		EvaluatorBase eval1 = (EvaluatorBase) cls1.newInstance();

		/*
		 * Invoke on postman1 ... Then PostmanImpl.java is modified and
		 * recompiled.
		 */

		// Reload class "sample.PostmanImpl" with a new classloader.
		URLClassLoader loader2 = new URLClassLoader(new URL[] { classesDir.toURL() }, parentLoader);
		Class cls2 = loader2.loadClass("sample.PostmanImpl");
		EvaluatorBase eval2 = (EvaluatorBase) cls2.newInstance();

		/*
		 * Work with postman2 from now on ... Don't worry about loader1, cls1,
		 * and postman1 they will be garbage collected automatically.
		 */
		
		return null;
	}

	// http://tutorials.jenkov.com/java-reflection/dynamic-class-loading-reloading.html - reading class from file
	protected Class loadClassFromFile(String name) throws ClassNotFoundException {
		if (!"reflection.MyObject".equals(name)) {
			return super.loadClass(name);
		}

		try {
			String url = "file:C:/data/projects/tutorials/web/WEB-INF/" + "classes/reflection/MyObject.class";
			URL myUrl = new URL(url);
			URLConnection connection = myUrl.openConnection();
			InputStream input = connection.getInputStream();
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			int data = input.read();

			while (data != -1) {
				buffer.write(data);
				data = input.read();
			}

			input.close();

			byte[] classData = buffer.toByteArray();
			
			Class clazz = defineClass("reflection.MyObject", classData, 0, classData.length);

			return clazz;

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

    public UdfClassLoader(ClassLoader parent) {
        super(parent);
    }

    public UdfClassLoader() {
		// Use lib path for this space
	}
}
