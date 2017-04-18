package org.conceptoriented.sc.core;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import com.google.common.base.CharMatcher;

public class Utils {

    public static boolean isNullOrEmpty(String param) {
        return param == null || param.trim().length() == 0;
    }

    //
    // Name validity
    //

    public static boolean validElementName(String name)
    {
        if (name == null) return false;
        if (Utils.isNullOrEmpty(name)) return false;
        
        char[] validCharacters = {'_', '-'};
        for(char c : validCharacters) {
            name = name.replace(c, ' '); // Replace by space (which is valid)
        }
        
        return StringUtils.isAlphanumericSpace(name);
    }

    //
    // Name equality
    //

    public static boolean sameElementName(String n1, String n2)
    {
        if (n1 == null || n2 == null) return false;
        if (Utils.isNullOrEmpty(n1) || Utils.isNullOrEmpty(n2)) return false;
        return n1.equalsIgnoreCase(n2);
    }

    public static boolean sameSchemaName(String n1, String n2)
    {
        return sameElementName(n1, n2);
    }

    public static boolean sameTableName(String n1, String n2)
    {
        return sameElementName(n1, n2);
    }

    public static boolean sameColumnName(String n1, String n2)
    {
        return sameTableName(n1, n2);
    }


    public static int[] intersect(int[] source, int[] target) { // Restrict the source array by elements from the second array
        int size=0;
        int[] result = new int[Math.min(source.length, target.length)];
        int trgFirst = 0;
        for(int src=0; src<source.length; src++) {

            for(int trg=trgFirst; trg<target.length; trg++) {
                if(source[src] != target[trg]) continue;
                // Found target in source
                result[size] = source[src]; // Store in the result
                size = size + 1;
                trgFirst = trg + 1;
                break;
            }
        }

        return java.util.Arrays.copyOf(result, size);
    }

    public static boolean isInt32(String[] values) {
        if(values == null) return false;

        for (String val : values)
        {
            if(val == null) continue; // assumption: null is supposed to be a valid number
            try {
                int intValue = Integer.parseInt((String) val);
            }
            catch(Exception e) {
                return false;
            }
        }
        return true;
    }

    public static boolean isDouble(String[] values) {
        if(values == null) return false;

        for (String val : values)
        {
            if(val == null) continue; // assumption: null is supposed to be a valid number
            try {
                double doubleValue = Double.parseDouble((String) val);
            }
            catch(Exception e) {
                return false;
            }
        }
        return true;
    }

    public static int toInt32(Object val) {
        if(val == null) {
            return 0;
        }
        else if (val instanceof Integer) {
             return ((Integer) val).intValue();
        }
        else if (val instanceof Double) {
            return ((Double) val).intValue();
        }
        else if (val instanceof Boolean) {
            return ((Boolean) val) == true ? 1 : 0;
        }
        else if (val instanceof String) {
             return Integer.parseInt((String) val);
        }
        else {
             String toString = val.toString();
             if (toString.matches("-?\\d+"))
             {
                  return Integer.parseInt(toString);
             }
             throw new IllegalArgumentException("This Object doesn't represent an int");
        }
    }

    public static double toDouble(Object val) {
        if(val == null) {
            return 0.0;
        }
        else if (val instanceof Integer) {
             return ((Integer) val).doubleValue();
        }
        else if (val instanceof Double) {
            return ((Double) val).doubleValue();
        }
        else if (val instanceof Boolean) {
            return ((Boolean) val) == true ? 1.0 : 0.0;
        }
        else if (val instanceof String) {
             return Double.parseDouble((String) val);
        }
        else {
             String toString = val.toString();
             if (toString.matches("-?\\d+"))
             {
                  return Double.parseDouble(toString);
             }
             throw new IllegalArgumentException("This Object doesn't represent a double");
        }
    }

    public static BigDecimal toDecimal(Object val) {
        if(val == null) {
            return null;
        }
        else if (val instanceof BigDecimal) {
             return (BigDecimal)val;
        }
        else {
            return new BigDecimal(val.toString());
        }
    }

    public static boolean toBoolean(Object val) {
        if(val == null) {
            return false;
        }
        if (val instanceof Integer) {
             return ((Integer) val) == 0 ? false : true;
        }
        else if (val instanceof Double) {
            return ((Double) val) == 0.0 ? false : true;
        }
        else if (val instanceof Boolean) {
            return ((Boolean) val).booleanValue();
        }
        else if (val instanceof String) {
             return ((String) val).equals("0") || ((String) val).equals("false") ? false : true;
        }
        else {
             throw new IllegalArgumentException("This Object doesn't represent a boolean");
        }
    }

    public static Instant toDateTime(Object val) {
        if(val == null) {
            return null;
        }
        else if (val instanceof Instant) {
             return ((Instant) val);
        }
        else {
            return Instant.parse(val.toString());
        }
    }




	public static List<String> recommendTypes(List<String> columnNames, List<Record> records) {
		// Try to find most appropriate data type for each column
		// Data types are recommended by trying to convert their values and choosing conversion that works best
		// There might be other approaches, for example, by using schema mappings but they can be implemented in other methods
		
		List<String> types = new ArrayList<String>();

		// For each column, scan sample values and try to determine the best (working) type
		for(String name : columnNames) {
			// Collect sample values
    		List<String> values = new ArrayList<String>();
            int recordNumber = 0;
    		for(Record rec : records) {
            	values.add(rec.get(name).toString());
            	if(recordNumber > 10) break;
            	recordNumber++;
			}

            // Determine type
    		String typeName;

    		if ( Utils.isInt32( values.toArray(new String[values.size()]) ) ) {
    			//typeName = "Integer";
    			typeName = "Double";
    		}
            else if ( Utils.isDouble( values.toArray(new String[values.size()]) ) ) {
            	typeName = "Double";
            }
            else {
            	typeName = "String";
            }

            types.add(typeName);
		}
		
		return types;
	}
	
	public static List<String> csvLineToList(String line, JSONObject paramsObj) {
		List<String> ret = new ArrayList<String>();
		String[] fields = line.split(",");
		for(int j=0; j<fields.length; j++) {
			String val = fields[j].trim();
			val = CharMatcher.is('\"').trimFrom(val); // Remove quotes if any
			ret.add(val);
		}
		return ret;
	}
}
