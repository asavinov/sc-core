package org.conceptoriented.sc.core;

/**
 * It is used to collect incoming events in row format without processing.
 * Here events get their time stamp but not row id.  
 *
 */
public class InputStream {

	// Append an event. It is normally used by external threads which receive events from external sources.
	
	// Retrieve an event. It is normally used by SC. 
	public Record pop() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isEmpty() {
		return false;
	}

}
