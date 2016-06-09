package org.conceptoriented.sc;

/**
 * It is used to collect outgoing events in row format.
 * These events are then expected to be processed by the corresponding connector. 
 * For example, events could be sent to a message bus or stored in a database.
 */
public class OutputStream {

	// Append an event. It is normally used by DC.
	// Events are appended either by evaluation methods (plugins) or by the central loop dispatcher 
	// which has to know which data has to be converted to output records. 
	// It is more important to be able to generate and send events from custom code because it does analysis.
	// Output events might have an arbitrary format, for example, in the form of notifications or e-mails.  
	
	// Retrieve an event. It is normally used by a specific connector.  

}
