package org.conceptoriented.sc.core;

public class DcError extends Exception {
	public DcErrorCode code;
	public String message;
	public String message2;
	
	public String toJson() {
		String jcode = "`code`:" + code.getValue() + "";
		String jmessage = "`message`:`" + message + "`";
		String jmessage2 = "`message2`:`" + message2 + "`";
		String json = "{`error`: {" + jcode + ", " + jmessage + ", " + jmessage2 + "}}";
		return json.replace('`', '"');
	}

	public static String error(DcErrorCode code, String message2) {
		String message = "";
		switch(code) {

			case NOT_FOUND_IDENTITY:
				message = "Identity not found. Session expired.";
				break;
			case GET_ELEMENT:
				message = "Error getting an element.";
				break;
			case CREATE_ELEMENT:
				message = "Error creating an element.";
				break;
			case UPATE_ELEMENT:
				message = "Error updating an element.";
				break;
			case DELETE_ELEMENT:
				message = "Error deleting an element.";
				break;
			default:
				message = "Unknown error";
				break;
		}
		
		DcError error = new DcError(code, message, message2);

		return error.toJson();
	}

	public static String error(DcErrorCode code, String message, String message2) {
		return (new DcError(code, message, message2)).toJson();
	}

	public DcError(DcErrorCode code, String message, String message2) {
		this.code = code;
		this.message = message;
		this.message2 = message2;
	}
}
