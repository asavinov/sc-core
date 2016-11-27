package org.conceptoriented.sc.core;

public class DcError extends Exception {
	public DcErrorCode code;
	public String message;
	public String description;
	
	public String toJson() {
		String jcode = "`code`:" + code.getValue() + "";
		String jmessage = "`message`:`" + message + "`";
		String jdescription = "`description`:`" + description + "`";

		String json = jcode + ", " + jmessage + ", " + jdescription;

		return ("{" + json + "}").replace('`', '"');
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

		return "{\"error\": " + error.toJson() + "}";
	}

	public static String error(DcErrorCode code, String message, String message2) {
		DcError error = new DcError(code, message, message2);
		return "{\"error\": " + error.toJson() + "}";
	}

	@Override
	public String toString() {
		return "[" + this.code + "]: " + this.message;
	}
	
	public DcError(DcErrorCode code, String message, String description) {
		this.code = code;
		this.message = message;
		this.description = description;
	}
}
