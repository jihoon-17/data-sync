package cn.com.xx.common.exception;


import java.io.PrintWriter;
import java.io.StringWriter;

public class DataException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private ErrorCode errorCode;

    public DataException(ErrorCode errorCode, String errorMessage) {
        super(errorCode.toString() + " - " + errorMessage);
        this.errorCode = errorCode;
    }

    private DataException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static DataException asDataXException(ErrorCode errorCode, String message) {
        return new DataException(errorCode, message);
    }

    public static DataException asDataXException(ErrorCode errorCode, String message, Throwable cause) {
        if (cause instanceof DataException) {
            return (DataException) cause;
        }
        return new DataException(errorCode, message, cause);
    }

    public static DataException asDataXException(ErrorCode errorCode, Throwable cause) {
        if (cause instanceof DataException) {
            return (DataException) cause;
        }
        return new DataException(errorCode, getMessage(cause), cause);
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    private static String getMessage(Object obj) {
        if (obj == null) {
            return "";
        }

        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
            // return ((Throwable) obj).getMessage();
        } else {
            return obj.toString();
        }
    }
}
