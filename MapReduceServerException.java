import java.lang.Exception;
import java.lang.Throwable;

public class MapReduceServerException extends Exception {
    
    public MapReduceServerException() {
	
    }

    public MapReduceServerException(String name) {
	super(name);
    }

    public MapReduceServerException(Throwable throwable) {
	super(throwable);
    }

    public MapReduceServerException(String name, Throwable throwable) {
	super(name, throwable);
    }

    public static void main(String[] argv) throws MapReduceServerException {
	throw new MapReduceServerException("MapReduce Server Exception.");

    }
}
