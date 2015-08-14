import java.io.*;
import java.lang.Exception.*;
import java.util.*;

public class MapReduceMessages {

    public static void sendWorkderReady(PrintWriter out) {
	out.println("WORKER_READY");
    }

    public static void sendJobStart(PrintWriter out, String chunkFile) {
	out.println("JOB_START,"+chunkFile);
    }

    public static void sendJobCompleted(PrintWriter out, String chunkFile) {
	out.println("JOB_DONE,"+chunkFile);
    }

    public static void sendJobFailed(PrintWriter out, String chunkFile) {
	out.println("JOB_FAIL,"+chunkFile);
    }

    public static void sendServerDone(PrintWriter out) {
	out.println("SERVER_DONE");
    }

    public static void receiveMessage(BufferedReader in, List<String> messages) throws IOException{
	
	String[] tokens = in.readLine().split(",");
	for (String s : tokens) {
	    messages.add(s);
	}
	
    }

}
