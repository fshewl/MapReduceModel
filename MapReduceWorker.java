import java.net.Socket;
import java.util.*;
import java.io.*;
import java.lang.Exception.*;


public class MapReduceWorker {
    String serverHost;
    String serverPort;
    String executablePath;
    String executable;
    String outputPath;
    String logPath;
    File logFile;
    PrintWriter out;
    
    public MapReduceWorker(String[] configs) {
	serverHost = configs[0];
	serverPort = configs[1];
	executablePath = configs[2];
	executable = configs[3];
	outputPath = configs[4];
	logPath = configs[5];

	prepareLogFile();
    }
    
    public MapReduceWorker(String sHost, String sPort, String exePath,
		  String exe, String outPath, String lPath) {
	serverHost = sHost;
	serverPort = sPort;
	executablePath = exePath;
	executable = exe;
	outputPath = outPath;
	logPath = lPath;

	prepareLogFile();
    }

    private void prepareLogFile() {
	String logFilePath = logPath + "/" + System.getenv().get("HOSTNAME") + "-" + executable + ".log";
	logFile = new File(logFilePath);

	try {
	    if (!logFile.exists()) {
		logFile.createNewFile();
	    }

	    out = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
	    out.println("log file is ready");
	} catch (Exception e) {
	}
    }

    private void log(String message) {
	out.println(message);
    }

    public void start() {
	while (true) {
	    String chunkFile = requestChunkFile();
	    
	    if (chunkFile == null) {
		break;
	    }

	    boolean success = processChunkFile(chunkFile);
	    if (success) {
		notifiyServerInputFileWasProcessed(chunkFile);
	    }

	    else {
		notifiyServerInputFileWasFailed(chunkFile);
	    }
	}

	out.close();
    }

    private Socket getServerSocket() throws IOException {

	Socket s = new Socket(serverHost, Integer.parseInt(serverPort));
	return s;
	
    }

    private String requestChunkFile() {
	try {
	    Socket socket = getServerSocket();
	    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
	    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

	    log("Sending WORKER_READY message to the server");
	    MapReduceMessages.sendWorkderReady(out);

	    log("Waiting for response from server...");
	    List<String> msgs = new ArrayList<String>();

	    MapReduceMessages.receiveMessage(in, msgs);

	    if (msgs.get(0).equals("SERVER_DONE")) {
		log("Sever done");
		return null;
	    }

	    return msgs.get(1);

	} catch (Exception e) {
	    return null;
	}
    }

    private String buildCommand(String chunkFile) {
	StringBuilder sb = new StringBuilder();
	String filename = chunkFile.substring(chunkFile.lastIndexOf("/")+1);
	String outputFile = outputPath + "/" + filename + ".out";
	String logFile = logPath + "/" + filename + ".log";
	
	sb.append("java -cp " + executablePath + " " + executable + " ");
	sb.append(chunkFile + " ");
	sb.append(outputFile + " ");
	sb.append(logFile);

	return sb.toString();
	
    }
    
    private boolean processChunkFile(String chunkFile) {

	log("Processing chunk " + chunkFile + "...");
	String command = buildCommand(chunkFile);
	log("Command is: " + command);
	String[] commands = new String[] { "/bin/sh", "-c", command };
	ProcessBuilder probuilder = new ProcessBuilder(commands);
	
	try {
	    Process process = probuilder.start();
	    int status = process.waitFor();
	    if (status == 0) return true;
	    else
		return false;
	    
	} catch (Exception e) {
	    log("Couldn't execute command");
	    return false;
	}
    
    }

    private void notifiyServerInputFileWasFailed(String chunkFile) {
	try {
	    Socket socket = getServerSocket();
	    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
	 
	    log("Sending JOB_FAIL message to the server");
	    MapReduceMessages.sendJobFailed(out, chunkFile);
	 
	} catch (Exception e) {
	    return;
	}
    }

    private void notifiyServerInputFileWasProcessed(String chunkFile) {
	try {
	    Socket socket = getServerSocket();
	    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
	 
	    log("Sending JOB_DONE message to the server");
	    MapReduceMessages.sendJobCompleted(out, chunkFile);
	 
	} catch (Exception e) {
	    return;
	}
    }

    public static void main(String[] argv) {
	MapReduceWorker mrw = new MapReduceWorker(argv);
	mrw.start();
    }
}
