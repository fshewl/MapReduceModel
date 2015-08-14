import java.io.*;
import java.lang.Exception;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.lang.Process;
import java.lang.ProcessBuilder;
import java.util.List;
import java.util.ArrayList;
import java.util.*;



public class MapReduceServer {
    private List<String> hosts = new ArrayList<String>() {{
	add("Wenlei-Frank.local");
	}};

    private HashMap<String, String> configs;
    private ChunkManager chunkManager;
    private String pwd;
    private String user;
    private String host;
    private int port;
    private String mapper;
    private String reducer;
    private int numMappers;
    private int numReducers;
    private String inputPath;
    private String executablePath;
    private String mapOutputPath;
    private String reduceInputPath;
    private String outputPath;
    private String logPath;
    private HashMap<String, String> hostMap;
    
    public MapReduceServer(String configFilePath, int p) throws MapReduceServerException {
	hostMap = new HashMap<String, String>();
	buildHostMap();
	
	environmentConfiguration(configFilePath);
	port = p;
	
	chunkManager = new ChunkManager();
	startServerThread(port);
    }

    public void run() {
	chunkManager.reload(inputPath);
	spawnMapper();

	groupByKey();
	
	chunkManager.reload(reduceInputPath);
	spawnReducer();
	
    }
    
    private void buildHostMap() {
	InetAddress address;

	try {
	    for (String host : hosts) {
		address = InetAddress.getByName(host);
		hostMap.put(address.getHostAddress(), host);
	    }
	} catch (Exception e) {

	}
    }

    private void environmentConfiguration(String configFilePath) throws MapReduceServerException {
	configs = new HashMap<String, String>();
	getSystemEnv();
	parseConfigFile(configFilePath);
	setEnvVariables();
    }

    private void setEnvVariables() throws MapReduceServerException {
	for (String key : configs.keySet()) {

	    if (key.equals("PWD")) {
		pwd = configs.get(key);
	    }

	    else if (key.equals("USER")) {
		user = configs.get(key);
	    }

	    else if (key.equals("HOST")) {
		host = configs.get(key);
	    }

	    else if (key.equals("mapper")) {
		mapper = configs.get(key);
	    }
	    
	    else if (key.equals("reducer")) {
		reducer = configs.get(key);
	    }

	    else if (key.equals("num-mappers")) {
		numMappers = Integer.parseInt(configs.get(key));
	    }
	    
	    else if (key.equals("num-reducers")) {
		numReducers = Integer.parseInt(configs.get(key));
	    }

	    else if (key.equals("input-path")) {
		inputPath = configs.get(key);
		File dir = new File(inputPath);

		if (!dir.exists() || !dir.isDirectory()) {
		    throw new MapReduceServerException("Input Path not found!");
		}
	    }

	    else if (key.equals("executable-path")) {
		executablePath = configs.get("PWD")+ "/" + configs.get(key);
		File dir = new File(executablePath);
		
		if (!dir.exists()) {
		    dir.mkdirs();
		}
	    }

	    else if (key.equals("map-output-path")) {
		mapOutputPath = configs.get("PWD") + "/" + configs.get(key);

		File dir = new File(mapOutputPath);
		
		if (!dir.exists()) {
		    dir.mkdirs();
		}
	    }

	    else if (key.equals("reduce-input-path")) {
		reduceInputPath = configs.get("PWD") + "/" + configs.get(key);

		File dir = new File(reduceInputPath);
		
		if (!dir.exists()) {
		    dir.mkdirs();
		}
	    }

	    else if (key.equals("output-path")) {
		outputPath = configs.get("PWD") + "/" + configs.get(key);

		File dir = new File(outputPath);
		
		if (!dir.exists()) {
		    dir.mkdirs();
		}
	    }

	    else if (key.equals("log-path")) {
		logPath = configs.get("PWD") + "/" + configs.get(key);

		File dir = new File(logPath);
		
		if (!dir.exists()) {
		    dir.mkdirs();
		}
	    }

	}
    }
    
    private static final String[] envNames = { "PWD", "USER", "HOST" };
    private void getSystemEnv() throws MapReduceServerException {
	Map<String, String> envMap = System.getenv();
	for (String env : envNames) {
	    if (envMap.containsKey(env)) {
		configs.put(env, envMap.get(env));
	    }

	    else {
		throw new MapReduceServerException("System environment not found!");
	    }
	}
    }

    private void parseConfigFile(String configFilePath) throws MapReduceServerException {
	File configFile = new File(configFilePath);

	if (!configFile.exists()) {
	    throw new MapReduceServerException("Configuration file " + configFilePath + " not found!");
	}

	

	try {
	    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(configFile)));
	    String line;
	    while ((line = in.readLine()) != null) {
		String[] tokens = line.split("=");
		configs.put(tokens[0], tokens[1]);
	    }
	} catch (Exception e) {
	    throw new MapReduceServerException("Configuration file failed!");
	}

    }
    


    /**
     * Method: startServerThread
     * ------------------------------------
     * starts server thread
     */
    private ServerSocket serverSocket;
    private ExecutorService serverThread;
    private void startServerThread(int port) throws MapReduceServerException {
	try {
	    serverSocket = new ServerSocket(port);
	    serverThread = Executors.newSingleThreadExecutor();
	    serverThread.submit(new ServerService(serverSocket, chunkManager, hostMap));
	    
	} catch (Exception e) {
	    throw new MapReduceServerException("Server thread couldn't start!");
	}

    }
    
    /**
     * Class: ServerService
     * ------------------------------------
     * a runnable subclass
     * it accepts incoming connection and invoke a handler
     */
    private static class ServerService implements Runnable {
	private ServerSocket serverSocket;
	private ExecutorService handlerThreadPool;
	static final int kNumThreads = 10;
	private ChunkManager chunkManager;
	private HashMap<String, String> hostMap;
	public ServerService(ServerSocket s, ChunkManager cm, HashMap<String, String> hm) {
	    serverSocket = s;
	    chunkManager = cm;
	    hostMap = hm;
	    handlerThreadPool = Executors.newFixedThreadPool(kNumThreads);
	}

	@Override
	public void run() {
	    try {
		while (true) {
		    Socket clientSocket = serverSocket.accept();
		    
		    System.out.println("Received a connection request from " +
				       hostMap.get(clientSocket.getInetAddress().toString().substring(1)));
		    
		    handlerThreadPool.submit(new Handler(clientSocket, chunkManager, hostMap));
		}
		
	    } catch (Exception e) {

	    }
	}

	private static class Handler implements Runnable {
	    private Socket socket;
	    private String socketIP;
	    private String hostName;
	    private ChunkManager chunkManager;
	    private HashMap<String, String> hostMap;
	    public Handler(Socket s, ChunkManager cm, HashMap<String, String> hm) {
		hostMap = hm;
		socket = s;
		socketIP = socket.getInetAddress().toString().substring(1);
		hostName = hostMap.get(socketIP);
		System.out.println("Handling request from " + hostName);
		chunkManager = cm;
	    }

	    @Override
	    public void run() {

		try {
		    BufferedReader in = new BufferedReader(
			new InputStreamReader(socket.getInputStream()));
		    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

		    List<String> messages = new ArrayList<String>();
		    MapReduceMessages.receiveMessage(in, messages);

		    String messageType = messages.get(0);

		    if (messageType.equals("WORKER_READY")) {
			List<String> chunkFile = new ArrayList<String>();

			if (chunkManager.getNextChunkFile(chunkFile)) {
			    System.out.println("Instructing worker at " + hostName +
					       " to process " + chunkFile);
			    MapReduceMessages.sendJobStart(out, chunkFile.get(0));
			}

			else {
			    System.out.println("Informing worker at " + hostName +
				" that all chunks have benn processed");

			    MapReduceMessages.sendServerDone(out);
			}
		    }

		    else if (messageType.equals("JOB_DONE")) {
			chunkManager.markChunkAsProcessed(hostName, messages.get(1));
		    }

		    else if (messageType.equals("JOB_FAIL")) {
			chunkManager.rescheduleChunk(hostName, messages.get(1));
		    }

		    else {
			System.out.println("Ignoring unrecognized message tyep " + messageType);
		    }

		} catch (Exception e) {
		    System.out.println("Conversation with " + hostName +
				       " has exception.");

		} finally {
		    try {
			socket.close();
		    } catch (Exception e) {

		    }
		}
		System.out.println("Conversation with " + hostName
				   + " is done.");
	    }
	}
    }
    
    private static String workerExecutable = "MapReduceWorker";
    private String buildCommand(String workerHost, String exePath, String exe, String outPath, String logPath) {
	StringBuilder command = new StringBuilder();

	command.append("ssh " + user + "@" + workerHost + " ");
	command.append(" 'java " + "-cp " + pwd + " " + workerExecutable + " ");
	command.append(host + " ");
	command.append(port + " ");
	command.append(exePath + " ");
	command.append(exe + " ");
	command.append(outPath + " ");
	command.append(logPath + "'");

	return command.toString();
    }
    

    private void spawnMapper() {
	ExecutorService workers = Executors.newFixedThreadPool(hosts.size());

	for (int i = 0; i < hosts.size(); i++) {
	    String command = buildCommand(hosts.get(i), executablePath, mapper, mapOutputPath, logPath);
	    workers.submit(new Worker(command));
	}

	workers.shutdown();

	try {
	    boolean isShutdown = workers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	    if (isShutdown) {
		System.out.println("All workers finished!");
	    }
		
	} catch (Exception e) {

	}
    }

    private void spawnReducer() {
	ExecutorService workers = Executors.newFixedThreadPool(hosts.size());

	for (int i = 0; i < hosts.size(); i++) {
	    String command = buildCommand(hosts.get(i), executablePath, reducer, outputPath, logPath);
	    System.out.println(command);
	    workers.submit(new Worker(command));
	}

	workers.shutdown();

	try {
	    boolean isShutdown = workers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	    if (isShutdown) {
		System.out.println("All workers finished!");
	    }
		
	} catch (Exception e) {

	}
    }



    private static class Worker implements Runnable {
	String command;
	public Worker(String c) {
	    command = c;
	}

	@Override
	public void run() {
	    System.out.println("Spawning worker with command: " + command);
	    String[] commands = new String[] { "/bin/sh", "-c", command };
	    ProcessBuilder probuilder = new ProcessBuilder(commands);
	    
	    try {
		Process process = probuilder.start();
		int status = process.waitFor();
		System.out.println("Command returned a value " + status);
		
	    } catch (Exception e) {
		System.out.println("Couldn't execute command");
	    }
	    
	}
    }

    private static class ChunkManager {

	File dir;
	Queue<String> unprocessedChunks;
	int size;
	public ChunkManager() {
	    unprocessedChunks = new LinkedList<String>();
	    size = 0;
	}

	public ChunkManager(String path) {
	    reload(path);
	}
	
	public synchronized void reload(String path) {
	    dir = new File(path);
	    unprocessedChunks = new LinkedList<String>();
	    size = 0;

	    for (File f : dir.listFiles()) {
		if (f.isFile()) {
		    unprocessedChunks.add(path + "/" + f.getName());
		    size++;
		}
	    }
	}
	
	public synchronized boolean getNextChunkFile(List<String> chunkFile) {
	    if (size > 0) {
		size--;
		chunkFile.add(unprocessedChunks.poll());
		return true;
	    }

	    else {
		return false;
	    }
	}

	public synchronized void markChunkAsProcessed(String IP, String chunkFile) {
	    System.out.println(chunkFile + " has been processed by " + IP);
	}

	public synchronized void rescheduleChunk(String IP, String chunkFile) {
	    System.out.println(chunkFile + " has not been processed by " + IP
			       + " successfully, rescheduled.");

	    unprocessedChunks.add(chunkFile);
	}
    }

    private void groupByKey() {
	System.out.println("==================");
	System.out.println("Grouping by key...");
	
	ChunkManager chunkManager = new ChunkManager();
	chunkManager.reload(mapOutputPath);

	List<File> outputFiles = new ArrayList<File>();

	try {

	    for (int i = 0; i < numReducers; i++) {
		String fileName = (new Integer(i)).toString() + ".tmp";
		File f = new File(reduceInputPath + "/" + fileName);

		if (!f.exists()) {
		    f.createNewFile();
		}
		outputFiles.add(f);
	    }

	    List<PrintWriter> outputFilesWriters = new ArrayList<PrintWriter>();

	    for (File f : outputFiles) {
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(f, true)));
		outputFilesWriters.add(pw);
	    }

	    List<String> file = new ArrayList<String>();

	    while (chunkManager.getNextChunkFile(file)) {
		String chunk = file.get(0);
		file.remove(0);

		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(chunk)));
		String line;

		while ((line = br.readLine()) != null) {
		    String key = line.split(" ")[0];
		    int hashGroup = Math.abs((key.hashCode()) % numReducers);
		    outputFilesWriters.get(hashGroup).println(line);
		}
	    }

	    for (PrintWriter pw : outputFilesWriters) {
		pw.close();
	    }

	    for (File f : outputFiles) {
		String filePath = reduceInputPath + "/" +  f.getName();
		String[] commands = new String[]{ "/bin/sh", "-c",
						   "sort " + filePath + " -o " + filePath };

		ProcessBuilder pb = new ProcessBuilder(commands);
		Process p = pb.start();
		int status = p.waitFor();
	    }

	    chunkManager.reload(reduceInputPath);
	    file = new ArrayList<String>();

	    while (chunkManager.getNextChunkFile(file)) {
		String chunk = file.get(0);
		file.remove(0);

		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(chunk)));
		String groundedFilePath = chunk + ".grounded";
		System.out.println(groundedFilePath);
		File groundedFile = new File(groundedFilePath);
		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(groundedFile, true)));
		
		String line;
		String lastKey = null;
		StringBuilder lineBuilder = new StringBuilder();

		while ((line = br.readLine()) != null) {
		    String key = line.split(" ")[0];
		    if (!key.equals(lastKey)) {
			if (lineBuilder.length() > 0) {
			    pw.println(lineBuilder.toString());
			}

			lineBuilder = new StringBuilder();
			lineBuilder.append(key + " 1");
			lastKey = key;
		    }

		    else {
			lineBuilder.append(" 1");
		    }
		}

		pw.close();
		(new File(chunk)).delete();
	    }
	    
	    
	} catch (Exception e) {
	    System.out.println(e.getMessage());
	}
	
	System.out.println("Done");
	System.out.println("====================");
    }

    public static void main(String[] argv) throws Exception {
	try {
	    MapReduceServer server = new MapReduceServer("mr.cfg", 1234);
	    server.run();
	} catch (MapReduceServerException e) {
	    System.out.println("Couldn't start server!");
	    System.out.println("ERROR: " + e.getMessage());
	}
    }
}
