import java.io.*;
import java.lang.Exception;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.net.Socket;
import java.net.ServerSocket;
import java.lang.Process;
import java.lang.ProcessBuilder;
import java.util.List;
import java.util.ArrayList;


public class MapReduceServer {

    public MapReduceServer(int port) throws MapReduceServerException {
	startServerThread(port);
    }

    public void start() {
	String[] command = new String[]{"/bin/sh", "-c", "echo hello frank >> ~/Downloads/frank.txt"};
	spawnWorker(command);
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
	    serverThread.submit(new ServerService(serverSocket));
	    
	} catch (Exception e) {
	    throw new MapReduceServerException();
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
	
	public ServerService(ServerSocket s) {
	    serverSocket = s;
	    handlerThreadPool = Executors.newFixedThreadPool(kNumThreads);
	}

	@Override
	public void run() {
	    try {
		while (true) {
		    Socket clientSocket = serverSocket.accept();
		    handlerThreadPool.submit(new Handler(clientSocket));
		}
		
	    } catch (Exception e) {

	    }
	}

	private static class Handler implements Runnable {
	    private Socket socket;
	    public Handler(Socket s) {
		socket = s;
	    }

	    @Override
	    public void run() {

		try {
		    BufferedReader in = new BufferedReader(
			new InputStreamReader(socket.getInputStream()));
		    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

		    out.println("Welcome!");

		} catch (Exception e) {

		} finally {
		    try {
			socket.close();
		    } catch (Exception e) {

		    }
		}

	    }
	}
    }

    private List<String> hosts = new ArrayList<String>() {{
	add("messis-notebook.local");
	}};
	
    private void spawnWorker(String[] command) {
	ExecutorService workers = Executors.newFixedThreadPool(hosts.size());

	for (int i = 0; i < hosts.size(); i++) {
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
	String[] command;
	public Worker(String[] c) {
	    command = c;
	}

	@Override
	public void run() {
	    System.out.println("Spawning worker with command: " + command);
	    ProcessBuilder probuilder = new ProcessBuilder(command);
	    
	    try {
		Process process = probuilder.start();
		int status = process.waitFor();
		System.out.println("Command returned a value " + status);
		
	    } catch (Exception e) {
		System.out.println("Couldn't execute command");
	    }
	    
	}
    }

    public static void main(String[] argv) {
	try {
	    MapReduceServer server = new MapReduceServer(1234);
	    server.start();
	} catch (MapReduceServerException e) {
	    System.out.println("Couldn't start server!");
	}
    }
}
