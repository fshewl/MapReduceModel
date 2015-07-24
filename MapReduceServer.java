import java.io.*;
import java.lang.Exception;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.net.Socket;
import java.net.ServerSocket;


public class MapReduceServer {

    public MapReduceServer(int port) throws MapReduceServerException {
	startServerThread(port);
    }

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

    public static void main(String[] argv) {
	try {
	    MapReduceServer server = new MapReduceServer(1234);
	} catch (MapReduceServerException e) {
	    System.out.println("Couldn't start server!");
	}
    }
}
