import java.io.*;
import java.util.*;

public class GroupByKeyTest {
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

    private ChunkManager chunkManager;
    public GroupByKeyTest(int numOfReducers, String inputPath, String outputPath) {
	chunkManager = new ChunkManager();

	try {
	    chunkManager.reload(inputPath);
	    List<File> tmpOutputFiles = new ArrayList<File>();
	    
	    for (int i = 0; i < numOfReducers; i++) {
		String fileName = (new Integer(i)).toString() + ".tmp";
		File f = new File(outputPath + "/" + fileName);

		if (!f.exists()) {
		    f.createNewFile();
		}
		tmpOutputFiles.add(f);
	    }

	    List<PrintWriter> outputFilesWriters = new ArrayList<PrintWriter>();

	    for (File f : tmpOutputFiles) {
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
		    int hashGroup = Math.abs((key.hashCode())%numOfReducers);
		    outputFilesWriters.get(hashGroup).println(line);
		}
	    }

	    for (PrintWriter pw : outputFilesWriters) {
		pw.close();
	    }

	    for (File f : tmpOutputFiles) {
		String filePath = outputPath + "/" +  f.getName();
			String[] commands = new String[]{ "/bin/sh", "-c",
						   "sort " + filePath + " -o " + filePath };

		ProcessBuilder pb = new ProcessBuilder(commands);
		Process p = pb.start();
		int status = p.waitFor();
	    }


	    chunkManager.reload(outputPath);

	    file = new ArrayList<String>();

	    while (chunkManager.getNextChunkFile(file)) {
		String chunk = file.get(0);
		file.remove(0);

		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(chunk)));
		String groundedFilePath = chunk + ".grounded";
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

    }

    public static void main(String[] argv) {
	GroupByKeyTest gbkt = new GroupByKeyTest(3, "testInput", "testOutput");		
	
    }

}
