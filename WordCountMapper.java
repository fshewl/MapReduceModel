import java.io.*;

public class WordCountMapper {
    
    public static void main(String[] argv) {
	String inputFilePath = argv[0];
	String outputDirPath = argv[1];
	String logFilePath = argv[2];
	int numOfSplit = Integer.parseInt(argv[3]);

	File inputFile = new File(inputFilePath);
	File logFile = new File(logFilePath);
	BufferedReader br;
	PrintWriter[] out;
	PrintWriter log;

	try {
	    br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
	    out = new PrintWriter[numOfSplit];

	    String inputFileName = inputFilePath.substring(inputFilePath.lastIndexOf("/")+1);

	    // create output files
	    for (int i = 0; i < numOfSplit; i++) {
		String dir = outputDirPath + "/" + i + "/";
		System.out.println(dir);
		File dirFile = new File(dir);
		if (!dirFile.exists()) {
		    dirFile.mkdirs();
		}
		String fileName = dir + inputFileName + ".tmp";
		out[i] = new PrintWriter(new BufferedWriter(new FileWriter(fileName)));
	    }

	    log = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
	    log.println("Processing file: " + inputFilePath);

	    String line;

	    while ((line = br.readLine()) != null) {
		String[] words = line.split(" ");
		for (String word : words) {
		    word = word.replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "");
		    word = word.toLowerCase();

		    // map and split
		    if (word.length() > 0) {
			int fileHash = Math.abs(word.hashCode())%numOfSplit;
			out[fileHash].println(word + " 1");
		    }
		}
	    }

	    log.println("exit code : 0");
	    
	    for (int i = 0; i< numOfSplit; i++)
		out[i].close();
	    
	    log.close();

	} catch (Exception e) {
	    System.exit(1);
	    
	} 
	    
    }

}
