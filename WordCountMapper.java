import java.io.*;

public class WordCountMapper {
    
    public static void main(String[] argv) {
	String inputFilePath = argv[0];
	String outputFilePath = argv[1];
	String logFilePath = argv[2];

	File inputFile = new File(inputFilePath);
	File outputFile =  new File(outputFilePath);
	File logFile = new File(logFilePath);
	BufferedReader br;
	PrintWriter out;
	PrintWriter log;


	try {
	    br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));

	    out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
	    log = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
	    log.println("Processing file: " + inputFilePath);

	    String line;

	    while ((line = br.readLine()) != null) {
		String[] words = line.split(" ");
		for (String word : words) {
		    word = word.replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "");
		    word = word.toLowerCase();
		    if (word.length() > 0)
			out.println(word + " 1");
		}
	    }

	    log.println("exit code : 0");
	    out.close();
	    log.close();

	} catch (Exception e) {
	    System.exit(1);
	    
	} 
	    
    }

}
