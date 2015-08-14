import java.io.*;

public class WordCountReducer {
    
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
		
		out.println(words[0] + " " + (new Integer(words.length-1)).toString());
		
	    }

	    log.println("exit code : 0");
	    out.close();
	    log.close();

	} catch (Exception e) {
	    System.exit(1);
	    
	} 
	    
    }

}
