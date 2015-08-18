import java.io.*;
import java.lang.*;

public class WordCountReducer {
    
    public static void main(String[] argv) {
	String inputFilePath = argv[0];
	String outputFilePath = argv[1];
	String logFilePath = argv[2];

	File inputFile = new File(inputFilePath);
	File outputFile =  new File(outputFilePath);
	String inputGroup;
	File logFile = new File(logFilePath);
	BufferedReader br;
	PrintWriter out;
	PrintWriter log;
	
	try {

	    // group all the input files
	    if (inputFile.isDirectory()) {
		inputGroup = inputFilePath + "/" + "group.tmp";
		outputFile = new File(outputFilePath + "/" +  inputFile.getName() + ".out");
		PrintWriter tmpOut = new PrintWriter(new BufferedWriter(new FileWriter(inputGroup)));
		for (File inFile : inputFile.listFiles()) {
		    
		    if (inFile.isFile()) {
			BufferedReader tmpBr = new BufferedReader(new InputStreamReader(new FileInputStream(inFile)));
			String l;
			while ((l = tmpBr.readLine()) != null) {
			    tmpOut.println(l);
			}
			
		    }
		}

		tmpOut.close();
		inputFilePath = inputGroup;
	    }

	    // sort the group file
	    String[] commands = new String[]{ "/bin/sh", "-c",
					      "sort " + inputFilePath + " -o " + inputFilePath };
	    
	    ProcessBuilder pb = new ProcessBuilder(commands);
	    Process p = pb.start();
	    int status = p.waitFor();
	    System.out.println(outputFile.getAbsolutePath());
	    br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFilePath)));
	    out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
	    log = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
	    log.println("Processing file: " + inputFilePath);


	    // iterate the sorted file and count words
	    String line;
	    String lastWord = "";
	    int count = 0;

	    while ((line = br.readLine()) != null) {
		String[] words = line.split(" ");
		if (lastWord.equals(words[0])) {
		    count++;
		}
		else {
		    if (lastWord != null)
			out.println(lastWord + " " + (new Integer(count)).toString());
		    count = 1;
		    lastWord = words[0];
		}
		
	    }
	    
	    out.println(lastWord + " " + (new Integer(count)).toString());
	    
	    log.println("exit code : 0");
	    out.close();
	    log.close();

	} catch (Exception e) {
	    System.exit(1);
	    
	} 
	    
    }

}
