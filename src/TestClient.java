import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

public class TestClient {
    private File outputFile;
    private ArrayList<String> tableNames;

    public static void main(String[] args){
       if(args.length != 1){
           System.err.println("usage: TestClient outputFileName");
           return;
       }

       String outputFileName = args[0];
       TestClient thisTestClient = new TestClient( outputFileName);
        thisTestClient.start();
    }


    private int _noThreads;

    public TestClient(String outputFileName){
        tableNames = new ArrayList<>();
        outputFile = new File(outputFileName);
    }

    public void start(){
        ArrayList<TestResult> results = new ArrayList<>();

        /*
        results.addAll(performTestPair(10,4, "TestTable"));
        results.addAll(performTestPair(20,4, "TestTable"));
        results.addAll(performTestPair(50,4, "TestTable"));
        results.addAll(performTestPair(100,4, "TestTable"));

        results.addAll(performTestPair(10,8, "TestTable"));
        results.addAll(performTestPair(20,8, "TestTable"));
        results.addAll(performTestPair(50,8, "TestTable"));
        results.addAll(performTestPair(100,8, "TestTable"));

        results.addAll(performTestPair(10,16, "TestTable"));
        results.addAll(performTestPair(20,16, "TestTable"));
        results.addAll(performTestPair(50,16, "TestTable"));
        results.addAll(performTestPair(100,16, "TestTable"));
        */

        results.addAll(performTestPair(1000,4,  "TestTable"));
        results.addAll(performTestPair(1000,8,  "TestTable"));
        results.addAll(performTestPair(1000,16, "TestTable"));

        System.out.println("Writing to output file " + outputFile.getName());
        try{
            PrintStream printStream = new PrintStream(outputFile);

            printStream.println("Tree Type, number of samples, sample size, insert rate, query rate");

            for(TestResult thisResult: results){
                printStream.println(thisResult.description + ","
                        + thisResult.noSamples + ","
                        + thisResult.sampleSize + ","
                        + thisResult.insertRate + ","
                        + thisResult.queryRate);
            }

        }catch(Exception e){
            System.err.println(e);
        }

    }

    private Collection<TestResult> performTestPair(int noSamples, int sampleSize, String tableName){
        ArrayList<TestResult> results = new ArrayList<>();

        results.add(performCloudTrieTest(noSamples, sampleSize, tableName));
        results.add(performEC2TrieTest(noSamples, sampleSize, 1));
        return results;
    }

    private static String[] generateSampleData(int noSamples, int sampleSize){
        String [] output = new String[noSamples];
        while(--noSamples >= 0){
            char[] thisString = new char[sampleSize];
            for(int i = 0; i < sampleSize; ++i){
                thisString[i] = (char)('a' + ThreadLocalRandom.current().nextInt(0,26));
            }
            output[noSamples] = new String(thisString);
        }
        return output;
    }
    
    private TestResult performCloudTrieTest(int noSamples, int sampleSize, String tableName){
        System.out.println("-------------------- Testing Cloud Trie ---------------------------------------");
        System.out.println("Sample Count: " + noSamples);
        System.out.println("Sample length: " + sampleSize);
        //System.out.println("Simultanious Client Count: " + noClients);


        CloudPrefixTree thisTrie = null;
    					
    			try {
    				thisTrie = new CloudPrefixTree( !tableNames.contains(tableName), tableName);
    			} catch (Exception e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
                TestResult result = new TestResult();
                result.noSamples = noSamples;
                result.sampleSize = sampleSize;
                result.description = "CloudTrie";

                String[] samples = generateSampleData(noSamples, sampleSize);

                thisTrie.initTree();
    			
    			
    			System.out.println("Testing insert speed...");
    			//  --------------------------- insert -----------------------------------

    			Long startTime = System.currentTimeMillis();
    			for(int i = 0; i < samples.length; ++i) {
    				thisTrie.insert(samples[i]);
    			}
    			Long finishTime = System.currentTimeMillis();

    			double rate = noSamples / ((finishTime - startTime) / 1000.0);
    	    	result.insertRate = rate;
                System.out.println("Cloud Prefix Tree Insert rate: " + rate);

                //  --------------------------- query -----------------------------------
                System.out.println("Testing query speed...");

                startTime = System.currentTimeMillis();
                for(int i = 0; i < samples.length; ++i) {
                    thisTrie.query(samples[i]);
                }
                finishTime = System.currentTimeMillis();
                thisTrie.closeTree();

                rate = noSamples / ((finishTime - startTime) / 1000.0);
                result.queryRate = rate;
                System.out.println("Cloud Prefix Query rate: " + rate);

                if(!tableNames.contains(tableName))
                    tableNames.add(tableName);
                return result;
    }
    
    private TestResult performEC2TrieTest(int noSamples, int sampleSize, int noClients){
        System.out.println("-------------------- Testing Trie on EC2 Instance ---------------------------------------");
    	System.out.println("Sample Count: " + noSamples);
    	System.out.println("Sample length: " + sampleSize);

        TestResult result = new TestResult();
    	result.description = "EC2Trie";
        result.noSamples = noSamples;
        result.sampleSize = sampleSize;


        int clientThreadCount = 0;
        ArrayList<ClientThread> clientThreads = new ArrayList<>();
        
        while(clientThreadCount < noClients){
            try{
                Socket newSocket = new Socket("52.36.221.62", 9898);
                ClientThread thisThread = new ClientThread(newSocket, "Client Thread " + (clientThreadCount++), noSamples, sampleSize);
                thisThread.start();
                clientThreads.add(thisThread);
            }
            catch(Exception ex){
                break;
            }
        }
        for(ClientThread thisClient: clientThreads){
        try{
        	thisClient.join();
        }
        catch(Exception e){}
        }

        for(ClientThread thisClient : clientThreads){
            result.insertRate = thisClient.getInsertRate();
            result.queryRate  = thisClient.getQueryRate();

            System.out.println(thisClient.getName() + ":  insert rate: " + thisClient.getInsertRate() + "  query rate: " + thisClient.getQueryRate());
        }
        return result;
    }


    private class TestResult{
        public double insertRate;
        public double queryRate;
        public int noSamples;
        public int sampleSize;
        public String description;
    }

    private class ClientThread extends Thread {
        private Socket thisSocket;
        private PrintWriter printWriter;
        private BufferedReader bufferedReader;
        private int _noSamples, _sampleSize;
        double insertRate = 0.0, queryRate = 0.0;

        public ClientThread(Socket socket, String name, int noSamples, int sampleSize) throws Exception{
            super(name);
            thisSocket = socket;
            printWriter = new PrintWriter(socket.getOutputStream(), true);
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            _noSamples = noSamples;
            _sampleSize = sampleSize;

        }

        public double getInsertRate(){return insertRate;}
        public double getQueryRate(){return queryRate;}
        
        @Override
        public void run(){
            long startTime = 0, finishTime = 0, timeTaken = 0;
            
            
            String[] insertSamples = TestClient.generateSampleData(_noSamples, _sampleSize);
            String[] querySamples = TestClient.generateSampleData(_noSamples, _sampleSize);

            try {
                // ----------------------   insert  -----------------------------
                startTime = System.currentTimeMillis();
                for(int i = 0; i < _noSamples;++i) {
                    printWriter.println("insert," + insertSamples[i]);
                    printWriter.flush();
                    bufferedReader.readLine();
                }

                finishTime = System.currentTimeMillis();

                timeTaken = finishTime - startTime;
                insertRate = _noSamples / ((timeTaken) / 1000.0);

                // ----------------------   query  -----------------------------
                startTime = System.currentTimeMillis();
                for(int i = 0; i < _noSamples;++i) {
                    printWriter.println("query," + querySamples[i]);
                    printWriter.flush();
                    bufferedReader.readLine();
                }
                finishTime = System.currentTimeMillis();

                timeTaken = finishTime - startTime;
                queryRate = _noSamples / ((timeTaken) / 1000.0);

                // --------------------------------------------------------------
                printWriter.close();
                thisSocket.close();
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }
}