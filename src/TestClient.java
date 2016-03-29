import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;


public class TestClient {

    public static void main(String[] args) {
        if (args.length != 4) {
            usage();
            System.exit(1);
        }

        try {
            String outputFileName = args[0];
            String EC2Domain = args[1];
            int EC2Port = Integer.parseInt(args[2]);
            String tableName = args[3];

            TestClient thisTestClient = new TestClient(outputFileName, EC2Domain, EC2Port, tableName);
            thisTestClient.start();

        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            System.exit(2);
        }

        System.exit(0);
    }

    private static void usage(){
        System.err.println("usage: TestClient outputFileName EC2Domain EC2Port DynamoDBTableName");
    }

    private File outputFile;
    private String EC2Domain_;
    private String tableName_;
    private int EC2Port_;

    private ArrayList<String> tableNames;

    public TestClient(String outputFileName, String EC2Domain, int EC2Port, String tableName){
        tableNames = new ArrayList<>();
        outputFile = new File(outputFileName);

        EC2Domain_ = EC2Domain;
        tableName_ = tableName;
        EC2Port_ = EC2Port;
    }

    public void start(){
        // Build our TableName parameter for the DynamoDB implementations
        ParameterEntry tableName = new ParameterEntry("TableName", tableName_);

        // Build Our Domain and port parameters for the EC2 implementation
        ParameterEntry EC2Domain = new ParameterEntry("EC2Domain", EC2Domain_);
        ParameterEntry EC2Port   = new ParameterEntry("EC2Port"  , EC2Port_);

        // Build Our Test Cases
        TestCase[] tests = new TestCase[]{
                // ------------------ DynamoDB ---------------------------
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 4,  "Distributed Trie",  tableName),
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 8,  "Distributed Trie",  tableName),
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 16, "Distributed Trie",  tableName),
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 32, "Distributed Trie",  tableName),
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 64, "Distributed Trie",  tableName),
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 4,  "Distributed Trie",  tableName),
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 8,  "Distributed Trie",  tableName),
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 16, "Distributed Trie",  tableName),
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 32, "Distributed Trie",  tableName),
                new TestCase(TreeImplementationType.DynamoDB, TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 64, "Distributed Trie",  tableName),
                // ------------------- Conventional ----------------------
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 4,  "Conventional Trie", EC2Domain, EC2Port),
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 8,  "Conventional Trie", EC2Domain, EC2Port),
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 16, "Conventional Trie", EC2Domain, EC2Port),
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 32, "Conventional Trie", EC2Domain, EC2Port),
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Insert, TreeType.PrefixTree, 50,  1, 64, "Conventional Trie", EC2Domain, EC2Port),
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 4,  "Conventional Trie", EC2Domain, EC2Port),
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 8,  "Conventional Trie", EC2Domain, EC2Port),
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 16, "Conventional Trie", EC2Domain, EC2Port),
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 32, "Conventional Trie", EC2Domain, EC2Port),
                new TestCase(TreeImplementationType.EC2,      TreeOperationType.Query,  TreeType.PrefixTree, 200, 1, 64, "Conventional Trie", EC2Domain, EC2Port)
        };

        // Execute the tests
        for(TestCase thisTest : tests){
            try {
                performTest(thisTest);
            }
            catch(Exception ex){
                ex.printStackTrace(System.err);
            }
        }

        // Write the results to a file in csv format
        System.out.println("Writing to output file " + outputFile.getName());
        try{
            PrintStream printStream = new PrintStream(outputFile);

            printStream.println(TestCase.getCSVHeader());

            // Write the results in csv format
            for(TestCase thisTest: tests){
                printStream.println(thisTest.getAsCSVLine());
            }
        }catch(Exception ex){
            System.err.println(ex.getMessage());
        }

    }

    private void performTest(TestCase testCase) throws Exception{
        if(testCase == null)
            throw new NullPointerException();

        // Prefix Tree
        if(testCase.getTreeType() == TreeType.PrefixTree) {
            switch (testCase.getImplementationType()) {
                case DynamoDB:
                    if (testCase.getNoThreads() > 26)
                        throw new IllegalArgumentException("Error: the number of client threads must not exceed 26 for DynamoDb implementations!");
                    performDistributedTrieTest(testCase);
                    break;
                case EC2:
                    performEC2TrieTest(testCase);
                    break;
            }
        }

        // Binary Search Tree
        if(testCase.getTreeType() == TreeType.BinarySearchTree){
            throw new UnsupportedOperationException("binary search tree tests are not supported in this version");
        }
   }

    private static String[] generateSampleData(String prefix, int noSamples, int sampleSize){
        if(prefix != null && prefix.length() > sampleSize)
            throw new IllegalArgumentException("Error: the prefix length must not exceed the sample size!");

        if(prefix == null)
            prefix = "";
        String [] output = new String[noSamples];
        while(--noSamples >= 0){
            char[] thisString = new char[sampleSize - prefix.length()];
            for(int i = 0; i < (sampleSize - prefix.length()); ++i){
                thisString[i] = (char)('a' + ThreadLocalRandom.current().nextInt(0,26));
            }
            output[noSamples] = new String(prefix + (new String(thisString)));
        }
        return output;
    }

    private void performDistributedTrieTest(TestCase thisTest){
        if(thisTest == null)
            throw new NullPointerException();

        if(!thisTest.getParams().containsKey("TableName"))
            throw new IllegalArgumentException("A table name must be specified to test a DynamoDB tree");

        System.out.println(thisTest);
        String tableName = (String)thisTest.getParams().get("TableName");
        CloudPrefixTree thisTrie = null;

        // Create the DynamoDB table
        try {
            thisTrie = new CloudPrefixTree(!tableNames.contains(tableName), tableName);
            thisTrie.initTree();
            if (!tableNames.contains(tableName))
                tableNames.add(tableName);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            thisTest.makeError("Error: There was a problem creating the CloudPrefix Tree Object; " + e.toString());
            return;
        }

        int noThreads = thisTest.getNoThreads();
        int noSamples = thisTest.getNoSamples();
        int sampleSize = thisTest.getSampleSize();

        // Generate the sample data
        String[][] samples = new String [thisTest.getNoThreads()][];
        char prefix = 'a';
        for(int i = 0; i < thisTest.getNoThreads(); ++i) {
            samples[i] = generateSampleData(prefix + "", noSamples, sampleSize);
            ++prefix;
        }

        // Create the threads
        ArrayList<CloudTrieThread> threads = new ArrayList<>();
        for(int threadNumber = 0; threadNumber < noThreads; ++threadNumber){
            threads.add(new CloudTrieThread("Cloud Tree Thread " + threadNumber, thisTrie, samples[threadNumber], thisTest));
        }

        // Start the threads
        for(CloudTrieThread thisThread : threads){
            thisThread.start();
        }

        // wait for the threads to finish
        for(CloudTrieThread thisThread : threads){
            try{
                thisThread.join();
            }catch(Exception ex){
                System.err.println(ex.getMessage());
            }
        }

    }
    
    private void performEC2TrieTest(TestCase thisTest){
        if(thisTest == null)
            throw new NullPointerException();

        if(!thisTest.getParams().containsKey("EC2Domain"))
            throw new IllegalArgumentException("A domain must be specified to test a EC2 tree");

        if(!thisTest.getParams().containsKey("EC2Port"))
            throw new IllegalArgumentException("A port must be specified to test a EC2 tree");

        System.out.println(thisTest);

        int noClients = thisTest.getNoThreads();
        String domain  = (String)thisTest.getParams().get("EC2Domain");
        int port       = ((Integer)thisTest.getParams().get("EC2Port")).intValue();

        int clientThreadCount = 0;
        ArrayList<ClientThread> clientThreads = new ArrayList<>();
        
        while(clientThreadCount < noClients){
            try{
                Socket newSocket = new Socket(domain, port);
                ClientThread thisThread = new ClientThread(newSocket, "Client Thread " + (clientThreadCount++), thisTest);
                thisThread.start();
                clientThreads.add(thisThread);
            }
            catch(Exception ex){
                System.err.println(ex.getMessage());
                break;
            }
        }
        for(ClientThread thisClient: clientThreads){
            try{thisClient.join();}
            catch(Exception ex){
                System.err.println(ex.getMessage());
            }
        }

    }


    private class CloudTrieThread extends Thread{
        CloudPrefixTree _thisTrie;
        String[] _samples;
        TestCase _testCase;

        public CloudTrieThread(String threadName, CloudPrefixTree thisTrie, String[] samples, TestCase thisTest){
            super(threadName);
            if(thisTrie == null || samples == null || samples.length == 0)
                throw new IllegalArgumentException();

            _thisTrie = thisTrie;
            _samples = samples;
            _testCase = thisTest;
        }

        @Override
        public void run(){
            long startTime = 0, endTime = 0;
            switch(_testCase.getOperationType()){
                case Insert:
                    startTime = System.currentTimeMillis();
                    for(int i = 0; i < _samples.length; ++i){
                        _thisTrie.insert(_samples[i]);
                    }
                    endTime = System.currentTimeMillis();
                    break;
                case Query:
                    startTime = System.currentTimeMillis();
                    for(int i = 0; i < _samples.length; ++i){
                        _thisTrie.query(_samples[i]);
                    }
                    endTime = System.currentTimeMillis();
                    break;
                case Delete:
                    startTime = System.currentTimeMillis();
                    for(int i = 0; i < _samples.length; ++i){
                        _thisTrie.delete(_samples[i]);
                    }
                    endTime = System.currentTimeMillis();
                    break;
            }
            _thisTrie.closeTree();

            long timeElapsed = endTime - startTime;
            double rate =  (double)timeElapsed / (double) _samples.length;
            _testCase.setResult(rate);
        }
    }


    private class ClientThread extends Thread {
        private Socket thisSocket;
        private PrintWriter printWriter;
        private BufferedReader bufferedReader;
        private int _noSamples, _sampleSize;
        private TestCase _testCase;

        public ClientThread(Socket socket, String name, TestCase thisTest) throws Exception{
            super(name);

            if(thisTest == null)
                throw new NullPointerException();

            // Get parameters from TestCase object
            _noSamples  = thisTest.getNoSamples();
            _sampleSize = thisTest.getSampleSize();
            _testCase = thisTest;
            thisSocket = socket;
            printWriter = new PrintWriter(socket.getOutputStream(), true);
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }

        @Override
        public void run(){
            long startTime = 0, finishTime = 0, timeTaken = 0;
            
            String[] samples = TestClient.generateSampleData("",_noSamples, _sampleSize);

            try {
                if(_testCase.getOperationType() == TreeOperationType.Delete){
                    _testCase.makeError(" Error: Delete is not supported for EC2 yet...");
                    return;
                }

                String procedureName = _testCase.getOperationType().toString();

                // Perform the actual test
                startTime = System.currentTimeMillis();
                for(int i = 0; i < _noSamples; ++i) {
                    printWriter.println(procedureName +  "," + samples[i]);
                    printWriter.flush();
                    bufferedReader.readLine();
                }
                finishTime = System.currentTimeMillis();

                printWriter.close();
                thisSocket.close();

                timeTaken = finishTime - startTime;
                double rate = (double)timeTaken / (double)_noSamples;

                _testCase.setResult(rate);
            }
            catch(Exception ex){
                System.err.println(ex.getMessage());
            }
        }
    }

}