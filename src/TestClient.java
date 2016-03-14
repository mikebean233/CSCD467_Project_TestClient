import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TestClient {
   public static void main(String[] args){
        TestClient thisTestClient = new TestClient(1);
        thisTestClient.start();
    }


    private int _noThreads;

    public TestClient(int noThreads){
        if(noThreads < 1)
            throw new IllegalArgumentException();
        _noThreads = noThreads;
    }

    public void start(){
    	// Perform EC2 Trie tests
        performEC2TrieTest(200, 16, 1);
    	
    	// Perform Cloud Trie Test
    	performCloudTrieTest(200,16,1);
    }

    private static String[] generateSampleData(int noSamples, int sampleLength){
        String [] output = new String[noSamples];
        while(--noSamples >= 0){
            char[] thisString = new char[sampleLength];
            for(int i = 0; i < sampleLength; ++i){
                thisString[i] = (char)('a' + ThreadLocalRandom.current().nextInt(0,26));
            }
            output[noSamples] = new String(thisString);
        }
        return output;
    }
    
    private void performCloudTrieTest(int noSamples, int sampleSize, int noClients){
    	System.out.println("Performing tests on Cloud Trie ...");		
    	
    	CloudPrefixTree thisTrie = null;
    					
    			try {
    				thisTrie = new CloudPrefixTree( true, "testTrie2");
    			} catch (Exception e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
                String[] samples = generateSampleData(noSamples, sampleSize);

                thisTrie.initTree();
    			
    			
    			System.out.println("Testing put speed...");
    			//  --------------------------- PUT -----------------------------------

    			Long startTime = System.currentTimeMillis();
    			for(int i = 0; i < samples.length; ++i) {
    				thisTrie.insert(samples[i]);
    			}
    			Long finishTime = System.currentTimeMillis();
    			thisTrie.closeTree();
    	
    			double rate = noSamples / ((finishTime - startTime) / 1000.0);
    			System.out.println("Cloud Prefix Tree rate: " + rate);
    }
    
    private void performEC2TrieTest(int noSamples, int sampleSize, int noClients){
        System.out.println("-------------------- Testing Trie on EC2 Instance ---------------------------------------");
    	System.out.println("Sample Count: " + noSamples);
    	System.out.println("Sample length: " + sampleSize);
    	System.out.println("Simultanious Client Count: " + noClients);
    	System.out.println("Starting Clients...");
    	
    	int clientThreadCount = 0;
        ArrayList<ClientThread> clientThreads = new ArrayList<>();
        
        while(clientThreadCount < noClients){
            try{
                Socket newSocket = new Socket("52.36.221.62", 9898);
                ClientThread thisThread = new ClientThread(newSocket, "Client Thread " + (clientThreadCount++));
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
        	System.out.println(thisClient.getName() + ":  put rate: " + thisClient.getPutRate() + "  query rate: " + thisClient.getQueryRate());
        }
    
    }
    
    private class ClientThread extends Thread {
        private Socket thisSocket;
        private PrintWriter printWriter;
        private BufferedReader bufferedReader;
        
        double putRate = 0.0, queryRate = 0.0;

        public ClientThread(Socket socket, String name) throws Exception{
            super(name);
            thisSocket = socket;
            printWriter = new PrintWriter(socket.getOutputStream(), true);
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }

        public double getPutRate(){return putRate;}
        public double getQueryRate(){return queryRate;}
        
        @Override
        public void run(){
            int noSamples = 1000;
            int sampleLength = 8;
            long startTime = 0, finishTime = 0, timeTaken = 0;
            
            
            String[] putSamples = TestClient.generateSampleData(noSamples, sampleLength);
            String[] querySamples = TestClient.generateSampleData(noSamples, sampleLength);

            try {
                // ----------------------   put  -----------------------------
                startTime = System.currentTimeMillis();
                for(int i = 0; i < noSamples;++i) {
                    printWriter.println("put," + putSamples[i]);
                    printWriter.flush();
                    bufferedReader.readLine();
                }
                finishTime = System.currentTimeMillis();

                timeTaken = finishTime - startTime;
                putRate = noSamples / ((timeTaken) / 1000.0);

                // ----------------------   query  -----------------------------
                startTime = System.currentTimeMillis();
                for(int i = 0; i < noSamples;++i) {
                    printWriter.println("query," + querySamples[i]);
                    printWriter.flush();
                    bufferedReader.readLine();
                }
                finishTime = System.currentTimeMillis();

                timeTaken = finishTime - startTime;
                queryRate = noSamples / ((timeTaken) / 1000.0);

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