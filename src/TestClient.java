import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Random;

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
        Random random = new Random();
        int clientThreadCount = 0;
        while(clientThreadCount < _noThreads){
            try{

                Socket newSocket = new Socket("52.36.221.62", 9898);
                ClientThread thisThread = new ClientThread(newSocket, "Client Thread " + (clientThreadCount++), random);
                thisThread.start();
            }
            catch(Exception ex){
                break;
            }
        }
    }

    private class ClientThread extends Thread {
        private Socket thisSocket;
        private PrintWriter printWriter;
        private BufferedReader bufferedReader;
        private Random _random;
        double rate = 0;

        public ClientThread(Socket socket, String name, Random random) throws Exception{
            super(name);
            thisSocket = socket;
            printWriter = new PrintWriter(socket.getOutputStream(), true);
            bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            _random = random;
        }

        private String[] generateSampleData(int noSamples, int sampleLength){
            String [] output = new String[noSamples];
            while(--noSamples > 0){
                char[] thisString = new char[sampleLength];
                for(int i = 0; i < sampleLength; ++i){
                    thisString[i] = (char)('a' + _random.nextInt(26));
                }
                output[noSamples] = new String(thisString);
            }
            return output;
        }

        @Override
        public void run(){
            int noSamples = 1000;
            int sampleLength = 8;
            long startTime = 0, finishTime = 0, timeTaken = 0;
            String[] putSamples = generateSampleData(noSamples, sampleLength);
            String[] querySamples = generateSampleData(noSamples, sampleLength);

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
                rate = noSamples / ((timeTaken) / 1000.0);
                System.out.println("---------------- put -----------------------");
                System.out.println("samples: " + noSamples);
                System.out.println("rate: " + rate + " samples per second");
                // -------------------------------------------------------------

                // ----------------------   query  -----------------------------
                startTime = System.currentTimeMillis();
                for(int i = 0; i < noSamples;++i) {
                    printWriter.println("query," + querySamples[i]);
                    printWriter.flush();
                    bufferedReader.readLine();
                }
                finishTime = System.currentTimeMillis();

                timeTaken = finishTime - startTime;
                rate = noSamples / ((timeTaken) / 1000.0);
                System.out.println("---------------- query (new Samples) -----------------------");
                System.out.println("samples: " + noSamples);
                System.out.println("rate: " + rate + " samples per second");
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