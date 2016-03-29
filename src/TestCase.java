import java.util.HashMap;
import java.util.function.BiConsumer;

public class TestCase{
    private TreeImplementationType  implementationType_;
    private TreeOperationType       operationType_;
    private TreeType                treeType_;
    private int                     noSamples_;
    private int                     sampleSize_;
    private int                     noThreads_;
    private String                  description_;
    private double                  result_;
    private HashMap<String, Object> params_;
    private boolean                 errorState_;

    public void setResult(double result) {
        this.result_ = result;
    }

    public HashMap<String, Object> getParams() {
        return params_;
    }

    public double getResult() {
        return result_;
    }

    public String getDescription() {
        return description_;
    }

    public int getNoThreads() {
        return noThreads_;
    }

    public int getSampleSize() {
        return sampleSize_;
    }

    public int getNoSamples() {
        return noSamples_;
    }

    public TreeOperationType getOperationType() {
        return operationType_;
    }

    public TreeImplementationType getImplementationType() {
        return implementationType_;
    }

    public TreeType getTreeType(){
        return treeType_;
    }

    public TestCase(TreeImplementationType implementationType, TreeOperationType operationType, TreeType treeType, int noSamples, int noThreads, int sampleSize, String description, ParameterEntry ... params){
        implementationType_ = implementationType;
        operationType_      = operationType;
        treeType_           = treeType;
        noSamples_          = noSamples;
        sampleSize_         = sampleSize;
        noThreads_          = noThreads;
        description_        = description;
        params_             = new HashMap<String, Object>();
        errorState_         = false;

        for(ParameterEntry thisEntry : params){
            params_.put(thisEntry.key, thisEntry.value);
        }
    }

    public void makeError(String problem){
        errorState_   = true;
        description_ += ": " + problem;
        noSamples_    = -1;
        sampleSize_   = -1;
        noThreads_    = -1;
        result_       = -1;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("------------ TestCase ------------------"   + System.lineSeparator());
        sb.append("description: "        + description_        + System.lineSeparator());
        sb.append("implementationType: " + implementationType_ + System.lineSeparator());
        sb.append("operationType: "      + operationType_      + System.lineSeparator());
        sb.append("tree type: "          + treeType_           + System.lineSeparator());
        sb.append("noSamples: "          + noSamples_          + System.lineSeparator());
        sb.append("sampleSize: "         + sampleSize_         + System.lineSeparator());
        sb.append("noThreads: "          + noThreads_          + System.lineSeparator());
        sb.append("result: "             + result_             + System.lineSeparator());
        sb.append("params: "             + System.lineSeparator());
        params_.forEach(new BiConsumer<String, Object>() {
            @Override
            public void accept(String s, Object e) {
                sb.append("{" + s + " : " + e + "}" + System.lineSeparator());
            }
        });
        return sb.toString();
    }

    public String getAsCSVLine(){
        StringBuilder sb = new StringBuilder();
        sb.append(getDescription()        + ",");
        sb.append(getImplementationType() + ",");
        sb.append(getOperationType()      + ",");
        sb.append(getNoSamples()          + ",");
        sb.append(getSampleSize()         + ",");
        sb.append(getNoThreads()          + ",");
        sb.append(getResult()             + ",");
        return sb.toString();
    }

    public static String getCSVHeader(){
        return "description, implementation type, operation type, sample count, sample size, thread count, rate (milliseconds per operation)";
    }
}