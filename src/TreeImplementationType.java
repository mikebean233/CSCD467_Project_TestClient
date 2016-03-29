public enum TreeImplementationType implements Comparable<TreeImplementationType>{
    DynamoDB(0),
    EC2(1);

    private final int _value;

    private TreeImplementationType(int value){
        _value = value;
    }

    @Override
    public String toString(){

        switch(_value){
            case 0:
                return "DynamoDB";
            case 1:
                return "EC2";
        }
        return "invalid";
    }

    public int getValue(){return _value;}
}


