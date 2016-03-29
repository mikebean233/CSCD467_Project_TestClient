public enum TreeType {
    PrefixTree(0), BinarySearchTree(1);

    private final int _value;

    private TreeType(int value){
        _value = value;
    }

    @Override
    public String toString(){
        switch(_value){
            case 0:
                return "Prefix Tree";
            case 1:
                return "Binary Search Tree";
        }
        return "invalid";
    }

    public int getValue(){return _value;}
}
