public enum TreeOperationType implements Comparable<TreeOperationType> {
        Insert(0), Query(1),Delete(2);
        private final int _value;

        private TreeOperationType(int value){
            _value = value;
        }

        @Override
        public String toString(){
            switch(_value){
                case 0:
                    return "insert";
                case 1:
                    return "query";
                case 2:
                    return "delete";
            }
            return "invalid";
        }

        public int getValue(){return _value;}
    }
