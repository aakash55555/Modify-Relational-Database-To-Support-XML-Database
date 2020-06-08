package intervalTree;

import global.IntervalType;

public class intervalKey extends KeyClass {


    private IntervalType key;

    /**
     * Class constructor
     *
     * @param value the value of the integer key to be set
     */
    public intervalKey(IntervalType value) {
        key = new IntervalType(value.getStart(), value.getEnd());
    }


    public String toString() {
        return "(" +
                "start=" + key.getStart() +
                ", end=" + key.getEnd() +
                ')';
    }

    /**
     * get a copy of the integer key
     *
     * @return the reference of the copy
     */
    public IntervalType getKey() {
        return new IntervalType(key.getStart(), key.getEnd());
    }

    /**
     * set the integer key value
     */
    public void setKey(IntervalType value) {
        key = new IntervalType(value.getStart(), value.getEnd());
    }
}
