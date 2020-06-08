package global;

import java.io.Serializable;

public class IntervalType implements Serializable {

    private int start;

    private int end;

    public IntervalType() {
    }

    public IntervalType(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }


    public void setStart(int start) {
        this.start = start;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "[" + start +
                "," + end + "]";
    }
}
