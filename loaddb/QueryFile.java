package loaddb;

import heap.Heapfile;

import java.util.ArrayList;

public class QueryFile {

    private ArrayList<PatternTreeNode> patternTreeNodes;
    private ArrayList<String> tags;
    private Heapfile[] hfs;

    public QueryFile(ArrayList<PatternTreeNode> patternTreeNodes, ArrayList<String> tags, Heapfile[] hfs) {
        this.patternTreeNodes = patternTreeNodes;
        this.tags = tags;
        this.hfs = hfs;
    }

    public ArrayList<PatternTreeNode> getPatternTreeNodes() {
        return patternTreeNodes;
    }

    public ArrayList<String> getTags() {
        return tags;
    }

    public Heapfile[] getHFs() {
        return hfs;
    }
}
