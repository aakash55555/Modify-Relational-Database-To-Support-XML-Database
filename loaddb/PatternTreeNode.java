package loaddb;

import static loaddb.XmlTupleUtil.prepareString;

// This class represents input pattern tree node

/**
 *
 */
public class PatternTreeNode {
    private String firstNode;
    private String secondNode;
    private String relation;
    private int firstNodeIndex;
    private int secondNodeIndex;

    public PatternTreeNode(String firstNode, String secondNode, String relation, int firstNodeIndex, int secondNodeIndex) {
        this.firstNode = prepareString(firstNode);
        this.secondNode = prepareString(secondNode);
        this.relation = relation;
        this.firstNodeIndex = firstNodeIndex;
        this.secondNodeIndex = secondNodeIndex;
    }

    public String getFirstNode() {
        return firstNode;
    }

    public String getSecondNode() {
        return secondNode;
    }

    public String getRelation() {
        return relation;
    }

    public int getFirstNodeIndex() {
        return firstNodeIndex;
    }

    public int getSecondNodeIndex() {
        return secondNodeIndex;
    }
}