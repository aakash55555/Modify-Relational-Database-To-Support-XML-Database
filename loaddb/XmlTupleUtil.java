package loaddb;

import global.AttrOperator;
import global.AttrType;
import global.IntervalType;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.IOException;
import java.util.ArrayList;

public class XmlTupleUtil {

    /**
     * @param rawTag
     * @return
     */
    public static String prepareString(String rawTag) {
        String tag = rawTag;
        if (rawTag.length() > 4) {
            tag = tag.substring(0, 5);
        } else {
            for (int i = rawTag.length(); i < 5; i++) {
                tag = tag + " ";
            }
        }
        return tag;
    }

    /**
     * @param tuple
     * @return
     */
    public static Tuple prepareTuple(Tuple tuple) {
        AttrType[] tupleDataType = getTupleAttrTypes(1);

        short[] tupleStringSize = getTupleStringShorts(1);

        try {
            tuple.setHdr((short) 3, tupleDataType, tupleStringSize);
        } catch (IOException | InvalidTypeException | InvalidTupleSizeException e) {
            e.printStackTrace();
        }

        return tuple;
    }

    /**
     * @param noOfNodes
     * @return
     */
    public static short[] getTupleStringShorts(int noOfNodes) {
        short[] tupleStringSize = new short[noOfNodes];
        for (int i = 0; i < noOfNodes; i++) {
            tupleStringSize[i] = 10;
        }
        return tupleStringSize;
    }

    /**
     * @param noOfNodes
     * @return
     */
    public static AttrType[] getTupleAttrTypes(int noOfNodes) {
        AttrType[] tupleDataType = new AttrType[noOfNodes * 3];
        for (int i = 0; i < noOfNodes * 3; i = i + 3) {
            tupleDataType[i] = new AttrType(AttrType.attrString);
            tupleDataType[i + 1] = new AttrType(AttrType.attrInterval);
            tupleDataType[i + 2] = new AttrType(AttrType.attrInteger);
        }
        return tupleDataType;
    }

    /**
     * @param tuple
     * @return
     */
    public static String tupleToString(Tuple tuple) {
        String tupleString = "Tuple  = [";
        for (int i = 0; i < tuple.noOfFlds(); i = i + 3) {
            try {
                tupleString = tupleString + " {Tag=" + tuple.getStrFld(i + 1) + ", " + tuple.getIntervalFld(i + 2).toString() + ", ParentNode= " + tuple.getIntFld(i + 3) + "}";
            } catch (IOException | FieldNumberOutOfBoundException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        tupleString = tupleString + " ]";
        return tupleString;
    }

    /**
     * @param nodesInOuter
     * @param nodesInInner
     * @param skipNodeInInnerList
     * @return
     */
    public static FldSpec[] getTupleProjection(int nodesInOuter, int nodesInInner, ArrayList<Integer> skipNodeInInnerList) {
        FldSpec[] projection = new FldSpec[(nodesInOuter + nodesInInner) * 3];

        ArrayList<Integer> skipList;
        if (skipNodeInInnerList == null) {
            skipList = new ArrayList<>();
        } else {
            skipList = skipNodeInInnerList;
        }
        for (int i = 0; i < nodesInOuter * 3; i = i + 3) {
            projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            projection[i + 1] = new FldSpec(new RelSpec(RelSpec.outer), i + 2);
            projection[i + 2] = new FldSpec(new RelSpec(RelSpec.outer), i + 3);
        }
        int nodePosition = 1;
        int l = nodesInOuter * 3;
        for (int k = nodesInOuter * 3; k < (nodesInOuter + nodesInInner) * 3; l = l + 3) {
            if (!skipList.contains(nodePosition)) {
                projection[k] = new FldSpec(new RelSpec(RelSpec.innerRel), l - (nodesInOuter * 3) + 1);
                projection[k + 1] = new FldSpec(new RelSpec(RelSpec.innerRel), l - (nodesInOuter * 3) + 2);
                projection[k + 2] = new FldSpec(new RelSpec(RelSpec.innerRel), l - (nodesInOuter * 3) + 3);
                k = k + 3;
            }
            nodePosition++;
        }
        return projection;
    }

    /**
     * @param firstOffset
     * @param secondOffset
     * @param attrOperator
     * @return
     */
    public static CondExpr[] getConditionExprForJoin(int firstOffset, int secondOffset, int attrOperator, String relationType) {
        CondExpr[] outFilter = new CondExpr[1];
        outFilter[0] = new CondExpr();
        outFilter[0].next = null;
        outFilter[0].op = new AttrOperator(attrOperator);
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), firstOffset);
        outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), secondOffset);
        if (relationType.equals("AD")) outFilter[0].relationType = 0;
        else if (relationType.equals("PC")) outFilter[0].relationType = 1;
        if (attrOperator == 0) outFilter[0].flag = 0;

        return outFilter;
    }

    public static CondExpr[] getConditionExprForIntJoin(int firstOffset, int secondOffset, int attrOperator, String relationType, String tag) {
        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[0].next = null;
        outFilter[0].op = new AttrOperator(attrOperator);
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), firstOffset);
        outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), secondOffset);
        if (relationType.equals("AD")) outFilter[0].relationType = 0;
        else if (relationType.equals("PC")) outFilter[0].relationType = 1;
        if (attrOperator == 0) outFilter[0].flag = 0;

        outFilter[1] = new CondExpr();
        outFilter[1].next = null;
        outFilter[1].op = new AttrOperator(AttrOperator.aopEQ);
        outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), secondOffset - 1);
        outFilter[1].type2 = new AttrType(AttrType.attrString);
        outFilter[1].operand2.string = tag;

        return outFilter;
    }

    public static CondExpr[] getScanFilterForTagIndex(int offset, String tag) {
        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[0].next = null;
        outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        outFilter[0].next = null;
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].type2 = new AttrType(AttrType.attrString);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset - 1);
        outFilter[0].operand2.string = tag;

        outFilter[1] = null;
        return outFilter;
    }

    public static CondExpr[] getScanFilterForIntIndex(int offset, IntervalType intervalType) {
        CondExpr[] outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[0].next = null;
        outFilter[0].op = new AttrOperator(AttrOperator.aopGT);
        outFilter[0].next = null;
        outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        outFilter[0].type2 = new AttrType(AttrType.attrInterval);
        outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset - 1);
        outFilter[0].operand2.intervalType = intervalType;

        outFilter[1] = null;
        return outFilter;
    }
}
