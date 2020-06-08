//FALLBACK PAGE
package loaddb;

import bufmgr.PCounter;
import global.AttrOperator;
import global.SystemDefs;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import iterator.Iterator;
import iterator.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static global.GlobalConst.MINIBASE_BUFFER_POOL_SIZE;
import static global.GlobalConst.MINIBASE_NUMBER_OF_PAGES;
import static loaddb.DBUtil.storeXmlAsHeapFile;
import static loaddb.HeapFileUtil.createSubsetHFforAllTags;
import static loaddb.XmlTupleUtil.*;

/**
 * Loads datafile from given path and queries based on input pattern tree
 */
public class XmlDB {

    private static String opTempHF = "temp_op";
    private static FileScan fs;
    private static FileScan fs2;

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String dbPath = "/tmp/" + System.getProperty("user.name") + ".minibase.storexml";

        System.out.println("Enter data file path:");
        //String filePath = "/home/yash/Desktop/sample1.xml";
        ///home/yash/Desktop/sample1.xml
        ///home/yash/Downloads/input_files/Queries2.txt
        String filePath = sc.nextLine();
        //String filePath = "/home/yash/asu/coursework/cse510/project/phase2/minjava/javaminibase/src/sample.xml";

        //Creates a DB with Random access file
        new SystemDefs(dbPath, MINIBASE_NUMBER_OF_PAGES, MINIBASE_BUFFER_POOL_SIZE, "Clock");

        storeXmlAsHeapFile(filePath);

        while (true) {
            System.out.println("Enter complex query file path:");
            String queryPath = sc.nextLine();
            String complexPath = queryPath;

            String queryPath1 = "";
            String queryPath2 = "";
            String operation = "";
            int operator1 = -1;
            int operator2 = -1;
            int bufSize = 1000;
            try (BufferedReader reader = new BufferedReader(new FileReader(complexPath))) {
                String temp2 = reader.readLine();
                queryPath1 = temp2.split(":")[1].trim();
                String nextLine = reader.readLine();
                if (nextLine.contains("ptree_2")) {
                    queryPath2 = nextLine.split(":")[1].trim();
                    String x = reader.readLine();
                    String temp = x.split(":")[1].trim();
                    operation = temp;
                    if (!operation.equals("CP")) {
                        operation = temp.split(" ")[0];
                        operator1 = Integer.parseInt(temp.split(" ")[1]);
                        operator2 = Integer.parseInt(temp.split(" ")[2]);
                    }
                    x = reader.readLine();
                    bufSize = Integer.parseInt(x.split(":")[1].trim());
                } else if (nextLine.contains("operation")) {

                    String temp = nextLine.split(":")[1].trim();
                    operation = temp.split(" ")[0];
                    operator1 = Integer.parseInt(temp.split(" ")[1]);
                    bufSize = Integer.parseInt(reader.readLine().split(":")[1].trim());
                } else {
                    bufSize = Integer.parseInt(nextLine.split(":")[1].trim());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            //TODO remove this if buffer issue is resolved
            bufSize = 1000;

            QueryFile q1;
            QueryFile q2 = null;

            OpIterator opIterator1;
            OpIterator opIterator2;
            switch (operation) {
                case "CP":
                    q1 = readPatternTree(queryPath1, null, false);
                    q2 = readPatternTree(queryPath2, q1.getTags(), true);
                    System.out.println("QUERY PLAN 1");
                    System.out.println("------------");
                    opIterator1 = getQP1(q1.getPatternTreeNodes(), q1.getTags(), -1, bufSize);
                    opIterator2 = getQP1(q2.getPatternTreeNodes(), q2.getTags(), -1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println("Query plan for pattern_tree2 : " + opIterator2.getQueryPlan());
                    System.out.println();
                    CARTESIAN_PRODUCT(opIterator1, opIterator2, bufSize);

                    printCounter();

                    System.out.println("\nQUERY PLAN 2");
                    System.out.println("------------");
                    opIterator1 = getQP2(q1.getPatternTreeNodes(), q1.getTags(), -1, bufSize);
                    opIterator2 = getQP2(q2.getPatternTreeNodes(), q2.getTags(), -1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println("Query plan for pattern_tree2 : " + opIterator2.getQueryPlan());
                    System.out.println();
                    CARTESIAN_PRODUCT(opIterator1, opIterator2, bufSize);

                    printCounter();

                    System.out.println("\nQUERY PLAN 3");
                    System.out.println("------------");
                    opIterator1 = getQP3(q1.getPatternTreeNodes(), q1.getTags(), -1, bufSize);
                    opIterator2 = getQP3(q2.getPatternTreeNodes(), q2.getTags(), -1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println("Query plan for pattern_tree2 : " + opIterator2.getQueryPlan());
                    System.out.println();
                    CARTESIAN_PRODUCT(opIterator1, opIterator2, bufSize);

                    printCounter();
                    break;
                case "TJ":
                    q1 = readPatternTree(queryPath1, null, false);
                    q2 = readPatternTree(queryPath2, q1.getTags(), true);
                    System.out.println("QUERY PLAN 1");
                    System.out.println("------------");
                    opIterator1 = getQP1(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    opIterator2 = getQP1(q2.getPatternTreeNodes(), q2.getTags(), operator2, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println("Query plan for pattern_tree2 : " + opIterator2.getQueryPlan());
                    System.out.println();
                    TAG_JOIN(opIterator1, opIterator2, bufSize);

                    printCounter();

                    System.out.println("\nQUERY PLAN 2");
                    System.out.println("------------");
                    opIterator1 = getQP2(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    opIterator2 = getQP2(q2.getPatternTreeNodes(), q2.getTags(), operator2, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println("Query plan for pattern_tree2 : " + opIterator2.getQueryPlan());
                    System.out.println();
                    TAG_JOIN(opIterator1, opIterator2, bufSize);

                    printCounter();

                    System.out.println("\nQUERY PLAN 3");
                    System.out.println("------------");
                    opIterator1 = getQP3(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    opIterator2 = getQP3(q2.getPatternTreeNodes(), q2.getTags(), operator2, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println("Query plan for pattern_tree2 : " + opIterator2.getQueryPlan());
                    System.out.println();
                    TAG_JOIN(opIterator1, opIterator2, bufSize);

                    printCounter();
                    break;
                case "NJ":
                    q1 = readPatternTree(queryPath1, null, false);
                    q2 = readPatternTree(queryPath2, q1.getTags(), true);
                    System.out.println("QUERY PLAN 1");
                    System.out.println("------------");
                    opIterator1 = getQP1(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    opIterator2 = getQP1(q2.getPatternTreeNodes(), q2.getTags(), operator2, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println("Query plan for pattern_tree2 : " + opIterator2.getQueryPlan());
                    System.out.println();
                    NODE_JOIN(opIterator1, opIterator2, q2, bufSize);

                    printCounter();

                    System.out.println("QUERY PLAN 2");
                    System.out.println("------------");
                    opIterator1 = getQP2(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    opIterator2 = getQP2(q2.getPatternTreeNodes(), q2.getTags(), operator2, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println("Query plan for pattern_tree2 : " + opIterator2.getQueryPlan());
                    System.out.println();
                    NODE_JOIN(opIterator1, opIterator2, q2, bufSize);

                    printCounter();

                    System.out.println("QUERY PLAN 3");
                    System.out.println("------------");
                    opIterator1 = getQP3(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    opIterator2 = getQP3(q2.getPatternTreeNodes(), q2.getTags(), operator2, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println("Query plan for pattern_tree2 : " + opIterator2.getQueryPlan());
                    System.out.println();
                    NODE_JOIN(opIterator1, opIterator2, q2, bufSize);

                    printCounter();
                    break;
                case "SRT":
                    q1 = readPatternTree(queryPath1, null, false);
                    System.out.println("QUERY PLAN 1");
                    System.out.println("------------");
                    opIterator1 = getQP1(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println();
                    SORT(opIterator1);

                    printCounter();

                    System.out.println("QUERY PLAN 2");
                    System.out.println("------------");
                    opIterator1 = getQP2(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println();
                    SORT(opIterator1);

                    printCounter();

                    System.out.println("QUERY PLAN 3");
                    System.out.println("------------");
                    opIterator1 = getQP3(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println();
                    SORT(opIterator1);

                    printCounter();
                    break;
                case "GRP":
                    q1 = readPatternTree(queryPath1, null, false);


                    System.out.println("QUERY PLAN 1");
                    System.out.println("------------");
                    opIterator1 = getQP3(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println();
                    GROUP_BY(opIterator1);

                    printCounter();

                    System.out.println("QUERY PLAN 2");
                    System.out.println("------------");
                    opIterator1 = getQP3(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println();
                    GROUP_BY(opIterator1);

                    printCounter();

                    System.out.println("QUERY PLAN 3");
                    System.out.println("------------");
                    opIterator1 = getQP3(q1.getPatternTreeNodes(), q1.getTags(), operator1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println();
                    GROUP_BY(opIterator1);

                    printCounter();
                    break;
                default:
                    q1 = readPatternTree(queryPath1, null, false);
                    System.out.println("QUERY PLAN 1");
                    System.out.println("------------");
                    opIterator1 = getQP1(q1.getPatternTreeNodes(), q1.getTags(), -1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println();
                    printIterator(opIterator1.getIterator());

                    printCounter();

                    System.out.println("QUERY PLAN 2");
                    System.out.println("------------");
                    opIterator1 = getQP2(q1.getPatternTreeNodes(), q1.getTags(), -1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println();
                    printIterator(opIterator1.getIterator());

                    printCounter();

                    System.out.println("QUERY PLAN 3");
                    opIterator1 = getQP3(q1.getPatternTreeNodes(), q1.getTags(), -1, bufSize);
                    System.out.println("Query plan for pattern_tree1 : " + opIterator1.getQueryPlan());
                    System.out.println();
                    printIterator(opIterator1.getIterator());

                    printCounter();
                    break;
            }
            fs.close();
            if (fs2 != null) fs2.close();
            if (q2 == null) deleteTempFiles(q1.getHFs(), q1.getTags(), null, null);
            else deleteTempFiles(q1.getHFs(), q1.getTags(), q2.getHFs(), q2.getTags());
        }
    }

    public static void printCounter() {
        System.out.println("\nRead Count: " + PCounter.getreads() + " ||| Write Count: " + PCounter.getwrites() + "\n");
        PCounter.initialize();
    }

    public static void printIterator(Iterator iterator) {

        Iterator i2 = iterator;
        try {
            Tuple tuple;
            while ((tuple = i2.get_next()) != null) {
                System.out.println(tupleToString(tuple));
            }
            iterator.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //Usage : NODE_JOIN(opIterator1, opIterator2, q2);
    private static void NODE_JOIN(OpIterator opIterator1, OpIterator opIterator2, QueryFile q2, int bufSize) {
        try {
            Heapfile hf = null;
            try {
                hf = createOpTempHF(opIterator2.getIterator());
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                int nodesIn1 = getNumberOfNodesFromName(opIterator1.getOrder());
                int nodesIn2 = getNumberOfNodesFromName(opIterator2.getOrder());

                CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(opIterator1.getOrder(), opIterator1.getOperationTag()),
                        getIntervalFieldPosition(opIterator2.getOrder(), opIterator1.getOperationTag()), AttrOperator.aopEQ, "");

                NestedLoopsJoins nlj = new NestedLoopsJoins(getTupleAttrTypes(nodesIn1), nodesIn1 * 3, getTupleStringShorts(nodesIn1),
                        getTupleAttrTypes(nodesIn2), nodesIn2 * 3, getTupleStringShorts(nodesIn2),
                        bufSize, opIterator1.getIterator(), opTempHF, outFilter, null, getTupleProjection(nodesIn1, nodesIn2, null), (nodesIn1 + nodesIn2) * 3);

                try {
                    Tuple tuple;
                    while ((tuple = nlj.get_next()) != null) {
                        tuple.printTree(getTupleAttrTypes((nodesIn1 + nodesIn2) * 3), nodesIn1 * 3, nodesIn2 * 3);
                        System.out.println();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

                nlj.close();
                opIterator1.getIterator().close();
                opIterator2.getIterator().close();
                hf.deleteFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Usage : SORT(inlj1);
    public static void SORT(OpIterator opIterator1) {
        int nodesIn1 = getNumberOfNodesFromName(opIterator1.getOrder());
        Iterator fscan = null;
        try {
            fscan = new Sort(getTupleAttrTypes(nodesIn1), (short) (nodesIn1 * 3), getTupleStringShorts(nodesIn1),
                    opIterator1.getIterator(), getIntervalFieldPosition(opIterator1.getOrder(), opIterator1.getOperationTag()) - 1,
                    new TupleOrder(TupleOrder.Ascending), 12, 1000);

        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Tuple tuple;
            while ((tuple = fscan.get_next()) != null) {
                System.out.println(tupleToString(tuple));
            }
            fscan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Usage : GROUP_BY(inlj1);
    public static void GROUP_BY(OpIterator opIterator1) {
        int nodesIn1 = getNumberOfNodesFromName(opIterator1.getOrder());
        Iterator fscan = null;
        int sort_fldNo = getIntervalFieldPosition(opIterator1.getOrder(), opIterator1.getOperationTag()) - 1;
        try {
            fscan = new Sort(getTupleAttrTypes(nodesIn1), (short) (nodesIn1 * 3), getTupleStringShorts(nodesIn1),
                    opIterator1.getIterator(), sort_fldNo,
                    new TupleOrder(TupleOrder.Ascending), 12, 1000);

        } catch (Exception e) {
            e.printStackTrace();
        }

        Tuple tuple;
        String tag = "";
        Tuple outTuple = new Tuple();
        Tuple JTuple = new Tuple();
        int count = 0;
        try {
            while ((tuple = fscan.get_next()) != null) {
                JTuple = new Tuple();
                if (tag.equals("")) {
                    tag = tuple.getStrFld(sort_fldNo);
                    outTuple = tuple;
                    count = 1;
                } else if (tag.equals(tuple.getStrFld(sort_fldNo))) {
                    int size = count * nodesIn1;
                    TupleUtils.setup_op_tuple(JTuple, getTupleAttrTypes((size + nodesIn1) * 3),
                            getTupleAttrTypes(size * 3), size * 3,
                            getTupleAttrTypes(nodesIn1), nodesIn1 * 3,
                            getTupleStringShorts(size), getTupleStringShorts(nodesIn1),
                            getTupleProjection(size, nodesIn1, null), (size + nodesIn1) * 3);
                    Projection.Join(outTuple, getTupleAttrTypes(size * 3),
                            tuple, getTupleAttrTypes(nodesIn1 * 3),
                            JTuple, getTupleProjection(size, nodesIn1, null), (size + nodesIn1) * 3);
                    outTuple = JTuple;
                    count = count + 1;

                } else {
                    outTuple.printmultinode(getTupleAttrTypes(((count * nodesIn1) + nodesIn1) * 3), nodesIn1 * 3, count);
                    tag = tuple.getStrFld(sort_fldNo);
                    outTuple = tuple;
                    count = 1;
                }
            }
            outTuple.printmultinode(getTupleAttrTypes(((count * nodesIn1) + nodesIn1) * 3), nodesIn1 * 3, count);
            fscan.close();
        } catch (Exception e) {
            try {
                fscan.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }

    }

    //Usage : CARTESIAN_PRODUCT(inlj1, inlj2);
    public static void CARTESIAN_PRODUCT(OpIterator opIterator1, OpIterator opIterator2, int bufSize) {
        Heapfile hf = null;
        try {
            hf = createOpTempHF(opIterator2.getIterator());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            int nodesIn1 = getNumberOfNodesFromName(opIterator1.getOrder());
            int nodesIn2 = getNumberOfNodesFromName(opIterator2.getOrder());

            NestedLoopsJoins nlj = new NestedLoopsJoins(getTupleAttrTypes(nodesIn1), nodesIn1 * 3, getTupleStringShorts(nodesIn1),
                    getTupleAttrTypes(nodesIn2), nodesIn2 * 3, getTupleStringShorts(nodesIn2),
                    bufSize, opIterator1.getIterator(), opTempHF, null, null, getTupleProjection(nodesIn1, nodesIn2, null), (nodesIn1 + nodesIn2) * 3);

            try {
                Tuple tuple;
                while ((tuple = nlj.get_next()) != null) {
                    tuple.printTree(getTupleAttrTypes((nodesIn1 + nodesIn2) * 3), nodesIn1 * 3, nodesIn2 * 3);
                    System.out.println();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            nlj.close();
            opIterator1.getIterator().close();
            opIterator2.getIterator().close();
            hf.deleteFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Usage : TAG_JOIN(inlj1, inlj2);
    private static void TAG_JOIN(OpIterator opIterator1, OpIterator opIterator2, int bufSize) {
        try {
            Heapfile hf = null;
            try {
                hf = createOpTempHF(opIterator2.getIterator());
            } catch (Exception e) {
                e.printStackTrace();
            }


            try {
                int nodesIn1 = getNumberOfNodesFromName(opIterator1.getOrder());
                int nodesIn2 = getNumberOfNodesFromName(opIterator2.getOrder());

                CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(opIterator1.getOrder(), opIterator1.getOperationTag()) - 1,
                        getIntervalFieldPosition(opIterator2.getOrder(), opIterator2.getOperationTag()) - 1, AttrOperator.aopEQ, "");

                NestedLoopsJoins nlj = new NestedLoopsJoins(getTupleAttrTypes(nodesIn1), nodesIn1 * 3, getTupleStringShorts(nodesIn1),
                        getTupleAttrTypes(nodesIn2), nodesIn2 * 3, getTupleStringShorts(nodesIn2),
                        bufSize, opIterator1.getIterator(), opTempHF, outFilter, null, getTupleProjection(nodesIn1, nodesIn2, null), (nodesIn1 + nodesIn2) * 3);

                try {
                    Tuple tuple;
                    while ((tuple = nlj.get_next()) != null) {
                        tuple.printTree(getTupleAttrTypes((nodesIn1 + nodesIn2) * 3), nodesIn1 * 3, nodesIn2 * 3);
                        System.out.println();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

                nlj.close();
                opIterator1.getIterator().close();
                opIterator2.getIterator().close();
                hf.deleteFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Heapfile createOpTempHF(Iterator it) {
        Heapfile hf = null;
        try {
            hf = new Heapfile(opTempHF);
            Tuple tuple;
            while ((tuple = it.get_next()) != null) {
                hf.insertRecord(tuple.returnTupleByteArray());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hf;
    }

    public static void deleteTempFiles(Heapfile[] hfs1, ArrayList<String> tags1, Heapfile[] hfs2, ArrayList<String> tags2) {
        for (int i = 0; i < hfs1.length; i++) {
            try {
                if (hfs1[i] != null) hfs1[i].deleteFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (hfs2 != null) {
            for (int i = 0; i < hfs2.length; i++) {
                try {
                    if (!tags1.contains(tags2.get(i)) && hfs2[i] != null) hfs2[i].deleteFile();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static QueryFile readPatternTree(String filename, ArrayList<String> checkTagsList, boolean checklist) {
        ArrayList<PatternTreeNode> patternTreeNodes = new ArrayList<>();

        ArrayList<String> tags = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line = reader.readLine();
            int noOfTags = Integer.parseInt(line.trim());
            tags = new ArrayList<>();
            for (int i = 0; i < noOfTags; i++) {
                line = reader.readLine();
                tags.add(prepareString(line.trim().replace("\"", "")));
            }
            line = reader.readLine();
            while (line != null) {
                String[] temp = line.split(" ");
                PatternTreeNode ptn = new PatternTreeNode(tags.get(Integer.parseInt(temp[0]) - 1), tags.get(Integer.parseInt(temp[1]) - 1), temp[2], Integer.parseInt(temp[0]), Integer.parseInt(temp[0]));
                patternTreeNodes.add(ptn);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Heapfile[] hfs = createSubsetHFforAllTags(tags, checkTagsList, checklist);

        //Collections.sort(patternTreeNodes, Comparator.comparingInt(PatternTreeNode::getFirstNodeIndex).thenComparingInt(o -> tagCount[o.getFirstNodeIndex() - 1]));
        return new QueryFile(patternTreeNodes, tags, hfs);
    }


    public static OpIterator getQP1(ArrayList<PatternTreeNode> patternTreeNodes, ArrayList<String> tags, int operator, int bufSize) {
        String tupleOrder = "";
        Iterator smj = null;
        int i = 0;
        String queryPlan = "";
        while (i < patternTreeNodes.size() - 1) {

            try {
                if (i == 0) {
                    PatternTreeNode p = patternTreeNodes.get(i);
                    String outer = p.getFirstNode();
                    String inner = p.getSecondNode();
                    if (outer.contains("*")) outer = "xml.sort";
                    if (inner.contains("*")) inner = "xml.sort";

                    fs = new FileScan(outer, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);

                    fs2 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);

                    CondExpr[] joinFilter = getConditionExprForJoin(2, 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            2, 10, 2, 50, bufSize, fs, fs2, false,
                            false, new TupleOrder(TupleOrder.Ascending), joinFilter, getTupleProjection(1, 1, null), 6);
                    tupleOrder = p.getFirstNode() + "│" + p.getSecondNode();
                    queryPlan = "(" + p.getFirstNode() + " smj " + p.getSecondNode() + ")";
                }
                if (patternTreeNodes.get(i + 1).getFirstNode().contains("*") || patternTreeNodes.get(i + 1).getSecondNode().contains("*")) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //SMJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    FileScan fs1 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);


                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getIntervalFieldPosition(tupleOrder, joinNode), 50, 2, 50, bufSize, smj, fs1, false,
                            false, new TupleOrder(TupleOrder.Ascending), outFilter, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " smj " + p.getSecondNode() + ")";
                    continue;
                }

                if (i == patternTreeNodes.size() - 2 && patternTreeNodes.get(i).getFirstNodeIndex() != patternTreeNodes.get(i + 1).getFirstNodeIndex()) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //NLJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new NestedLoopsJoins(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            bufSize, smj, inner, outFilter, null, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " inlj " + p.getSecondNode() + ")";
                    break;
                }
                if (i == patternTreeNodes.size() - 2 && patternTreeNodes.get(i).getFirstNodeIndex() == patternTreeNodes.get(i + 1).getFirstNodeIndex()) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //SMJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    FileScan fs1 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);


                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getIntervalFieldPosition(tupleOrder, joinNode), 50, 2, 50, bufSize, smj, fs1, false,
                            false, new TupleOrder(TupleOrder.Ascending), outFilter, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " smj " + p.getSecondNode() + ")";
                    break;
                }

                if (i < patternTreeNodes.size() - 1 && patternTreeNodes.get(i).getFirstNodeIndex() == patternTreeNodes.get(i + 1).getFirstNodeIndex()) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //SMJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    FileScan fs1 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);


                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getIntervalFieldPosition(tupleOrder, joinNode), 50, 2, 50, bufSize, smj, fs1, false,
                            false, new TupleOrder(TupleOrder.Ascending), outFilter, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " smj " + p.getSecondNode() + ")";
                    continue;
                }
                if (i < patternTreeNodes.size() - 2 && patternTreeNodes.get(i + 1).getFirstNodeIndex() == patternTreeNodes.get(i + 2).getFirstNodeIndex()) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //SMJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    FileScan fs1 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);


                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getIntervalFieldPosition(tupleOrder, joinNode), 50, 2, 50, bufSize, smj, fs1, false,
                            false, new TupleOrder(TupleOrder.Ascending), outFilter, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " smj " + p.getSecondNode() + ")";
                    continue;
                } else {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //NLJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new NestedLoopsJoins(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            bufSize, smj, inner, outFilter, null, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " inlj " + p.getSecondNode() + ")";

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        String tag = "";
        if (operator != -1) tag = tags.get(operator - 1);
        return new OpIterator(smj, tupleOrder, tag, queryPlan);
    }

    public static OpIterator getQP2(ArrayList<PatternTreeNode> patternTreeNodes, ArrayList<String> tags, int operator, int bufSize) {
        String tupleOrder = "";
        Iterator smj = null;
        int i = 0;
        String queryPlan = "";
        while (i < patternTreeNodes.size() - 1) {

            try {
                if (i == 0) {
                    PatternTreeNode p = patternTreeNodes.get(i);
                    String outer = p.getFirstNode();
                    String inner = p.getSecondNode();
                    if (outer.contains("*")) outer = "xml.sort";
                    if (inner.contains("*")) inner = "xml.sort";

                    fs = new FileScan(outer, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);

                    CondExpr[] joinFilter = getConditionExprForJoin(2, 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new NestedLoopsJoins(getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            bufSize, fs, inner, joinFilter, null, getTupleProjection(1, 1, null), 6);
                    tupleOrder = p.getFirstNode() + "│" + p.getSecondNode();
                    queryPlan = "(" + p.getFirstNode() + " inlj " + p.getSecondNode() + ")";
                }
                if (patternTreeNodes.get(i + 1).getFirstNode().contains("*") || patternTreeNodes.get(i + 1).getSecondNode().contains("*")) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //SMJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    FileScan fs1 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);


                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getIntervalFieldPosition(tupleOrder, joinNode), 50, 2, 50, bufSize, smj, fs1, false,
                            false, new TupleOrder(TupleOrder.Ascending), outFilter, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " smj " + p.getSecondNode() + ")";
                    continue;
                }
                if (i == patternTreeNodes.size() - 2 && patternTreeNodes.get(i).getFirstNodeIndex() != patternTreeNodes.get(i + 1).getFirstNodeIndex()) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //NLJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new NestedLoopsJoins(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            bufSize, smj, inner, outFilter, null, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " inlj " + p.getSecondNode() + ")";
                    break;
                }
                if (i == patternTreeNodes.size() - 2 && patternTreeNodes.get(i).getFirstNodeIndex() == patternTreeNodes.get(i + 1).getFirstNodeIndex()) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //SMJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    FileScan fs1 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);


                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getIntervalFieldPosition(tupleOrder, joinNode), 50, 2, 50, bufSize, smj, fs1, false,
                            false, new TupleOrder(TupleOrder.Ascending), outFilter, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " smj " + p.getSecondNode() + ")";
                    break;
                }

                if (i < patternTreeNodes.size() - 1 && patternTreeNodes.get(i).getFirstNodeIndex() == patternTreeNodes.get(i + 1).getFirstNodeIndex()) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //SMJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    FileScan fs1 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);


                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getIntervalFieldPosition(tupleOrder, joinNode), 50, 2, 50, bufSize, smj, fs1, false,
                            false, new TupleOrder(TupleOrder.Ascending), outFilter, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " smj " + p.getSecondNode() + ")";
                    continue;
                }
                if (i < patternTreeNodes.size() - 2 && patternTreeNodes.get(i + 1).getFirstNodeIndex() == patternTreeNodes.get(i + 2).getFirstNodeIndex()) {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //SMJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    FileScan fs1 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);


                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getIntervalFieldPosition(tupleOrder, joinNode), 50, 2, 50, bufSize, smj, fs1, false,
                            false, new TupleOrder(TupleOrder.Ascending), outFilter, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " smj " + p.getSecondNode() + ")";
                    continue;
                } else {
                    i++;
                    PatternTreeNode p = patternTreeNodes.get(i);
                    //NLJ
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new NestedLoopsJoins(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            bufSize, smj, inner, outFilter, null, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " inlj " + p.getSecondNode() + ")";

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        String tag = "";
        if (operator != -1) tag = tags.get(operator - 1);
        return new OpIterator(smj, tupleOrder, tag, queryPlan);
    }

    public static OpIterator getQP3(ArrayList<PatternTreeNode> patternTreeNodes, ArrayList<String> tags, int operator, int bufSize) {
        String tupleOrder = "";
        Iterator smj = null;

        String queryPlan = "";
        for (int i = 0; i < patternTreeNodes.size(); i++) {
            PatternTreeNode p = patternTreeNodes.get(i);
            try {
                if (i == 0) {

                    String outer = p.getFirstNode();
                    String inner = p.getSecondNode();
                    if (outer.contains("*")) outer = "xml.sort";
                    if (inner.contains("*")) inner = "xml.sort";

                    fs = new FileScan(outer, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);

                    fs2 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);

                    CondExpr[] joinFilter = getConditionExprForJoin(2, 2, AttrOperator.aopGT, p.getRelation());

                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            2, 10, 2, 50, bufSize, fs, fs2, false,
                            false, new TupleOrder(TupleOrder.Ascending), joinFilter, getTupleProjection(1, 1, null), 6);
                    tupleOrder = p.getFirstNode() + "│" + p.getSecondNode();
                    queryPlan = "(" + p.getFirstNode() + " smj " + p.getSecondNode() + ")";
                } else {
                    String inner = p.getSecondNode();
                    if (inner.contains("*")) inner = "xml.sort";

                    String joinNode = getCommonNode(tupleOrder, p.getFirstNode() + "│" + p.getSecondNode());
                    int noOfNodeInOuterRel = getNumberOfNodesFromName(tupleOrder);
                    CondExpr[] outFilter = getConditionExprForJoin(getIntervalFieldPosition(tupleOrder, joinNode), 2, AttrOperator.aopGT, p.getRelation());

                    fs2 = new FileScan(inner, getTupleAttrTypes(1), getTupleStringShorts(1),
                            (short) 3, (short) 3, getTupleProjection(1, 0, null), null);


                    //Join outer file with inner file based on AD/PC relations
                    smj = new SortMerge(getTupleAttrTypes(noOfNodeInOuterRel), noOfNodeInOuterRel * 3, getTupleStringShorts(noOfNodeInOuterRel),
                            getTupleAttrTypes(1), 3, getTupleStringShorts(1),
                            getIntervalFieldPosition(tupleOrder, joinNode), 50, 2, 50, bufSize, smj, fs2, false,
                            false, new TupleOrder(TupleOrder.Ascending), outFilter, getTupleProjection(noOfNodeInOuterRel, 1, null), (noOfNodeInOuterRel + 1) * 3);

                    tupleOrder = tupleOrder + "│" + p.getSecondNode();
                    queryPlan = "(" + queryPlan + " smj " + p.getSecondNode() + ")";
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        String tag = "";
        if (operator != -1) tag = tags.get(operator - 1);
        return new OpIterator(smj, tupleOrder, tag, queryPlan);
    }

    /**
     * @param fileName
     * @return
     */
    private static int getNumberOfNodesFromName(String fileName) {
        Set<String> mySet = new LinkedHashSet<>(Arrays.asList(fileName.split("│")));
        return mySet.size();
    }

    /**
     * @param nodeSequence
     * @param searchNode
     * @return
     */
    private static int getIntervalFieldPosition(String nodeSequence, String searchNode) {
        String[] nodeSeq = nodeSequence.split("│");
        ArrayList<String> nodeList = new ArrayList<>();
        for (int i = 0; i < nodeSeq.length; i++) {
            if (!nodeList.contains(nodeSeq[i])) {
                nodeList.add(nodeSeq[i]);
            }
        }
        int position = nodeList.indexOf(searchNode);
        int offset = (3 * position) + 2;
        return offset;
    }

    /**
     * @param temp1ArrayIn
     * @param temp2ArrayIn
     * @return
     */
    public static String getCommonNode(String temp1ArrayIn, String temp2ArrayIn) {
        String[] temp1Array = temp1ArrayIn.split("│");
        String[] temp2Array = temp2ArrayIn.split("│");
        String node = "";

        for (int i = 0; i < temp1Array.length; i++) {
            for (int j = 0; j < temp2Array.length; j++) {
                if (temp1Array[i].equals(temp2Array[j])) {
                    node = temp1Array[i];
                    break;
                }
            }
        }
        return node;
    }
}