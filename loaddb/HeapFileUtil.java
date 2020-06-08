package loaddb;

import global.IndexType;
import heap.Heapfile;
import heap.Tuple;
import index.BTIndexScan;
import iterator.CondExpr;
import iterator.Iterator;

import java.util.ArrayList;

import static loaddb.XmlTupleUtil.*;

public class HeapFileUtil {

    /**
     * @param tags
     */

    public static Integer[] tagCount;

    public static Heapfile[] createSubsetHFforAllTags(ArrayList<String> tags, ArrayList<String> checkTagList, boolean check) {
        //Heapfile hf = null;

        Heapfile[] hfSubset = new Heapfile[tags.size()];
        tagCount = new Integer[tags.size()];
        for (int i = 0; i < tags.size(); i++) {
            tagCount[i] = 0;
            try {
                String tag = tags.get(i);
                boolean process = true;
                if (check) {
                    if (checkTagList.contains(tag)) process = false;
                }
                if (tag.contains("*")) {
                    process = false;
                    tagCount[i] = 1600000;
                }
                if (process) {
                    hfSubset[i] = new Heapfile(tags.get(i));
                    Iterator scan = null;
                    try {
                        CondExpr[] scanFilter = getScanFilterForTagIndex(2, tag);

                        scan = new BTIndexScan(new IndexType(IndexType.B_Index), "xml.sort",
                                "xml.index", getTupleAttrTypes(1), getTupleStringShorts(1), 3, 3,
                                getTupleProjection(1, 0, null), scanFilter, 1, false);
                        Tuple tuple;
                        while ((tuple = scan.get_next()) != null) {
                            tuple = prepareTuple(tuple);
                            hfSubset[i].insertRecord(tuple.returnTupleByteArray());
                            tagCount[i]++;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if (scan != null) try {
                        scan.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return hfSubset;
    }
}
