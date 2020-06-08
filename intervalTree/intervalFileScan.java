package intervalTree;

import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

import java.io.IOException;

public class intervalFileScan extends IndexFileScan
        implements GlobalConst {

    intervalTreeFile bfile;
    String treeFilename;     // intervalTree tree we're scanning
    ITLeafPage leafPage;   // leaf page containing current record
    RID curRid;       // position in current leaf; note: this is
    // the RID of the key/RID pair within the
    // leaf page.
    boolean didfirst;        // false only before getNext is called
    boolean deletedcurrent;  // true after deleteCurrent is called (read
    // by get_next, written by deleteCurrent).

    KeyClass endkey;    // if NULL, then go all the way right
    // else, stop when current record > this value.
    // (that is, implement an inclusive range
    // scan -- the only way to do a search for
    // a single value).
    int keyType;
    int maxKeysize;


    /**
     * Delete currently-being-scanned(i.e., just scanned)
     * data entry.
     *
     * @throws ScanDeleteException delete error when scan
     */

//    public intervalFileScan(){}
    public void delete_current()
            throws ScanDeleteException {

        KeyDataEntry entry;
        try {
            if (leafPage == null) {
                System.out.println("No Record to delete!");
                throw new ScanDeleteException();
            }

            if ((deletedcurrent == true) || (didfirst == false))
                return;

            entry = leafPage.getCurrent(curRid);
            SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
            bfile.Delete(entry.key, ((LeafData) entry.data).getData());
            leafPage = bfile.findRunStart(entry.key, curRid);

            deletedcurrent = true;
            return;
        } catch (Exception e) {
            e.printStackTrace();
            throw new ScanDeleteException();
        }
    }


    public void DestroyintervalTreeFileScan()
            throws IOException, bufmgr.InvalidFrameNumberException, bufmgr.ReplacerException,
            bufmgr.PageUnpinnedException, bufmgr.HashEntryNotFoundException {
        if (leafPage != null) {
            SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
        }
        leafPage = null;
    }


    public KeyDataEntry get_next()
            throws ScanIteratorException {

        KeyDataEntry entry;
        PageId nextpage;
        try {
            if (leafPage == null)
                return null;

            if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
                didfirst = true;
                deletedcurrent = false;
                entry = leafPage.getCurrent(curRid);
            } else {
                entry = leafPage.getNext(curRid);
            }

            while (entry == null) {
                nextpage = leafPage.getNextPage();
                SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
                if (nextpage.pid == INVALID_PAGE) {
                    leafPage = null;
                    return null;
                }

                leafPage = new ITLeafPage(nextpage, keyType);

                entry = leafPage.getFirst(curRid);
            }

            if (endkey != null)
                if (IT.keyCompare(entry.key, endkey) > 0) {
                    // went past right end of scan
                    SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
                    leafPage = null;
                    return null;
                }

            return entry;
        } catch (Exception e) {
            e.printStackTrace();
            throw new ScanIteratorException();
        }
    }

    public int keysize() {
        return maxKeysize;
    }

}