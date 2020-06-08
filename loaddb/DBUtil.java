package loaddb;

import btree.BTreeFile;
import btree.StringKey;
import global.AttrType;
import global.IntervalType;
import global.RID;
import global.TupleOrder;
import heap.*;
import intervalTree.intervalTreeFile;
import iterator.FileScan;
import iterator.Iterator;
import iterator.Sort;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Stack;

import static loaddb.XmlTupleUtil.*;

public class DBUtil {
    /**
     * @param filePath
     * @return
     */

    public static Heapfile storeXmlAsHeapFile(String filePath) {
        boolean storedSuccessfully = true;
        Heapfile hf = null;
        try {
            hf = new Heapfile("xml.in");
        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            storedSuccessfully = false;
            e.printStackTrace();
        }
        System.out.println("Parsing File");
        //Read XML using StAX
        try {
            File xmlFile = new File(filePath);
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            XMLStreamReader streamReader = inputFactory.createXMLStreamReader(new FileReader(xmlFile));

            Stack<Tuple> xmlStack = new Stack();
            int intervalCounter = 1;
            int parentStart = 0;

            while (streamReader.hasNext()) {
                streamReader.next();
                if (streamReader.getEventType() == XMLStreamReader.START_ELEMENT) {
                    try {
                        //Prepare Node for Tag and push on stack
                        Tuple tuple = XmlTupleUtil.prepareTuple(new Tuple());
                        String rawTag = streamReader.getLocalName();

                        IntervalType intervalType = new IntervalType(intervalCounter++, -1);

                        tuple.setStrFld(1, XmlTupleUtil.prepareString(rawTag));
                        tuple.setIntervalFld(2, intervalType);
                        xmlStack.push(tuple);

                        parentStart = intervalCounter - 1;

                        //Create a node for every attribute and its value in the tuple
                        if (streamReader.getAttributeCount() > 0) {
                            for (int i = 0; i < streamReader.getAttributeCount(); i++) {
                                try {
                                    Tuple nameNodeTuple = XmlTupleUtil.prepareTuple(new Tuple());
                                    Tuple valueNodeTuple = XmlTupleUtil.prepareTuple(new Tuple());

                                    String nameTag = XmlTupleUtil.prepareString(streamReader.getAttributeLocalName(i));
                                    String valueTag = XmlTupleUtil.prepareString(streamReader.getAttributeValue(i));

                                    IntervalType nameTagIT = new IntervalType(intervalCounter++, -1);
                                    int parentStartforValueTag = intervalCounter - 1;

                                    IntervalType valueTagIT = new IntervalType(intervalCounter++, intervalCounter++);

                                    nameTagIT.setEnd(intervalCounter++);

                                    nameNodeTuple.setStrFld(1, nameTag);
                                    nameNodeTuple.setIntervalFld(2, nameTagIT);
                                    nameNodeTuple.setIntFld(3, parentStart);

                                    valueNodeTuple.setStrFld(1, valueTag);
                                    valueNodeTuple.setIntervalFld(2, valueTagIT);
                                    valueNodeTuple.setIntFld(3, parentStartforValueTag);

                                    hf.insertRecord(nameNodeTuple.returnTupleByteArray());
                                    hf.insertRecord(valueNodeTuple.returnTupleByteArray());
                                } catch (InvalidSlotNumberException | SpaceNotAvailableException | InvalidTupleSizeException | HFException | HFBufMgrException | HFDiskMgrException e) {
                                    e.printStackTrace();
                                    storedSuccessfully = false;
                                }
                            }
                        }
                    } catch (IOException | FieldNumberOutOfBoundException e) {
                        e.printStackTrace();
                        storedSuccessfully = false;
                    }
                } else if (streamReader.getEventType() == XMLStreamReader.CHARACTERS && !streamReader.getText().startsWith("\n")) {
                    //Store value node for the start tag
                    Tuple valueNodeTuple = XmlTupleUtil.prepareTuple(new Tuple());

                    IntervalType valueIT = new IntervalType(intervalCounter++, intervalCounter++);

                    String tagValue = streamReader.getText();
                    valueNodeTuple.setStrFld(1, XmlTupleUtil.prepareString(tagValue));
                    valueNodeTuple.setIntervalFld(2, valueIT);
                    valueNodeTuple.setIntFld(3, parentStart);

                    try {
                        hf.insertRecord(valueNodeTuple.returnTupleByteArray());

                    } catch (InvalidSlotNumberException | IOException | InvalidTupleSizeException | SpaceNotAvailableException | HFException | HFBufMgrException | HFDiskMgrException e) {
                        e.printStackTrace();
                        storedSuccessfully = false;
                    }
                } else if (streamReader.getEventType() == XMLStreamReader.END_ELEMENT) {
                    //Pop node from stack on end element
                    try {

                        Tuple onStackTuple = xmlStack.pop();
                        IntervalType stackTupleIT = onStackTuple.getIntervalFld(2);
                        stackTupleIT.setEnd(intervalCounter++);

                        parentStart = 0;
                        if (!xmlStack.empty()) {
                            Tuple kParent = xmlStack.peek();
                            parentStart = kParent.getIntervalFld(2).getStart();
                        }
                        onStackTuple.setIntervalFld(2, stackTupleIT);
                        onStackTuple.setIntFld(3, parentStart);

                        hf.insertRecord(onStackTuple.returnTupleByteArray());

                    } catch (IOException | ClassNotFoundException | InvalidSlotNumberException | InvalidTupleSizeException | SpaceNotAvailableException | HFException | HFBufMgrException | HFDiskMgrException e) {
                        e.printStackTrace();
                        storedSuccessfully = false;
                    }
                }

            }
        } catch (XMLStreamException | IOException | FieldNumberOutOfBoundException e) {
            e.printStackTrace();
            storedSuccessfully = false;
        }
        Heapfile hfnew = null;
        if (!storedSuccessfully) {
            System.err.println("Error while storing. Exiting.");
            System.exit(1);
        } else {

            try {

                BTreeFile btf = null;
                intervalTreeFile itf = null;
                try {
                    hfnew = new Heapfile("xml.sort");
                    btf = new BTreeFile("xml.index", AttrType.attrString, 12, 1);
                    itf = new intervalTreeFile("int.index", AttrType.attrInterval, 12, 1);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                FileScan fs = new FileScan("xml.in", getTupleAttrTypes(1), getTupleStringShorts(1),
                        (short) 3, (short) 3, getTupleProjection(1, 0, null), null);

                Iterator fscan = new Sort(getTupleAttrTypes(1), (short) 3, getTupleStringShorts(1), fs, 1,
                        new TupleOrder(TupleOrder.Ascending), 12, 1000);

                Tuple tuple;
                System.out.println("Sorting File");
                while ((tuple = fscan.get_next()) != null) {
                    hfnew.insertRecord(tuple.returnTupleByteArray());
                }
                System.out.println("Creating Tag Based and Interval Based B+ Tree Index");

                Scan scanForBTagIndex = hfnew.openScan();
                RID ridBIndex = new RID();
                Tuple tuple1;
                while ((tuple1 = scanForBTagIndex.getNext(ridBIndex)) != null) {
                    tuple1.setHdr((short) 3, getTupleAttrTypes(1), getTupleStringShorts(1));
                    btf.insert(new StringKey(tuple1.getStrFld(1)), ridBIndex);
                }

               /* Scan scanForBIntIndex = hfnew.openScan();
                RID ridBIntIndex = new RID();
                Tuple tuple2;
                while ((tuple2 = scanForBIntIndex.getNext(ridBIntIndex)) != null) {
                    tuple2.setHdr((short) 3, getTupleAttrTypes(1), getTupleStringShorts(1));
                    itf.insert(new intervalKey(tuple2.getIntervalFld(2)), ridBIntIndex);
                }*/

                fs.close();
                fscan.close();
                scanForBTagIndex.closescan();
                //scanForBIntIndex.closescan();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (hf != null) {
            try {
                hf.deleteFile();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return hfnew;
    }
}
