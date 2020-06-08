# Database Management System Implementation

List of Tasks:

The following is a list of tasks that you need to perform for the this phase of the project:

• interval data type declaration
• sort, nestedloop, and sortmerge with interval data types
• XML data to interval conversion and storage in the DB

Note that getting these working may involve other changes to other modules not described below.
• Set the minibase pagesize to 256 bytes;
public static final int MINIBASE_PAGESIZE = 256;
• Define a new data type ”intervaltype” which consists of 2 integers, start and end of the interval.

filename: intervaltype.java

=========================================================

package global;

public class intervaltype {
int s ;
int e ;

public void assign(int a, int b) {
this.s = a;
this.e = b;
}
}

If you need to define a minimum and a maximum value for the intervaltype, you can use
				{−100000,−100000}

as the minimum and
          			{100000, 100000}

as the maximum.

• Modify attribute type definitions to include a new attribute type called ”attrInterval”, in addition to the
attribute types already defined in MiniBase.

public static final int attrInterval = 5;

• Modify tuple field get and set methods to include

getIntervalFld(int fldNo)
convert this field into intervaltype
setIntervalFld(int fldNo, intervaltype val)
set this field to intervaltype value

• Modify page get and set methods to include

getIntervalValue(int position, byte[] data)
read from given byte array at the specified position convert
it to a intervaltype
setIntervalValue(intervaltype value, int position, byte[] data)
update a intervaltype in the given byte array at the specified
position

• Modify operand definitions to include operands of type intervaltype

public intervaltype interval;

• Modify tuple comparison methods CompareTupleWithTuple and CompareTupleWithValue, such that they
return

– 1 for containment
– 2 for enclosure
– 3 for other types of overlap
– 0 for no-overlap

if the fields that are compared are of type attrInterval.

• Modify condition expressions, CondExpr, to include an extra field flag to be used in some range based
conditions

public int flag;

If the operands are of type attrInterval, then flag will be set to a non-negative integer.

• Modify Eval to work with attributes of type attrInterval. For example,
– the operator aopGT should return true, if the first operand contains the second operand.
– the operator aopLT should return true, if the first operand is contained within the second operand.
– the operator aopEQ should return true
	∗ if flag = 0, if the two operands are equal
	∗ if flag = 1, if the two operands overlap
– the operator aopNE should return true
	∗ if flag = 0, if the two operands are not equal
	∗ if flag = 1, if the two operands do not overlap
• operators aopGE are aopLE are not defined if operands are of type attrInterval.
• Modify Sort such that if the sort attribute is of type attrInterval, then the sort function sorts all tuples
according to the start values.

Sort(AttrType[] in, short len_in, short[] str_sizes,
Iterator am, int sort_fld, TupleOrder sort_order,
int sort_fld_len, int n_pages)

• Make sure that NestedLoopsJoins and SortMerge iterators, which take CondExpr type input parameters,
work with the new definition of CondExpr.

NestedLoopsJoins(AttrType[] in1, int len_in1, short[] t1_str_sizes,
AttrType[] in2, int len_in2, short[] t2_str_sizes,
int amt_of_mem, Iterator am1, java.lang.String
relationName, CondExpr[] outFilter, CondExpr[]
rightFilter, FldSpec[] proj_list, int n_out_flds)

SortMerge(AttrType[] in1, int len_in1, short[] s1_sizes,
AttrType[] in2, int len_in2, short[] s2_sizes,
int join_col_in1, int sortFld1Len, int join_col_in2,
int sortFld2Len, int amt_of_mem,
Iterator am1, Iterator am2,
boolean in1_sorted, boolean in2_sorted,
TupleOrder order, CondExpr[] outFilter,
FldSpec[] proj_list, int n_out_flds)

• Implement a program, which given a tree-structured XML file, stores its elements in the miniBase using
an interval based representation, with the following schema

nodeT able(nodeIntLabel, nodeT ag)

For simplicity, you can treat any attributes and any plaintext content as an individual “node”, with the
upto first 5 charcaters of the plaintext content serving as the tag. For example, given an XML snippet

..
<Ref num="1" pos="SEQUENCE FROM N.A">
<Comment>STRAIN=CV. VF36</Comment>
<Comment>TISSUE=ANTHER</Comment>
<DB>MEDLINE</DB>
<MedlineID>94143497</MedlineID>
<Author>Chen R</Author>
<Author>Smith A.G</Author>
<Cite>Plant Physiol. 101:1413-1413(1993)</Cite>
</Ref>
..

you can treat this as if it were an XML document of the following format:

..
<Ref num="1" pos="SEQUENCE FROM N.A">
<num><1></1></num>
<pos><SEQUE></SEQUE></pos>
<Comment><STRAI></STRAI></Comment>
<Comment><TISSU></TISSU></Comment>
<DB><MEDLI></MEDLI></DB>
<MedlineID><94143></94143></MedlineID>
<Author><"Chen "></Chen "></Author>
<Author><Smith></Smith></Author>
<Cite><Plant></Plant></Cite>
</Ref>
..


• Implement a program which, given an interval-indexed XML database and a file containing a pattern tree
[format described below], identifies matching nodes using the query processing operators defined above.
The pattern tree format will be as follows:

m % number of nodes
"tag1" % tag of node 1 ( "*" matches all tags)
"tag2" % tag of node 2 ( "*" matches all tags)
..
"tag m" % tag of node m ( "*" matches all tags)
i j AD % node i is an ancestor of node j
..
k l PC % node k is a parent of node l
...

The output of the program are as follows:
– for each result, the nodeids (and the tags) matching the query nodes
– the number of pages accessesed to obtain all the results
For each query, implement three distinct query plans.

• Modify Minibase’s buffer manager to count the number of pages that are requested from the buffer
manager. Please see the seperate “pcounter instruction.pdf” file to see one way to achieve this.


IMPORTANT: If you need to process large amounts of data (for example to sort a file), do not use the memory.
Do everything on the disk using the tools and methods provided by minibase.

#Group Members:
Aakash Rastogi
Manoj Tiwaskar
Narsimha Reddy Sarasani
Sumit Rawat
Varun Rao Veeramaneni
Yash Jain