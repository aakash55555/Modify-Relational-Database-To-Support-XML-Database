package iterator;

import global.AttrOperator;
import global.AttrType;

/**
 * This clas will hold single select condition
 * It is an element of linked list which is logically
 * connected by OR operators.
 */

public class CondExpr {

    /**
     * Operator like "<"
     */
    public AttrOperator op;

    /**
     * Types of operands, Null AttrType means that operand is not a
     * literal but an attribute name
     */
    public AttrType type1;
    public AttrType type2;

    /**
     * the left operand and right operand
     */
    public Operand operand1;
    public Operand operand2;

    /**
     * Pointer to the next element in linked list
     */
    public CondExpr next;

    /**
     * flag for range based query
     */
    public int flag;

    /**
     * set 0 for AD
     * set 1 for PC
     */
    public int relationType;

    /**
     * constructor
     */
    public CondExpr() {

        operand1 = new Operand();
        operand2 = new Operand();

        operand1.integer = 0;
        operand2.integer = 0;

        next = null;

        operand1.intervalType = null;
        operand2.intervalType = null;

        flag = -1;
        relationType = -1;
    }
}

