package loaddb;

import iterator.Iterator;

public class OpIterator {

    private Iterator iterator;
    private String order;
    private String operationTag;
    private String queryPlan;

    public OpIterator(Iterator iterator, String order, String operationTag, String queryPlan) {
        this.iterator = iterator;
        this.order = order;
        this.operationTag = operationTag;
        this.queryPlan = queryPlan;
    }

    public Iterator getIterator() {
        return iterator;
    }

    public String getOrder() {
        return order;
    }

    public String getOperationTag() {
        return operationTag;
    }

    public String getQueryPlan() {
        return queryPlan;
    }
}
