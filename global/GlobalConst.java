package global;

public interface GlobalConst {

    public static final int MINIBASE_MAXARRSIZE = 50;
    public static final int NUMBUF = 50;
    public static final String RP_CLOCK = "Clock";
    /**
     * Size of page.
     */
    public static final int MINIBASE_PAGESIZE = 2048;           // in bytes
    public static final int MINIBASE_NUMBER_OF_PAGES = 50000;
    /**
     * Size of each frame.
     */
    public static final int MINIBASE_BUFFER_POOL_SIZE = 8192 * 3;   // in Frames

    public static final int MAX_SPACE = 16384;   // in Frames

    /**
     * in Pages => the DBMS Manager tells the DB how much disk
     * space is available for the database.
     */
    public static final int MINIBASE_DB_SIZE = 200000;
    public static final int MINIBASE_MAX_TRANSACTIONS = 100;
    public static final int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;

    /**
     * also the name of a relation
     */
    public static final int MAXFILENAME = 15;
    public static final int MAXINDEXNAME = 40;
    public static final int MAXATTRNAME = 15;
    public static final int MAX_NAME = 50;

    public static final int INVALID_PAGE = -1;
}
