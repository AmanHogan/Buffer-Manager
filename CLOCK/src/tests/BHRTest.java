package tests;

import global.Convert;
import global.Minibase;
import global.Page;
import global.PageId;
import java.util.*;

/**
 * Test suite for the bufmgr layer.
 */
class BHRTest extends TestDriver {

  /** The display name of the test suite. */
  private static final String TEST_NAME = "Tests for BHR and reference count";

  /**
   * Test application entry point; runs all tests.
   */
   private static final int MAX_PIN_COUNT = 10;
   private static final int MAX_SEQUENCE = 1000;
   private static final int MAX_ITERATIONS = 3;
   private static final int BUF_SIZE_MULTIPLIER = 5; //disk pages allocated
   private static final int OUTER_PAGE_FRACTION = 10; //10% bufs for outer relation
   private static final double FRACTION_FOR_RR = 0.3;   //30% to be added in each round
   
  public static void main(String argv[]) {

    // create a clean Minibase instance

    // run all the test cases
    System.out.println("\n" + "Running " + TEST_NAME + "...");
    BHRTest bhr ;
    boolean status;
    
    bhr = new BHRTest();
    bhr.create_minibase();
    status = PASS;
    status &= bhr.testMRU();
    
    bhr = new BHRTest();
    bhr.create_minibase();
    status = PASS;
    status &= bhr.testRR();
    
    bhr = new BHRTest();
    bhr.create_minibase();
    status = PASS;
    status &= bhr.testRANDOM();
    

    // display the final results
    System.out.println();
    if (status != PASS) {
      System.out.println("Error(s) encountered during " + TEST_NAME + ".");
    } else {
      System.out.println("All " + TEST_NAME + " completed successfully!");
    }

  } // public static void main (String argv[])

  /**
   * 
   */
protected boolean testMRU() {

    System.out.print("\n  Starting Test MRU (nested loop join) \n with 1 buf for outer relation and rest for inner relation\n");
    System.out.print("# bufferes for inner relation is than inner relation pages\n");
    System.out.print("\n This should do better for MRU than LRU and others \n");

    boolean status = PASS;
      
    int numBufPages = Minibase.BufferManager.getNumUnpinned(); //buf frames
    int numDiskPages = numBufPages*BUF_SIZE_MULTIPLIER;
    System.out.print("numBufPages: buf and disk: " + numBufPages + "  and " + numDiskPages+"\n");
    
    Page pg = new Page();
    PageId pid;
    PageId pid2;
    PageId firstPid = new PageId();
    System.out.println("  - Allocate all pages\n"); //allocates 8 (used by MiniBase)  + numDiskPages!
    try {
      firstPid = Minibase.BufferManager.newPage(pg, numDiskPages);
      //System.out.print("First page id allocated: " + firstPid.pid + "\n"); //print the first page number
    } catch (Exception e) {
      System.err.print("*** Could not allocate " + numDiskPages);
      System.err.print(" new pages in the database.\n");
      e.printStackTrace();
      return false;
    }

    
    // unpin and free that first page... to simplify our loop
    try {
      Minibase.BufferManager.unpinPage(firstPid, UNPIN_CLEAN);
    } catch (Exception e) {
      System.err.print("*** Could not unpin the first new page.\n");
      e.printStackTrace();
      status = FAIL;
    }
    // remove the page from the buffer
    try {
      Minibase.BufferManager.freePage(firstPid);
    } catch (Exception e) {
      System.err.print("*** Could not free/remove the first new page.\n");
      e.printStackTrace();
      status = FAIL;
    }

    numBufPages = Minibase.BufferManager.getNumUnpinned(); //buf frames
    System.out.print("numPages: buf and disk: " + numBufPages + "  and " + numDiskPages+"\n");
    
    // now nothing is pinned AND no pages in buffer; numBufPages in buffers and numDiskPages on disk
    
    pid = new PageId();
    pid2 = new PageId();
    
    int outerRelPages = numDiskPages/OUTER_PAGE_FRACTION; //pages in the outer relation
    int innerRelPages = numDiskPages - outerRelPages;     // pages in the inner relation
    int bufsForInnerR = NUMBUF-1;  //only one buffer is used for outer relation; 8 is used by MiniMase (allocated separately) and 1 outer
    
    System.out.print("  - starting nested loop join with bufPages: " + NUMBUF + "  Outer pages: " + outerRelPages + " inner pages: " + (numDiskPages-outerRelPages) + "\n");
    int innerBufCount = 0;
    PageId innerPid[];
    innerPid = new PageId[bufsForInnerR];
    for (int j=0; j < bufsForInnerR; j++) {innerPid[j] = new PageId();}
    for (pid.pid = firstPid.pid; status == PASS
        && pid.pid < firstPid.pid+outerRelPages; pid.pid = pid.pid + 1) {
      //System.out.print("\n Outer page: " + pid.pid + "; Inner pages: ");
      try {
        Minibase.BufferManager.pinPage(pid, pg, PIN_DISKIO);
      } catch (Exception e) {
        status = FAIL;
        System.err.print("*** Could not pin new page " + pid.pid + "\n");
        e.printStackTrace();
      }
        for (pid2.pid = firstPid.pid + outerRelPages; status == PASS
        && pid2.pid < firstPid.pid+numDiskPages; pid2.pid = pid2.pid + 1) {
            //System.out.print(pid2.pid + ", ");
            //System.out.println(innerBufCount + " % " + bufsForInnerR);
            try {
            Minibase.BufferManager.pinPage(pid2, pg, PIN_DISKIO);
            innerPid[innerBufCount].pid = pid2.pid;
            innerBufCount = innerBufCount+1; 
          } catch (Exception e) {
            status = FAIL;
            System.err.print("**** Could not pin new page " + pid2.pid + "\n");
            e.printStackTrace();
        }
        
        if ((innerBufCount+1) % bufsForInnerR == 0){
            for(int i = 0; status == PASS && i < innerBufCount; i = i+1) {
                try {
                    //System.out.print("unpin " + innerPid[i].pid + ", ");
                    Minibase.BufferManager.unpinPage(innerPid[i], UNPIN_CLEAN);
                } catch (Exception e) {
                status = FAIL;
                System.err.print("**** Could not unpin dirty page " + innerPid[i].pid + "\n");
                e.printStackTrace();
                }   
            }
        innerBufCount =0;
        }
      }
      //unpin the outer rel page sitting in the buffer
      if (status == PASS) {
          try {
            Minibase.BufferManager.unpinPage(pid, UNPIN_CLEAN);
          } catch (Exception e) {
            status = FAIL;
            System.err
                .print("*** Could not unpin dirty page " + pid.pid + "\n");
            e.printStackTrace();
          }
        }
  }
   //unpin the last few pages that did NOt get unpinned inside the loop
   for(int i = 0; status == PASS && i < innerBufCount; i = i+1) {
                try {
                    //System.out.print("unpin " + innerPid[i].pid + ", ");
                    Minibase.BufferManager.unpinPage(innerPid[i], UNPIN_CLEAN
                    );
                } catch (Exception e) {
                status = FAIL;
                System.err.print("**** Could not unpin dirty page " + innerPid[i].pid + "\n");
                e.printStackTrace();
                }   
            }
   
   if (status == PASS){
     
    //invoke to print results
    Minibase.BufferManager.printBhrAndRefCount();
    System.out.println("++++++++++++++++++++++++++==============");
    System.out.print("  Test 1 completed successfully.\n");
    System.out.print("\n  +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
    }
    return status;


  } // protected boolean testMRU ()
  
// --------------------- end of testMRU ------------------------------
    
  protected boolean testRR() {  // testing all policies thru random sequence of page 
                                // (multiple) accesses and check for page faults and BHR
  
    System.out.print("\n  Starting Test RR (a) and (b) using Round robin way of pinning and unpinning pages\n");
    System.out.print("\n");
    System.out.print("\n This should do differently for different replacement policies \n");

    boolean status = PASS;
    int numBufPages = Minibase.BufferManager.getNumUnpinned(); //buf frames
    int numDiskPages = numBufPages*BUF_SIZE_MULTIPLIER;
    System.out.print("numBufPages: buf and disk: " + numBufPages + "  and " + numDiskPages+"\n");
    
    Page pg = new Page();
    PageId firstPid = new PageId();
    System.out.println("  - Allocate all pages\n"); //allocates 8 (used by MiniBase)  + numDiskPages!
    try {
      firstPid = Minibase.BufferManager.newPage(pg, numDiskPages);
      System.out.print("First page id allocated: " + firstPid.pid + "\n"); //print the first page number
    } catch (Exception e) {
      System.err.print("*** Could not allocate " + numDiskPages);
      System.err.print(" new pages in the database.\n");
      e.printStackTrace();
      return false;
    }

    
    // unpin and free that first page... to simplify our loop
    try {
      Minibase.BufferManager.unpinPage(firstPid, UNPIN_CLEAN);
    } catch (Exception e) {
      System.err.print("*** Could not unpin the first new page.\n");
      e.printStackTrace();
      status = FAIL;
    }
    // remove the page from the buffer
    try {
      Minibase.BufferManager.freePage(firstPid);
    } catch (Exception e) {
      System.err.print("*** Could not free/remove the first new page.\n");
      e.printStackTrace();
      status = FAIL;
    }
    numBufPages = Minibase.BufferManager.getNumUnpinned(); //buf frames
    System.out.print("numBufPages: buf and disk: " + numBufPages + "  and " + numDiskPages+"\n");
    
    // now nothing is pinned AND no pages in buffer; numBufPages in buffers and numDiskPages on disk
    
    PageId pid;
    PageId pid2;
    pid = new PageId();
    pid2 = new PageId();
    
    pid = new PageId();
    
    //just do round robin and see whether it makes a diff between policies
    System.out.println("entering round robin stage ...\n");
    int rpid;
    int rPage;
    Random randomPage = new Random();
    Random iter = new Random();
    Random pin = new Random();
    iter.setSeed(1347);  //prime number
    pin.setSeed(19381);
    int it = MAX_ITERATIONS;
    for (int j = 1; j <= it; j++){
    
    for ( int i=0; i < numDiskPages; i++){
        
       pid.pid = firstPid.pid + i;
        
        for (int k = 1; k <= pin.nextInt(MAX_PIN_COUNT)+1; k++){
        try {
            Minibase.BufferManager.pinPage(pid, pg, PIN_DISKIO);
                        
            } catch (Exception e) {
            status = FAIL;
            System.err.print("*** Could not pin new page " + pid.pid + "\n");
            e.printStackTrace();
            }             
                
        // Copy the page number + 99999 onto each page. It seems
        // unlikely that this bit pattern would show up there by
        // coincidence.
        int data = pid.pid + 99999;
        Convert.setIntValue(data, 0, pg.getData());    
        
            try {
                Minibase.BufferManager.unpinPage(pid, UNPIN_DIRTY);
                } catch (Exception e) {
                    status = FAIL;
                    System.err.print("*** Could not unpin dirty page " + pid.pid + "\n");
                    e.printStackTrace();
                }
            }  
      
    }
    
    }
    if (status == PASS){   
        //invoke print 
        System.out.println("  Test RR (a): Round Robin after "+it+" iterations");  
        Minibase.BufferManager.printBhrAndRefCount();
         System.out.print("\n++++++++++++++++++++++++++==============\n");
        }       

  // randomly load pages, pin them and unpin them a large number of times
  // load pages in some order to generate page faults
    Random seq = new Random();
    seq.setSeed(999331); //another prime num;
    pin.setSeed(iter.nextInt(13447)+1);
    randomPage.setSeed(13); //another prime num; 
    
    for (int j = 1; j <= MAX_ITERATIONS; j++){
    
   
    for ( int i=1; i <= seq.nextInt(MAX_SEQUENCE)+1; i++){
    
        rpid = randomPage.nextInt(numDiskPages)+1;
        pid.pid = firstPid.pid + rpid;
        
        for (int k = 1; k <= pin.nextInt(MAX_PIN_COUNT)+1; k++){
        try {
            Minibase.BufferManager.pinPage(pid, pg, PIN_DISKIO);
                        
            } catch (Exception e) {
            status = FAIL;
            System.err.print("*** Could not pin new page " + pid.pid + "\n");
            e.printStackTrace();
            }             
                
        // Copy the page number + 99999 onto each page. It seems
        // unlikely that this bit pattern would show up there by
        // coincidence.
        int data = pid.pid + 99999;
        Convert.setIntValue(data, 0, pg.getData());    
        
            try {
                Minibase.BufferManager.unpinPage(pid, UNPIN_DIRTY);
                } catch (Exception e) {
                    status = FAIL;
                    System.err.print("*** Could not unpin dirty page " + pid.pid + "\n");
                    e.printStackTrace();
                }
            }  
      
    }
    if (status == PASS){   
        //invoke print 
        //System.out.println("  Test RR (b) after " + it " Iterations");  
        //Minibase.BufferManager.printBhrAndRefCount();
         //System.out.print("\n++++++++++++++++++++++++++==============\n");
        }       
    }
     
   if (status == PASS){
    System.out.println("  Test RR (b) after " + it + " Iterations");  
        Minibase.BufferManager.printBhrAndRefCount();
        System.out.println("++++++++++++++++++++++++++==============");
    System.out.println("  Test RR completed successfully.");  
    System.out.println("++++++++++++++++++++++++++==============");
    System.out.println(" compare page faults for each policy");
    System.out.print("++++++++++++++++++++++++++==============");
    }
    return status;
   
  } // protected boolean testRR ()
//+++++++++++++++++++++++ end of TestRR ++++++++++++++++++++++++++++

  protected boolean testRANDOM() {

     System.out.print("\n  started Test Random ...");

    // we choose this number to ensure that at least one page
    // will have to be written during this test
    boolean status = PASS;
    int numBufPages = Minibase.BufferManager.getNumUnpinned();
    int numDiskPages = numBufPages*BUF_SIZE_MULTIPLIER;
    System.out.print("numBufPages: buf and disk: " + numBufPages + "  --- " + numDiskPages+"\n");

    Page pg = new Page();
    PageId pid, rPid;
    PageId lastPid;
    PageId firstPid = new PageId();
    ArrayList <Integer> pinnedPages = new ArrayList<Integer>();
    HashMap <Integer, Integer> pinCount = new HashMap<Integer, Integer>();
    System.out.println("  - Allocate a bunch of new pages");
    try {
      firstPid = Minibase.BufferManager.newPage(pg, numBufPages*3);
    } catch (Exception e) {
      System.err.print("*** Could not allocate " + numBufPages*3);
      System.err.print(" new pages in the database.\n");
      e.printStackTrace();
      return false;
    }
System.out.print("\n  Random pinning and unpinning of pages ");
    // unpin that first page... to simplify our loop
    try {
      Minibase.BufferManager.unpinPage(firstPid, UNPIN_CLEAN);
    } catch (Exception e) {
      System.err.print("*** Could not unpin the first new page.\n");
      e.printStackTrace();
      status = FAIL;
    }
    Random random = new Random();
    random.setSeed(1000);
    Random randomU = new Random();
    randomU.setSeed(100);

    Random randomR = new Random();
    randomR.setSeed(4444);
    System.out.print("  - Write something on each one\n");
    pid = new PageId();
    rPid = new PageId();
    lastPid = new PageId();

    for (pid.pid = firstPid.pid, lastPid.pid = pid.pid + (numBufPages*3/4); status == PASS
        && pid.pid < lastPid.pid; pid.pid = pid.pid + 1) {
      rPid.pid=random.nextInt(lastPid.pid);
      if (rPid.pid < firstPid.pid) rPid.pid = firstPid.pid; //added for fall 2022 by Sc
      try {
        Minibase.BufferManager.pinPage(rPid, pg, PIN_DISKIO);
        
      } catch (Exception e) {
        status = FAIL;
        System.err.print("*** Could not pin new page " + rPid.pid + "\n");
        e.printStackTrace();
      }
      if(pinCount.get(rPid.pid) == null){
      pinnedPages.add(rPid.pid);
        pinCount.put(rPid.pid, 1);
      }
      else{
        pinCount.put(rPid.pid, pinCount.get(rPid.pid)+1);
      }
    }

    for(int i=0; i<300; i++){
      rPid.pid = pinnedPages.get(randomR.nextInt(pinnedPages.size()));
      try {
        Minibase.BufferManager.pinPage(rPid, pg, PIN_DISKIO);
        
        pinCount.put(rPid.pid, pinCount.get(rPid.pid)+1);
      } catch (Exception e) {
        status = FAIL;
        System.err.print("*** Could not pin new page " + rPid.pid + "\n");
        e.printStackTrace();
      }
    }

    for (pid.pid = firstPid.pid, lastPid.pid = pid.pid+numBufPages*3; pid.pid < lastPid.pid; pid.pid = pid.pid + 1){
       int change = randomU.nextInt(pinnedPages.size());
       rPid.pid=pinnedPages.get(change);
       // System.out.println("change "+change+" rPid "+rPid.pid+" pinCount val "+pinCount.get(rPid.pid));

        if (status == PASS) {
          
            try {
            Minibase.BufferManager.unpinPage(rPid, UNPIN_CLEAN);
            } catch (Exception e) {
            status = FAIL;
            System.err
                .print("*** Could not unpin dirty page " + rPid.pid + "\n");
            e.printStackTrace();
          }
          if (pinCount.get(rPid.pid) == 1 && pinCount.get(rPid.pid)!= null){
              pinnedPages.remove(change);
              pinCount.remove(rPid.pid);
            }
            else
              pinCount.put(rPid.pid, pinCount.get(rPid.pid)-1);
            
          
        }
    }

      
if (status == PASS){
     
    //involke print and cleanup
    Minibase.BufferManager.printBhrAndRefCount();
    System.out.println("++++++++++++++++++++++++++==============");
    System.out.println("  Test RANDOM completed successfully.\n");
    System.out.println("++++++++++++++++++++++++++==============");
    //System.out.println("max references vary from 16 to 12, high BHR1 and BHR2 values");
    }

    return status;

  } // protected boolean testRANDOM ()
  
//+++++++++++++++++++++++++++++++++ end of testRANDOM +++++++++++++++++++++++++

} // class BHRTest extends TestDriver
