package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/* revised slightly by sharma on 8/22/2023 */

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager reads disk pages into a mains memory page as needed. The
 * collection of main memory pages (called frames) used by the buffer manager
 * for this purpose is called the buffer pool. This is just an array of Page
 * objects. The buffer manager is used by access methods, heap files, and
 * relational operators to read, write, allocate, and de-allocate pages.
 * policy class name has to be changed in the constructior using name of the 
 * class you have implementaed
 */
public class BufMgr implements GlobalConst {

    /** Actual pool of pages (can be viewed as an array of byte arrays). */
    protected Page[] bufpool;

    /** Array of descriptors, each containing the pin count, dirty status, etc\
	. */
    protected FrameDesc[] frametab;

    /** Maps current page numbers to frames; used for efficient lookups. */
    protected HashMap<Integer, FrameDesc> pagemap;

    /** The replacement policy to use. */
    protected Replacer replacer;
    
//-------------------------------------------------------------
    /** 
        you may add HERE variables NEEDED for calculating hit ratios 
        a public void printBhrAndRefCount() has been provided at the bottom 
        which is called from test modules. To use that
            either use the same variable names OR
            modify the print method with variables you have used
    */

    // Init BHR variables
    protected int totPageHits;
    protected int totPageRequests;
    protected int pageLoadHits;
    protected int pageLoadRequests;
    protected int uniquePageLoads = 0;
    protected int pageFaults = 0;
    protected float aggregateBHR = 0;
    protected float pageLoadBHR = -1;
    protected int[] pageRefCount = new int[100];
    protected final int maxPages = 100;

    
//----------------------------List<E>-----------------------------



  /**
   * Constructs a buffer mamanger with the given settings.
   * 
   * @param numbufs number of buffers in the buffer pool
   */
  public BufMgr(int numbufs) 
  {   
	  //initializing buffer pool and frame table 
      numbufs = 15;
	  bufpool = new Page[numbufs];
      frametab = new FrameDesc[numbufs];

 
      
      for(int i = 0; i < frametab.length; i++)
      {
              bufpool[i] = new Page();
    	  	  frametab[i] = new FrameDesc(i);
      }
      
      //initializing page map and replacer here. 
      pagemap = new HashMap<Integer, FrameDesc>(numbufs);
      replacer = new Clock(this);   // change Policy to replacement class name
  }

  /**
   * Allocates a set of new pages, and pins the first one in an appropriate
   * frame in the buffer pool.
   * 
   * @param firstpg holds the contents of the first page
   * @param run_size number of pages to allocate
   * @return page id of the first new page
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */

    public PageId newPage(Page firstpg, int run_size)
    {
        // Initializing variables for page hit and page load count
        totPageHits = 0;
        totPageRequests = 0;
        pageLoadHits = 0;
        pageLoadRequests = 0;

        // TODO: Initialize arrays for printing top k Page Reference
      
	  //Allocating set of new pages on disk using run size. 8/22/2023
	  PageId firstpgid = Minibase.DiskManager.allocate_page(run_size);
	  try {
		  //pin the first page using pinpage() function using the id of firstpage, page firstpg and skipread = PIN_MEMCPY(true)
		  pinPage(firstpgid, firstpg, PIN_MEMCPY);

          }
          catch (Exception e) {
        	  //pinning failed so deallocating the pages from disk
        	  for(int i=0; i < run_size; i++)
        	  {   
        		  firstpgid.pid += i;
 	  	          Minibase.DiskManager.deallocate_page(firstpgid);
                
        	  }
        	  return null;
      }
	  
	  //notifying replacer
      replacer.newPage(pagemap.get(Integer.valueOf(firstpgid.pid)));
      
      // you may have to add some BHR code here
      
      
      //return the page id of the first page
      return firstpgid; 
  }
  
  /**
   * revised  sightly to fix something 8/22/2022
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) 
  {  
	  //the frame descriptor as the page is in the buffer pool 
	  FrameDesc tempfd = pagemap.get(Integer.valueOf(pageno.pid));
	  
      //the page is in the pool so it cannot be null.
      if(tempfd != null)
      {
    	  //checking the pin count of frame descriptor
          if(tempfd.pincnt > 0)
              throw new IllegalArgumentException("Page currently pinned");
          
            //remove page as it's pin count is 0, remove the page, updating its pin count and dirty status, the policy and notifying replacer.
          pagemap.remove(Integer.valueOf(pageno.pid));
          tempfd.pageno.pid = INVALID_PAGEID;
          tempfd.pincnt = 0;
          tempfd.dirty = false;
          tempfd.state = Clock.AVAILABLE;
          replacer.freePage(tempfd);

          
      }
      //deallocate the page from disk 
      Minibase.DiskManager.deallocate_page(pageno);
  }

  /**
   * Pins a disk page into the buffer pool. If the page is already pinned, this
   * simply increments the pin count. Otherwise, this selects another page in
   * the pool to replace, flushing it to disk if dirty.
   * 
   * @param pageno identifies the page to pin
   * @param page holds contents of the page, either an input or output param
   * @param skipRead PIN_MEMCPY (replace in pool); PIN_DISKIO (read the page in)
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
    public void pinPage(PageId pageno, Page page, boolean skipRead) 
    {  
        //the frame descriptor as the page is in the buffer pool 
	    FrameDesc tempfd = pagemap.get(Integer.valueOf(pageno.pid));
	  
        if(tempfd != null)
	    {
            
          
            
            //if the page is in the pool and already pinned then by using PIN_MEMCPY(true) throws an exception "Page pinned PIN_MEMCPY not allowed" 
            if(skipRead)
            {
                throw new IllegalArgumentException("Page pinned so PIN_MEMCPY not allowed");
            }
                
          
            else
            {
                //else the page is in the pool and has not been pinned so incrementing the pincount and setting Policy status to pinned
                tempfd.pincnt++;
                tempfd.state = Clock.PINNED;
                page.setPage(bufpool[tempfd.index]);
                
                //some BHR code may go here
                if(tempfd.pageno.pid > 8)
                {
                    totPageHits++;
                    tempfd.numOfHits++;
                    pageRefCount[tempfd.pageno.pid] = tempfd.numOfHits;
                }
                
                return;
            }
        }

        else
        {   
            //as the page is not in pool choosing a victim
            int i = replacer.pickVictim();

          
            //if buffer pool is full throws an Exception("Buffer pool exceeded")
            if(i < 0)
                throw new IllegalStateException("Buffer pool exceeded");
                
            tempfd = frametab[i];

            
          
            //if the victim is dirty writing it to disk 
            if(tempfd.pageno.pid != -1)
            {
                

                pagemap.remove(Integer.valueOf(tempfd.pageno.pid));
                if(tempfd.dirty)
                    Minibase.DiskManager.write_page(tempfd.pageno, bufpool[i]);
            }

            //reading the page from disk to the page given and pinning it. 
            if(skipRead)
                bufpool[i].copyPage(page);
            else
                Minibase.DiskManager.read_page(pageno, bufpool[i]);
            
            page.setPage(bufpool[i]);
            tempfd.numOfLoads++;
            pageLoadRequests++;
            
            
	    }

        //updating frame descriptor and notifying to replacer
        
        tempfd.pageno.pid = pageno.pid;
        tempfd.pincnt = 1;
        tempfd.dirty = false;
        pagemap.put(Integer.valueOf(pageno.pid), tempfd);
        tempfd.state =Clock.PINNED;
        replacer.pinPage(tempfd);
        
        
    }

  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherrwise
   * @throws IllegalArgumentException if the page is not present or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) 
  {  
	  //the frame descriptor as the page is in the buffer pool 
	  FrameDesc tempfd = pagemap.get(Integer.valueOf(pageno.pid));
	  
	  //if page is not present an exception is thrown as "Page not present"
      if(tempfd == null)
          throw new IllegalArgumentException("Page not present");
      
      //if the page is present but not pinned an exception is thrown as "page not pinned"
      if(tempfd.pincnt == 0)
      {
          throw new IllegalArgumentException("Page not pinned");
      } 
      else
      {
    	  //unpinning the page by decrementing pincount and updating the frame descriptor and notifying replacer
          tempfd.pincnt--;
          tempfd.dirty = dirty;
          if(tempfd.pincnt== 0)
          tempfd.state = Clock.REFERENCED;
          replacer.unpinPage(tempfd);
          return;
      }
  }

  /**
   * Immediately writes a page in the buffer pool to disk, if dirty.
   */
  public void flushPage(PageId pageno) 
  {  
	  for(int i = 0; i < frametab.length; i++)
	 	  //checking for pageid or id the pageid is the frame descriptor and the dirty status of the page
          if((pageno == null || frametab[i].pageno.pid == pageno.pid) && frametab[i].dirty)
          {
        	  //writing down to disk if dirty status is true and updating dirty status of page to clean
              Minibase.DiskManager.write_page(frametab[i].pageno, bufpool[i]);
              frametab[i].dirty = false;
          }
  }

  /**
   * Immediately writes all dirty pages in the buffer pool to disk.
   */
  public void flushAllPages() 
  {
	  for(int i=0; i<frametab.length; i++) 
		  flushPage(frametab[i].pageno);
  }

  /**
   * Gets the total number of buffer frames.
   */
  public int getNumBuffers() 
  {
	  return bufpool.length;
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() 
  {
	  int numUnpinned = 0;
	  for(int i=0; i<frametab.length; i++) 
	  {
		  if(frametab[i].pincnt == 0)
			  numUnpinned++;
	  }
	  return numUnpinned;
  }
  
/*// Function to sort by column 
    public static void sortbyColumn(int arr[][], final int col) 
    { 
        // Using built-in sort function Arrays.sort 
        Arrays.sort(arr, new Comparator<int[]>() { 
            
          @Override              
          // Compare values according to columns 
          public int compare(final int[] entry1,  
                             final int[] entry2) { 
  
            // To sort in descending order revert  
            // the '>' Operator 
            if (entry1[col] > entry2[col]) 
                return -1; 
            else
                return 1; 
          } 
        });  // End of function call sort(). 
    }*/ 
    
    public void printBhrAndRefCount(){ 
    
    
    //print counts:
    System.out.println("+----------------------------------------+");
    System.out.println("Aggregate Page Hits: "+totPageHits);
    System.out.println("+----------------------------------------+");
    //System.out.println("totPageRequests: "+totPageRequests);
    //System.out.println("pageLoadHits: "+pageLoadHits);
    System.out.println("Aggregate Page Loads: "+pageLoadRequests);
    System.out.println("+----------------------------------------+");
    System.out.println("Unique page loads: "+uniquePageLoads);
    System.out.println("Page faults (policy dependent): "+pageFaults);
    System.out.println("+----------------------------------------+");
    
    System.out.println("+----------------------------------------+");
    System.out.println("Page No.\tNo. of Page Hits");

    for (int i = 0; i < pageRefCount.length; i++) {
        System.out.println(i + "\t\t" + + pageRefCount[i]);
    }

    System.out.println("+----------------------------------------+");
    //compute BHR1 and BHR2 
    aggregateBHR = -1; //replce -1 with the formula   
    pageLoadBHR = -1;  //replce -1 with the formula  
  
    System.out.print("Aggregate BHR (BHR1): ");
    System.out.printf("%9.5f\n", aggregateBHR);
    System.out.print("Load-based BHR (BHR2): ");
    System.out.printf("%9.5f\n", pageLoadBHR);
    System.out.println("+----------------------------------------+");


    
       
/*    //before sorting, need to compare the LAST refcounts and fix it
    for (int i = 0; i < pageRefCount.length ; i++) {
        if (pageRefCount[i][0] > pageRefCount[i][1]) pageRefCount[i][1] = pageRefCount[i][0];
        pageRefCount[i][0] = 0;
    }
    //Sort and print top k page references here. done by this code
    sortbyColumn(pageRefCount, 1);
    
    System.out.println("The top k (10) referenced pages are:");
    System.out.println("       Page No.\t\tNo. of references");
       
    for (int i = 0; i < 10 ; i++)    
      System.out.println("\t"+pageRefCount[i][2]+"\t\t"+pageRefCount[i][1]);
    
    System.out.println("+----------------------------------------+");
    //* System.out.println("pageRefCount.length: "+pageRefCount.length);
    // *for (int i = 0; i < pageRefCount.length ; i++)    
      // *System.out.println("\t"+pageRefCount[i][2]+"\t\t"+pageRefCount[i][1]+"\t\t"+pageRefCount[i][0]);*/
}

} // public class BufMgr implements GlobalConst
