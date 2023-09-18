package bufmgr;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/*
 * The buffer manager reads disk pages into a mains memory page as needed. The
 * collection of main memory pages (called frames) used by the buffer manager
 * for this purpose is called the buffer pool. This is just an array of Page
 * objects.
 */
public class BufMgr implements GlobalConst 
{

    /* Actual pool of pages */
    protected Page[] bufpool;

    /* Array of frame descriptors*/
    protected FrameDesc[] frametab;

    /* Maps current page numbers to frames*/
    protected HashMap<Integer, FrameDesc> pagemap;

    /* The replacement policy  */
    protected Replacer replacer;

    // BHR variables
    protected int totPageHits;
    protected int totPageRequests;
    protected int pageLoadHits;
    protected int pageLoadRequests;
    protected int uniquePageLoads = 0;
    protected int pageFaults = 0;
    
    // BHR Values - The number of hits divided by the number of requests
    protected double aggregateBHR = 0;
    protected double pageLoadBHR = -1;
    protected final int maxPages = 100;

    // Array that keeps track of information about pages
    // [0]PageNo, [1]Loads, [2]Hits, [3]Victim count
    protected int[][] pageRefCount = new int[1000][4]; 

    // The number of pages you want to print out the information about
    protected int kTopPages = 50;

    /**
     * Constructs a buffer mamanger with the given settings.
     * @param numbufs number of buffers in the buffer pool
     **/
    public BufMgr(int numbufs) 
    {   
        numbufs = 15;
        // Initialize bufferpool and frametable
        bufpool = new Page[numbufs];
        frametab = new FrameDesc[numbufs];
        for(int i = 0; i < frametab.length; i++)
        {
            bufpool[i] = new Page();
            frametab[i] = new FrameDesc(i);
        }

        // Initialize first pages to -1, since they arent used in calculations
        for(int i = 0; i < 9; i++)
            pageRefCount[i][0] = -1;
    
        // Initializing page map and replacer here. 
        pagemap = new HashMap<Integer, FrameDesc>(numbufs);
        replacer = new Clock(this);  
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
        // Initialize BHR variables
        totPageHits = 0;
        totPageRequests = 0;
        pageLoadHits = 0;
        pageLoadRequests = 0;

        //Allocating set of new pages on disk using run size. 8/22/2023
        PageId firstpgid = Minibase.DiskManager.allocate_page(run_size);
        
        try 
        {
            //pin the first page using pinpage() function using the id of firstpage, page firstpg and skipread = PIN_MEMCPY(true)
            pinPage(firstpgid, firstpg, PIN_MEMCPY);
        }
        
        catch (Exception e) 
        {
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
        
        //return the page id of the first page
        return firstpgid; 
    }
  
  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
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

        // Increment the number of total hits in the page ref
        if(tempfd != null && tempfd.pageno.pid > 8 )
        {
            pageRefCount[tempfd.pageno.pid][0] = tempfd.pageno.pid;
            pageRefCount[tempfd.pageno.pid][2] = pageRefCount[tempfd.pageno.pid][2] + 1;
        }

        // If the page is in the pool ...
        if(tempfd != null)
	    {
            //if the page is in the pool and already pinned then by using PIN_MEMCPY(true) throws an exception "Page pinned PIN_MEMCPY not allowed" 
            if(skipRead)
                throw new IllegalArgumentException("Page pinned so PIN_MEMCPY not allowed");
            
            else
            {
                //else the page is in the pool and has not been pinned so incrementing the pincount and setting Policy status to pinned
                tempfd.pincnt++;
                tempfd.state = Clock.PINNED;
                page.setPage(bufpool[tempfd.index]);
                
                // increment number of hits in buffer pool
                if(tempfd.pageno.pid > 8)
                    totPageHits++;
                
                return;
            }
        }

        // If the page is not in the pool ...
        else
        {   
            // choose a page in the pool to evict
            int i = replacer.pickVictim();
          
            // if buffer pool is full throws an Exception("Buffer pool exceeded")
            if(i < 0)
                throw new IllegalStateException("Buffer pool exceeded");
                
            tempfd = frametab[i];

            // Increment the number of page references
            if(tempfd != null && tempfd.pageno.pid > 8 )
            {
                pageRefCount[tempfd.pageno.pid][0] = tempfd.pageno.pid;
                pageRefCount[tempfd.pageno.pid][3] = pageRefCount[tempfd.pageno.pid][3] + 1;
            }
          
            // if the victim is dirty writing it to disk 
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

            // add page to buffer pool
            page.setPage(bufpool[i]);
   
            // Increment number of load requests and update page ref array
            pageLoadRequests++;
            if(tempfd.pageno.pid != -1 && tempfd.pageno.pid > 8)
            {   
                tempfd.numOfLoads++;
                pageRefCount[tempfd.pageno.pid][0] = tempfd.pageno.pid;
                pageRefCount[tempfd.pageno.pid][1] = pageRefCount[tempfd.pageno.pid][1] + 1;
            }
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
      
         // if the page is present but not pinned an exception is thrown as "page not pinned"
        if(tempfd.pincnt == 0)
            throw new IllegalArgumentException("Page not pinned");
      
        else
        {
            // unpinning the page by decrementing pincount and updating the frame descriptor and notifying replacer
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
        {
            //checking for pageid or id the pageid is the frame descriptor and the dirty status of the page
            //writing down to disk if dirty status is true and updating dirty status of page to clean
            if((pageno == null || frametab[i].pageno.pid == pageno.pid) && frametab[i].dirty)
            {
                Minibase.DiskManager.write_page(frametab[i].pageno, bufpool[i]);
                frametab[i].dirty = false;
            }
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
  
    
    public void printBhrAndRefCount()
    { 
        // Sort the 2d array first
        Arrays.sort(pageRefCount, (a, b) -> Integer.compare(b[2],a[2])); //decreasing order
        
        // Fix page reference counts an calculate BHR
        pageLoadRequests = pageLoadRequests - 1;
        aggregateBHR = ( (double)totPageHits / (double)pageLoadRequests );
        
        // pageLoadBHR = -1;
        
        //print counts:
        System.out.println("+----------------------------------------+");
        System.out.println("Aggregate Page Hits: "+ totPageHits);
        System.out.println("+----------------------------------------+");
        System.out.println("Aggregate Page Loads: "+ pageLoadRequests);
        System.out.println("+----------------------------------------+");
        System.out.print("Aggregate BHR (BHR1) : ");
        System.out.printf("%9.5f\n", aggregateBHR);
        System.out.println("+----------------------------------------+");
        System.out.println("The top pages with respect to hits are:\n");
        
        // If total pages are > 0, calculate hit ratio
        if(totPageHits > 0)
        {
            System.out.println("Page No.\tNo. of Page Loads\tNo. of Page Hits\tNo. of times Victim\tHit Ratios");
            for(int i =0; i <  kTopPages; i++)
                System.out.println(pageRefCount[i][0] + "\t\t\t" + pageRefCount[i][1] + "\t\t\t" + pageRefCount[i][2] + "\t\t\t" + pageRefCount[i][3] + "\t\t\t" + (pageRefCount[i][2]/totPageHits));
        }

        // If total pages are < 0, do not calculate hit ratios
        else
        {
            System.out.println("Page No.\tNo. of Page Loads\tNo. of Page Hits\tNo. of times Victim\tHit Ratios");
            for(int i =0; i < kTopPages; i++)
                System.out.println(pageRefCount[i][0] + "\t\t\t" + pageRefCount[i][1] + "\t\t\t" + pageRefCount[i][2] + "\t\t\t" + pageRefCount[i][3] + "\t\t\t" + 0);
        }
        System.out.println("+----------------------------------------+");
    }
} // public class BufMgr implements GlobalConst