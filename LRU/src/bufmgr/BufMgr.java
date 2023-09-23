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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/*
 * @brief The buffer manager reads disk pages into a mains memory page as needed. The
 * collection of main memory pages (called frames) used by the buffer manager
 * for this purpose is called the buffer pool. This is just an array of Page
 * objects.
 */
public class BufMgr implements GlobalConst 
{
    /** @brief - number of buffers in buffer pool (15 and 42) */
    //int numberOfBuffers = 42;
     int numberOfBuffers = 15;

    /** Actual pool of pages */
    protected Page[] bufpool;

    /** Array of frame descriptors*/
    protected FrameDesc[] frametab;

    /** Maps current page numbers to frames*/
    protected HashMap<Integer, FrameDesc> pagemap;

    /** The replacement policy  */
    protected Replacer replacer;

    /** 
     * @brief - BHR Variables used in calculating Hit / Load Requests
    */
    protected int totPageHits;
    protected int totPageRequests;
    protected int pageLoadHits;
    protected int pageLoadRequests;
    protected int uniquePageLoads = 0;
    protected int pageFaults = 0;
    
    /**
     * @ desc - BHR Values - The number of hits divided by the number of requests
    */
    protected double aggregateBHR = 0;
    protected double pageLoadBHR = -1;
    protected final int maxPages = 100;

    /**
     * @brief Array that keeps track of information about pages
     * [0]PageNo, [1]Loads, [2]Hits, [3]Victim count
     */
    protected int[][] pageRefCount = new int[1000][4]; 

    /**
     * @brief - The number of pages you want to print relative to hit count
     */
    protected int kTopPages = 50;

    /**
     * Constructs a buffer mamanger with the given settings.
     * @param numbufs number of buffers in the buffer pool
     */
    public BufMgr(int numbufs) 
    {   
        numbufs = numberOfBuffers;

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
        replacer = new Lru(this);  
    }

    /**
     * Allocates a set of new pages, and pins the first one in an appropriate
     * frame in the buffer pool.
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
            tempfd.state = Lru.AVAILABLE;
            replacer.freePage(tempfd);
        }

        //deallocate the page from disk 
        Minibase.DiskManager.deallocate_page(pageno);
    }

  /**
   * @brief - Pins a disk page into the buffer pool. If the page is already pinned, this
   * simply increments the pin count. Otherwise, this selects another page in
   * the pool to replace, flushing it to disk if dirty.
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
        if(pageno.pid > 8 )
        {
            totPageRequests++;
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
                tempfd.state = Lru.PINNED;
                page.setPage(bufpool[tempfd.index]);
                
                // increment number of hits in buffer pool
                if(pageno.pid > 8)
                {
                    totPageHits++;
                    pageRefCount[pageno.pid][0] = pageno.pid;
                    pageRefCount[pageno.pid][2] = pageRefCount[pageno.pid][2] + 1;
                }
                         
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

            if(pageno.pid > 8)
            {   
                pageLoadRequests++;
                pageRefCount[pageno.pid][0] = pageno.pid;
                pageRefCount[pageno.pid][1] = pageRefCount[pageno.pid][1] + 1;
                pageRefCount[pageno.pid][3] = pageRefCount[pageno.pid][3] + 1;
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
	    }

        //updating frame descriptor and notifying to replacer
        tempfd.pageno.pid = pageno.pid;
        tempfd.pincnt = 1;
        tempfd.dirty = false;
        pagemap.put(Integer.valueOf(pageno.pid), tempfd);
        tempfd.state =Lru.PINNED;
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
                tempfd.state = Lru.REFERENCED;
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
    /**
     * @breif Prints our information about the BHR, Page HIts, Load Requests, Replacer used,
     * and it outputs this infomration to a file and in the console.
     */
    public void printBhrAndRefCount()
    {
        /** Prints the replcer being used */
        replacer.printReplacerInfo();
        
        // Sort the 2d array first
        Arrays.sort(pageRefCount, (a, b) -> Integer.compare(b[2],a[2])); //decreasing order
        
        // Fix page reference counts an calculate BHR
        pageLoadRequests = pageLoadRequests - 1;
        aggregateBHR = ( (double)totPageHits / (double)pageLoadRequests );
            
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

        try
        {
            outputToFile();
        }

       catch(Exception e)
       {
        System.out.println(e);;
       }
    }

    /**
     * @brief Prints the BHR information to an output file
     * <Replacer>-bhrtest-output-<numbufs>.txt
     * @throws IOException
     */
    public void outputToFile() throws IOException 
    {
        String folderName = "output";
        String fileName = "Lru-bhrtest-output-" + numberOfBuffers + ".txt" ;
        File folder = new File(folderName);
        File file = new File(folder, fileName);

        try 
        {
            // Create the folder if it doesn't exist
            if (!folder.exists()) {
                if (folder.mkdirs()) {
                    System.out.println("Folder created: " + folder.getAbsolutePath());
                } else {
                    System.err.println("Failed to create folder: " + folder.getAbsolutePath());
                    return;
                }
            }

            // Create the file and write content to it
            FileWriter writer = new FileWriter(file, true);

            //print counts:
            writer.write("+----------------------------------------+\n");
            writer.write("The policy Being used is Lru\n");
            writer.write("+----------------------------------------+\n");
            writer.write("The number of buffers is: " + getNumBuffers() + "\n");
            writer.write("+----------------------------------------+\n");
            writer.write("Aggregate Page Hits: "+ totPageHits + "\n");
            writer.write("+----------------------------------------+\n");
            writer.write("Aggregate Page Loads: "+ pageLoadRequests + "\n");
            writer.write("+----------------------------------------+\n");
            writer.write("Aggregate BHR (BHR1) : ");
            writer.write(String.valueOf(aggregateBHR) + "\n");
            writer.write("+----------------------------------------+\n");
            writer.write("The top (" + kTopPages + ") pages with respect to hits are:\n");
        
            // If total pages are > 0, calculate hit ratio
            if(totPageHits > 0)
            {
                writer.write("Page No.\t\tNo. of Page Loads\t\tNo. of Page Hits\t\tNo. of times Victim\t\tHit Ratios\n");
                for(int i =0; i <  kTopPages; i++)
                    writer.write(pageRefCount[i][0] + "\t\t\t\t" + pageRefCount[i][1] + "\t\t\t\t" + pageRefCount[i][2] + "\t\t\t\t" + pageRefCount[i][3] + "\t\t\t\t" + (pageRefCount[i][2]/totPageHits) + "\n");
            }

            // If total pages are < 0, do not calculate hit ratios
            else
            {
                writer.write("Page No.\t\tNo. of Page Loads\t\tNo. of Page Hits\t\tNo. of times Victim\t\tHit Ratios\n");
                for(int i =0; i < kTopPages; i++)
                    writer.write(pageRefCount[i][0] + "\t\t\t\t" + pageRefCount[i][1] + "\t\t\t\t" + pageRefCount[i][2] + "\t\t\t\t" + pageRefCount[i][3] + "\t\t\t\t" + 0 + "\n");
            }

            writer.write("+----------------------------------------+\n");
            writer.close();
            System.out.println("File created and content written: " + file.getAbsolutePath());
        } 
        
        catch (IOException e) 
        {
            System.err.println("An error occurred: " + e.getMessage());
        }
    }
} // public class BufMgr implements GlobalConst