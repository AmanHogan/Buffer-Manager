package bufmgr;
import diskmgr.*;
import global.*;

/**
 * @brief Implements the First in First Out Algorithm for page replacement
 */
class FIFO extends Replacer 
{

	// My constants werent working globally, so I initialized them here
	// Constants that describe status of page
	protected static final int AVAILABLE = 10;
	protected static final int REFERENCED = 11;
	protected static final int PINNED = 12;
    private BufMgr mgrArg;

    // Counter to keep track of the next frame to replace
    int nextFrameToReplace;

    public FIFO(BufMgr mgrArg) 
    {
        super(mgrArg);
        this.mgrArg = mgrArg;

        // Initilize array of frame indexs
		// Frames are initially available
		for (int i = 0; i < frametab.length; i++) 
            frametab[i].state = AVAILABLE;

        nextFrameToReplace = 0;
    } 

    /**
     * Notifies the replacer of a new page.
     */
    public void newPage(FrameDesc fdesc) 
    {
        // no need to update frame state
    }

    /**
     * Notifies the replacer of a free page.
     */
    public void freePage(FrameDesc fdesc) 
    {
        fdesc.state = AVAILABLE;
    }

    /**
     * Notifies the replacer of a pined page.
     */
    public void pinPage(FrameDesc fdesc) 
    {
        fdesc.state = PINNED;
    }

    /**
     * Notifies the replacer of an unpinned page.
     */
    public void unpinPage(FrameDesc fdesc) 
    {
        if (fdesc.pincnt == 0)
            fdesc.state = REFERENCED;
    }

    /**
     * @brief - Selects the best frame to use for pinning a new page.
     * @param None
     * @return victim frame number, or -1 if none available
     */
    public int pickVictim() 
    {
        int bufferCount = mgrArg.getNumBuffers();
        int victim = nextFrameToReplace;
        nextFrameToReplace = (nextFrameToReplace + 1) % bufferCount;

        // Check if all frames are pinned
        int attempts = 0;
        while (frametab[victim].state == PINNED && attempts < bufferCount) 
        {
            victim = (victim + 1) % bufferCount;
            attempts++;
        }

        // No available frame
        if (attempts >= bufferCount) 
            return -1; 
        
        return victim;
    }

    /**
     * @brief - Prints out the name of the policy and the number of buffers being used
     * @param None
     * @return 	return the frame number return -1 if failed
    */
	public void printReplacerInfo()
	{
		System.out.println("+----------------------------------------+");
		System.out.println("	The policy Being used is FIFO ");
		System.out.println("+----------------------------------------+");
		System.out.println("	The number of buffers is: " + mgrArg.getNumBuffers());
		System.out.println("+----------------------------------------+");
	}
} 