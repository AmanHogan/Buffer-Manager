package bufmgr;
import diskmgr.*;
import global.*;

/**
 * class Policy is a subclass of class Replacer use the given replacement
 * policy algorithm for page replacement
 */

class Lru extends Replacer 
{
	// My constants werent working globally, so I initialized them here
	// Constants that describe status of page
	protected static final int AVAILABLE = 10;
	protected static final int REFERENCED = 11;
	protected static final int PINNED = 12;

	private BufMgr mgrArg;

	/* private field - An array to hold number of frames in the buffer pool */
	private int  nframes; 

	/* private field - number of frames used */ 
	private int  frames[]; 

	public Lru(BufMgr mgrArg)
	{
		super(mgrArg);
		this.mgrArg = mgrArg;

		// Initilize array of frame indexs
		// Frames are initially available
		for (int i = 0; i < frametab.length; i++) 
			frametab[i].state = AVAILABLE;
		
		nframes = 0;
		frames = new int[frametab.length];
	}

	/**
	 * This pushes the given frame to the end of the list.
	 * List Order: < LRU (Least recently used frame ) <------> MRU (Most recently used frame)>
	 * @param frameNo- the frame number
	**/
	private void update(int frameNo) 
	{
		// Check to see if frame is in pool
		int framePos = 0;
		for (framePos = 0; framePos < nframes; ++framePos) 
		{
			if (frames[framePos] == frameNo ) 
				break;
		}

		// Update position of the Frame
		// Most recently used prame goes at the front of the array
		while (++framePos < nframes)
			frames[framePos-1] = frames[framePos];
		frames[nframes-1] = frameNo;
	}


	/**
	 * Notifies the replacer of a pined page.
	 * @param fdesc - frame descriptor object that contains details about a frame
	 */
	public void pinPage(FrameDesc fdesc) 
	{
		// Pin the page and update the frame array
		int frameIndex = fdesc.index;
		frametab[frameIndex].state = PINNED;
		update(frameIndex);
	}

	/**
	 * Finding a free frame in the buffer pool
	 * or choosing a page to replace using your policy
	 * @param None
	 * @return 	return the frame number return -1 if failed
	*/
	public int pickVictim() 
	{

		int bufferSize = mgrArg.getNumBuffers();
		int victim = 0;

		// If the number of frames is less than th buffer size ...
		// Put the frame at the end of the frametab
		//  Make the frame status pinned
		if (nframes < bufferSize)
		{
			victim = nframes++;
			frames[victim] = victim;
			frametab[victim].state = PINNED;
			return victim;
		}

		// check for unpinned frames in the frametab
 		// pin the first encountered unpinned frame and 
		// call the update function and return the frame
		for (int i = 0; i < bufferSize; ++i)
		{
			victim = frames[i];
			if(frametab[victim].state != PINNED)
			{
				frametab[victim].state = PINNED;
				update(victim);
				return victim;
			}
		}

		// No frames were available
		return -1;
	}
}
