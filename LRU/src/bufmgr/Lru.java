package bufmgr;
import diskmgr.*;
import global.*;

/**
 * class Policy is a subclass of class Replacer use the given replacement
 * policy algorithm for page replacement
 */

class Lru extends Replacer 
{
  protected static final int AVAILABLE = 10;
  protected static final int REFERENCED = 11;
  protected static final int PINNED = 12;

  private BufMgr mgrArg;
  private int  frames[]; /* private field - An array to hold number of frames in the buffer pool */
  private int  nframes; /* private field - number of frames used */ 

  public Lru(BufMgr mgrArg)
  {
    super(mgrArg);
    this.mgrArg = mgrArg;

    for (int i = 0; i < frametab.length; i++) 
    {
      frametab[i].state = AVAILABLE;
    }
    
    // initialize parameters for LRU and MRU
    nframes = 0;
    frames = new int[frametab.length];
  }

  /**
    This pushes the given frame to the end of the list.
    @param frameNo- the frame number
    List Order: < LRU --------------- MRU >
  **/

  private void update(int frameNo) 
  {
    int framePos = 0;
    for (framePos = 0; framePos < nframes; ++framePos) 
    {
      // If page is pinned or already in pool, break
      if (frames[framePos] == frameNo ) 
      {
        break;
      }
    }

    while ( ++framePos < nframes )
    {
      frames[framePos-1] = frames[framePos];
    }
    frames[nframes-1] = frameNo;
  }


  /**
   * Notifies the replacer of a pined page.
   */
  public void pinPage(FrameDesc fdesc) 
  {
    int frameIndex = fdesc.index;
    frametab[frameIndex].state = PINNED;
    update(frameIndex);
  }

  public void unpinPage(FrameDesc fdesc) { }

  /**
   * Finding a free frame in the buffer pool
   * or choosing a page to replace using your policy
   *
   * @return 	return the frame number
   *		return -1 if failed
   */
  public int pickVictim() 
  {

    int numberOfBuffers = mgrArg.getNumBuffers();
    int victimFrame = 0;

    if (nframes < numberOfBuffers)
    {
      victimFrame = nframes++;
      frames[victimFrame] = victimFrame;
      frametab[victimFrame].state = PINNED;
      return victimFrame;
    }

    for (int i = 0; i < numberOfBuffers; ++i)
    {
      victimFrame = frames[i];
      if(frametab[victimFrame].state != PINNED)
      {
        frametab[victimFrame].state = PINNED;
        update(victimFrame);
        return victimFrame;
      }

    }

    return -1; // No frames available
  }

  public void newPage(FrameDesc fdesc) { }

  public void freePage(FrameDesc fdesc) { }

}
