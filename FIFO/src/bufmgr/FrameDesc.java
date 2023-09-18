package bufmgr;

import global.PageId;

/**
 * A frame descriptor; contains info about each page in the buffer pool.
 */
class FrameDesc {

/** @desc - Index in the buffer pool. */
  public int index;
  
  /** @desc - Identifies the frame's page. */
  public PageId pageno;

  /** @desc - The frame's pin count. */
  public int pincnt;

  /** @desc - The frame's dirty status. */
  public boolean dirty;

  /* @desc - Generic state used by replacers. */
  public int state;

  /* @desc - number of hits for current page */
  public int numOfHits;

  /* @desc - number of loads for current page */
  public int numOfLoads;
  
  /**
   * Default constructor; empty frame.
   */
  public FrameDesc(int index) 
  {
    this.index = index;
    pageno = new PageId();
    pincnt = 0;
    dirty = false;
    state = 0;
    numOfHits = 0;
    numOfLoads = 0;
  }

} // class FrameDesc
