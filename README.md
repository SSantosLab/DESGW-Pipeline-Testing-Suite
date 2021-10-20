# DESGW-Pipeline-Testing-Suite

Testing suite for the DESGW Data Pipeline

Started during the DESGW Workshop Oct 11 - 14

## STEPS 

1) Configure dagmaker.rc

   a) A script that grabs exposures based on a random sky map
      and returns a df of exposures (templates and search).
      This includes some selection criteria to select which 
      set of exposures will be search versus template images 
      and require consecutive expnums, >30sec and < 200?sec 
      exposures, and teff limits.
   b) A script that edits dagmaker.rc to ensure we don’t use 
      the chosen search exposures as templates (e.g., edits
      the min / max nite, teff limits, t window, and season).
   c) A script to verify the dagmaker.rc file was configured 
      correctly. Includes smart assert statements.

2) Run DAGMaker and submit jobs

   a) A script that runs DAGMaker for the search exposures
      and submits the resulting dag files. Includes checks to
      ensure that the dag is what we expect and that the jobs
      were submitted.

3) Evaluatate the success of SEDiff

   a) A script that checks how many .FAILs are present in 
      /pnfs in the forcephoto and normal areas.

4) Run Post Processing
   
   a) A script to create the postproc_SEASON.ini file with
      the appropriate settings
   b) A script to run the Post-Processing Pipeline.
 
5) Evaluate the success of Post-Processing
   
   a) Check for dat files, html, stamps for the test event.
