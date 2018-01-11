# Google Apps Script Project: gasThreader
This repo (gasThreader) was automatically created on 11 January 2018 18:07:10 GMT by GasGit
for more information see the [desktop liberation site](http://ramblings.mcpher.com/Home/excelquirks/drivesdk/gettinggithubready "desktop liberation")
you can see [library and dependency information here](dependencies.md)

For info see
http://ramblings.mcpher.com/Home/excelquirks/gasthreader

Quotas

We've all come across the problem of the run time limit, trigger timing accuracy, cache size limitations and so on, all of which makes running significantly sized jobs difficult. Splitting the job up into chunks is possible of course, but then bringing all the data back together and storing it somewhere in the meantime is a problem. A large job will also use up so much memory that the Server side JavaScript will run very slowly or not at all.

GasThreader 

is my latest solution to these conundrums and has these features
-Run stages in sequence
-Automatically split stages of jobs into manageable chunks that can be run in parallel.
-Watch progress on a webapp dashboard.
-Persist all the results Server side, and have access to any previous stage.
-Automatically reduce the results of parallel chunks.
-Be restartable (not yet, but soon)
