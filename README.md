# DratiniFS with ExtremeSpeed PSP exFAT Driver

please see https://qberty.com/dratinifs-leftovers-finally-exfat-for-your-psp-with-extremespeed for more information.

this repo is a raw dump of the POC + fixes since it's latest release. The driver itself is pretty stable from my testing (mostly game playing / file io), but has some major niche bugs that I have not been able to squash thus far (notable the DATAINSTALL race condition). PR's and suggestions are fully welcome if you see anything easy to clean up and improve just open a PR.

keep in mind I've only done 1 compat pass and 2 optimization passes specifically for exFAT and ES performance. so I've left somerelevant comments where I could that I needed to draw attention to where I think performance or efficiency could be improved. However there were a lot of regressions in my second optimization pass that let to some rollbacks of performant code to resort to more of a sony-like structure. Sony's original code tends to prefer dirty opens and unkept chains in various areas so I'd like 2 more passes to get things narrowed down and the race conditions caught up.

most of the code is boring io filesystem code... but it gets interesting when you take a look at the ES interactions. essentially giving fatms disk visibility to detect ES partitions and coexist / format with them. exFAT here supports contiguous files so skipping fat table lookups bypass any cluster chain walking bottlenecks that plague fat32 for the PSP.


## What's Missing

well a fully used upcase table at the very least, I've included one for formatting for now. so unicode support isn't quite there yet. I had put checksum validation but had to remove it because the PSP hated it. Timestamps on write / accessing just aren't in because that wasn't a priority. We can't do truncate to 0 on chains because that will break some games catastrophically. deleting entries are not reclaimed by packing as we normally should do...

FAT16 / 12 support is also missing so sdcards smaller than 4 GB (if they aren't formatted with exFAT) won't be supported if they are set at FAT16/12.

aside from the race conditions and DATAINSTALL issues for some games (works in LBP but not MGS:PW??), there's another issue that cannot be fixed in software automatically, and that is the total disk size. Most adapters i've seen report 57 GB total, some less or more than others but it's never the correct size (in an attempt to fool the controller) so i've bundled an MSSTOR patcher that maxes out the total usable space to 2 TB (theoretical max of the hardware). This solves letting you use any size sdcard without msstor tripping the validation, but most importantly both DratiniFS and Leftovers will patch it in mem after it can read all available partitions to give it a real value of accessible data. My issue with this, is that it's not "real" (if you have 1 TB with a 400 GB partition on it the driver will patch in 400 GB). We can't patch in the actual disk size because the adapter never reports it. So connecting the PSP to your PC will always show only visible partitions and not the whole disk. The only solution to this is rewrite MSSTOR completely to handle this (which is outside of my wheelhosue) or have a homebrew app scan the disk for it's "real" size and store it per sdcard.... :/

i'll update this repo with proper docs of my findings plus better commenting when I have some time.

no license as this is a hobby project do whatever you'd like with it, though do let me know (if I could also improve something as well).