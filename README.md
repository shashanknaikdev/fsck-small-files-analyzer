# fsck-small-files-analyzer


Script to get fsck extract, parse and load it into a Hive table for small files analysis

Run fsck_prepare.sh.

Once parsed and processed fsck data is loaded into tables, we can look for small files by drilling into the filesystem using queries such as:

    select  path,

        round(avgblocksize,5) as avgBlockSizeMB,

        round(totalsize,5) as sumFileSizeMB,

        totalblocks

    from fsck_small_files_tbl

    where   path ilike ("/analyze/this/folder%") and length(regexp_replace(path,'[^/]',''))<5

        and extract_dt='2019-04-18'
