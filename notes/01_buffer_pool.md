- There can be 1 or multiple buffer pools. To put a page to a pull we can use primary key and md5 hash/mod to round robin.
- + We can pre-fetch pages to buffer pool
    - For sequential read (full scan) we can already put pages to the pool while current page is processed
    - For index scans - once we know what are next pages - put them to the pool
- Scan sharing
    - Select avg limit -- will produce different results under load because of cursor will be reused and start from different points


** Use O_DIRECT ** to bypass file cache