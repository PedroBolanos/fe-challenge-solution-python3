# Solution Feedback

## Functionality and Efficiency
### The quality of the solution e.g., no bugs, etc.
1.  No  bugs found
### The efficiency of the data pipeline.
1.  The pipeline seems fairly efficient. However, the data could be stored in a more efficient way for query use-cases - see comment in next bullet.
### Data modeling within the relational database.
1.  The data could be stored in a more efficient way to meet the query use-cases e.g., Enriching open data w/ data from matching receipts to avoid joining the two tables
2.  Storing the email domain as a separate column.
### Your approach to handling data not used for analysis.
1.  It is good that headers were stored as JSON.
2.  Consider parsing out name from email address when provided.
## Code quality 
### Production-quality i.e., good error handling, considerate of edge cases, documented, etc.
1.  Missing handling for failures that could occur when records fail to process e.g., unexpected date or number formats.
2.  Zipfile handling is a bit awkward i.e., unzipping if/when an exception occurred.
### Code should be well-structured and easy to follow.
1.  Consider pulling DDLs out of data-loading process and handled through a separate bootstrapping process (or db changeset tool like liquibase).
2.  The regular expression for an email is not very maintainable given its complexity. Consider outsourcing this to a third-party lib.  Assuming you couldn’t find one that delivered this level of completeness but felt it was important, you should provide a link to someone who is maintaining this regex. 
3.  The regex for transaction amount only accepts positive numbers; the behavior could be replaced with “.astype”.
4.  Consider using environment variables to provide configuration (vs. editing the script itself). Also consider usage of fewer global variables.
### The code should be well-tested.
1.  No automated tests
### Prudent use of third-party libraries/frameworks.
1.  No issues here.