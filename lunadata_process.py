'''Extract Session Papers data from Luna and make some case files'''
import sys, requests
from moonsun_miner import luna_login, solr_query

# Let's get the data!
# Start by logging into Luna
s = requests.Session()
if not luna_login(s):
    # Uh-oh
    print("Luna Login Failed")
    sys.exit(1)

# Ok, logged in, let's grab the records
query = 'mediaCollectionId:"UoE~1~1"' # Match all records in Scottish Session Papers collection
fields = ['work_shelfmark', # Volume
          'work_subset_index',  # Case Number
          'sequence', # Page number of image (in Case, not Volume!)
          'repro_title', # 'Case X, Page Y' - Useful for asserting data is correct?
          'mediaFileName' # Image filename
          ]
# Sort down hierarchically in Solr for efficiency
sort = ['work_shelfmark asc', # Volume
        'work_subset_index asc', # Case in Volume
        'sequence asc' # Page in Case
        ]

# Ok here we go!
results = solr_query(s, query, fields=fields, sort=sort)
# So either we have results or moonsun_miner exited with error, so we can be  a bit lazy and not bother to check
# the response before processing!!
# (I know this is very bad program design, but I intend to properly module-ise moonsun_miner in the near future,
#  to a class-based design, which will make this issue irrelevant!)

# Show the first few results
from pprint import pprint
pprint(results[0:10])