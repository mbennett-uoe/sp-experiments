# -*- coding: utf-8 -*-
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


# Lets loop through the results, and spit them out to a CSV
# Unfortunately, because the fields are 'multivalued' in Solr, the actual data always comes as a list even though
# all the actual data fields only have one entry.
# We can fix that in two ways:
# a) Loop through the results, and reduce the one-item lists to strings
# b) Use the equivalent _sortable fields in Solr for the query
#
# I am tending towards solution a here, because the _sortable fields may well have been modified by the Solr routines
# that prepare text to be searchable (i.e you might get "Bronte" where the original data says "Brontë")

# Here is a routine implementing solution a
def reduce_singles(source_list):
    '''Take a list of dictionaries and collapse single item lists within the dictionary values'''
    new_list = []
    for result in source_list:
        # This should be a dictionary of a single result, so let's loop it and get rid of single item lists
        for item, val in result.items():
            if isinstance(val, list) and len(val) == 1:
                val = val[0]
                result[item] = val
        new_list.append(result)
    return new_list

# Process the results as above and show a couple to check it's working
from pprint import pprint
new_results = reduce_singles(results)
pprint(new_results[0:10])