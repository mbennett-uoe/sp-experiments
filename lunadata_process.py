# -*- coding: utf-8 -*-
'''Extract Session Papers data from Luna and make some case files'''
import sys, requests, os, json
from moonsun_miner import luna_login, solr_query
from xml_handler import gettree, additem, writetree
from redis import Redis
from datetime import datetime
r = Redis()

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
          'mediafileName', # Image filename
          'urlSize4' # We need something with image directory
          ]
# Sort down hierarchically in Solr for efficiency
sort = ['work_shelfmark_sortable asc', # Volume
        'work_subset_index_sortable asc', # Case in Volume
        'sequence_sortable asc' # Page in Case
        ]

imagedir = "./images/UoE~1~1/"

# Ok here we go!
results = solr_query(s, query, fields=fields, sort=sort) #, limit=10)
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
#pprint(new_results[0:10])

# ok, let's trim that url path a bit since we only need the end!
for item in new_results:
    # The Python syntax here is a bit confusing because of the chaining
    # It starts with the character which will be used to re-join the split string - "/".join
    # This takes an argument of a list, which we are generating by splitting the data on / - item["urlSize4"].split("/")
    # Once the above split is done, we only want the last two arguments to pass to the joiner - [-2:]
    item["filepath"] = "/".join(item["urlSize4"].split("/")[-2:])

    # get rid of the full path, because we now have no use for it
    del item["urlSize4"]

# Now that we've changed what data is held, lets amend the fields list so we can re-use it for CSV export etc
fields.append("filepath")
fields.remove("urlSize4")

# Lets try writing a csv
import csv
with open('lunadata.csv2', 'w') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fields)
    writer.writeheader()
    writer.writerows(new_results)

errors = open("lunadata_errors.log", "w")

# state trackers to enable i/o efficiency
t = None
curr_shelf = None
curr_index = None
for result in new_results:
    if result.get("work_shelfmark") is None:
	print "No shelfmark!!! %s"%result
	errors.write("%s Missing Shelfmark. Record %s\n"%(datetime.now(), result))
	continue
    if result.get("work_subset_index") is None:
        print "No case number!!! %s"%result
        errors.write("%s Missing Case Number. Record %s\n"%(datetime.now(), result))
	continue
    # for efficiency, don't bother closing and writing the tree until all pages are processed
    if result["work_shelfmark"] != curr_shelf or result["work_subset_index"] != curr_index:
    	print "New case: %s - %s"%(result["work_shelfmark"], result["work_subset_index"])
        if t is not None:
	     print "Current tree object populated, writing to file"
	     writetree(curr_shelf, curr_index, t)
        # update state trackers and refresh tree object
        curr_shelf = result["work_shelfmark"]
        curr_index = result["work_subset_index"]
	print "Getting new tree"
        t = gettree(result["work_shelfmark"], result["work_subset_index"])
    print "Adding sequence %s" %result["sequence"]
    origin = imagedir + result["filepath"]
    t = additem(t, result["sequence"], title=result["repro_title"], origin=origin)
    json_item = json.dumps({"shelfmark": result["work_shelfmark"],
                            "index": result["work_subset_index"],
                            "sequence": result["sequence"]})
    r.lpush("images:to_process", json_item)

# write the last tree!
print "Writing last tree"
writetree(curr_shelf, curr_index, t)
print "Writing error log"
errors.close()



# ok, lets see if we can actually find the media files!
# for this test, we want a subset of pages for which both exist, so we can directly compare the OCR results
#jpg_prefix = "/home/mike/Projects/sp-experiments/images/UoE~1~1/"
#tiff_prefix = "/media/diu_projects/SessionPapers/0133000-0133999/Process/"

# subset_results = []
#
# for item in new_results:
#     jpg = jpg_prefix + item["filepath"]
#     tiff = tiff_prefix + item["mediafileName"]
#     if os.path.isfile(jpg) and os.path.isfile(tiff): subset_results.append(item)
#
# # lets see
# print("Found %s results with both images"%len(subset_results))
# #pprint(subset_results)
# # dump to csv
# import csv
# with open('testable_luna_data.csv', 'w') as outfile:
#     writer = csv.DictWriter(outfile, fieldnames=fields)
#     writer.writeheader()
#     writer.writerows(subset_results)




