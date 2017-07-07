# -*- coding: utf-8 -*-
'''Poll a redis queue for images to process, run the crop tool over them and write them to disk'''

import os, sys, json
from datetime import datetime
from time import sleep
from redis import Redis
from sp_crop import process_image
#from logging import Logger

r = Redis()

# Config
queues = {"read":"images:to_process",
          "write":"images:processed",
          "work":"images:in_progress",
          "error":"images:errors"
          }
status = "status:image_worker"

wait_seconds =15 # How long to sleep for if no items in the queue
wait_modifier = 1 # Multiplier for wait_seconds if consecutive polls are empty
wait_maxseconds = 900 # What stage to stop increasing the wait time
exit_when_empty = False

current_wait = wait_seconds
should_exit = False
while not should_exit:
    # See if we can pop an item from the queue!
    json_item = r.rpoplpush(queues["read"],queues["work"])
    if json_item:
        # Ok, lets get to work :D
        #print("Item found: %s"%json_item)

        # Reset the wait timer
        current_wait = wait_seconds
        # Some basic checks, if any fail, push the item to the error queue and let some other poor bugger deal with it
        # Firstly, can we reserialise the data from redis?
        try:
            item = json.loads(json_item)
        except Exception as e:
            error = {"error": "Could not load item dictionary from redis: %s"%e,
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": json_item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        # Do we have the two bits of data we need?
        if not item["infile"] or not item["outfile"]:
            # Well, this is awkward! If I'm the only one populating the queue, I would hope that we should
            # never end up here unless I've done something monumentally stupid, but better safe than sorry!
            error = {"error":"Missing in or out file name(s)",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        # Does the desired input file exist?
        if not os.path.isfile(item["infile"]):
            error = {"error": "Input file does not exist",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        # Does the proposed output file exist? If so, and the overwrite flag is not set, it's a problem!
        if os.path.isfile(item["outfile"]) and "overwrite" not in item:
            error = {"error": "Output file exists and overwrite flag not set",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        # ok, so at this point everything should be cool, let's try and process the image
        try:
            #print("Calling the image processor...")
            r.set(status,"%s: Processing %s"%(datetime.now().strftime("%d/%m/%y %H:%M:%S"),item["infile"]))
            process_image(item["infile"], item["outfile"])
            # if this didn't error out to the except block, we can assume process complete
            # write to complete, remove from in progress
            r.rpush(queues["write"], json_item)
            r.lrem(queues["work"], json_item)
            r.set(status, "%s: Waiting for work"%datetime.now().strftime("%d/%m/%y %H:%M:%S"))
            #print("Done")
            # all done, go to the top and start again!
            continue
        except Exception as e:
            # something went wrong with image processing
            error = {"error": str(e),
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            r.set(status, "%s: Waiting for work" % datetime.now().strftime("%d/%m/%y %H:%M:%S"))
    else:
        if exit_when_empty:
            r.set(status, "%s: Terminated due to empty queue"%datetime.now().strftime("%d/%m/%y %H:%M:%S"))
            sys.exit(1)
        # no item, wait and try again
        #print("No items in queue, sleeping for %ss"%current_wait)
        r.set(status, "%s: No items in queue, sleeping for %ss" %(datetime.now().strftime("%d/%m/%y %H:%M:%S"), current_wait))
        sleep(current_wait)
        current_wait = current_wait * wait_modifier
        if current_wait > wait_maxseconds: current_wait = wait_maxseconds
        continue





