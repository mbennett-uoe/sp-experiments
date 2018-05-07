# -*- coding: utf-8 -*-
'''Poll a redis queue for images to process, run the crop tool over them and write them to disk'''

import os, sys, json
from datetime import datetime
from time import sleep
from redis import Redis
from sp_crop import process_image
from xml_handler import gettree, getorigin, addimage, addlog, writetree
#from logging import Logger

r = Redis()

# Config
queues = {"read":"images:to_process",
          "write":"images:processed",
          "work":"images:in_progress",
          "error":"images:errors"
          }

# was worker started with an id number?
if len(sys.argv) > 1 and sys.argv[1] == "-n" and int(sys.argv[2]) > 0:
    # TODO: Check if a worker with the same ID already exists! Ideally the supervisor would handle this, but a
    #       failsafe here is probably a good idea.
    worker_id = "_%s"%sys.argv[2]
else:
    worker_id = ""

status = "status:image_worker" + worker_id
pid = "pid:image_worker" + worker_id

output_path = "./output/images"

wait_seconds = 15 # How long to sleep for if no items in the queue
wait_modifier = 1 # Multiplier for wait_seconds if consecutive polls are empty
wait_maxseconds = 900 # What stage to stop increasing the wait time
exit_when_empty = False

# write pid to redis
r.set(pid,os.getpid())

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
        # Do we have the three bits of data we need?
        if not item["shelfmark"] or not item["index"] or not item["sequence"]:
            # Well, this is awkward! If I'm the only one populating the queue, I would hope that we should
            # never end up here unless I've done something monumentally stupid, but better safe than sorry!
            error = {"error":"Missing shelfmark, index or sequence",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue

        # Does our item have an xml file with a valid origin?
        tree = gettree(item["shelfmark"], item["index"], item["sequence"]) # This will always return a valid ET, so no check needed
        origin = getorigin(tree, item["sequence"])
        if not origin:
            error = {"error": "No origin image file",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            tree = addlog(tree, item["sequence"], "image_worker", "No origin image file")
            writetree(item["shelfmark"],item["index"],item["sequence"],tree)
            continue

        # Does the desired input file exist?
        if not os.path.isfile(origin):
            error = {"error": "Input origin file does not exist",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            tree = addlog(tree, item["sequence"], "image_worker", "Input origin image file does not exist")
            writetree(item["shelfmark"], item["index"], item["sequence"], tree)
            continue

        outfile = origin.replace(".jpg", ".crop.png")
        outfile = outfile.split("/")[:-1]
        outpath = "%s/%s/%s/%s"%(output_path, item["shelfmark"], item["index"], item["sequence"])
        os.makedirs(outpath, exist_ok=True)
        # Does the proposed output file exist? Uncomment this block to error here if you don't want to overwrite
        # if os.path.isfile(outfile):
        #     error = {"error": "Output file exists",
        #              "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
        #              "data": item}
        #     r.lpush(queues["error"], json.dumps(error))
        #     r.lrem(queues["work"], json_item)
        #     continue


        # ok, so at this point everything should be cool, let's try and process the image
        try:
            #print("Calling the image processor...")
            r.set(status,"%s: Processing %s"%(datetime.now().strftime("%d/%m/%y %H:%M:%S"),origin))
            process_image(origin, outpath+outfile)
            # if this didn't error out to the except block, we can assume process complete
            # add the new image to the xml file
            tree = addimage(tree, item["sequence"], "crop", outpath+outfile)
            tree = addlog(tree, item["sequence"], "image_worker", "Successfully created initial cropped image")
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
            tree = addlog(tree, item["sequence"], "image_worker", "Error creating cropped image: "%str(e))
            r.lrem(queues["work"], json_item)
            r.set(status, "%s: Waiting for work" % datetime.now().strftime("%d/%m/%y %H:%M:%S"))
        finally:
            writetree(item["shelfmark"],item["index"], item["sequence"], tree)
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





