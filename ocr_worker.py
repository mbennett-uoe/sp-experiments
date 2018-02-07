# -*- coding: utf-8 -*-
'''Poll a redis queue for images to OCR, run them through tesseract and write the results to disk'''

import os, sys, json, codecs
from datetime import datetime
from time import sleep
from redis import Redis
from PIL import Image
import pyocr
from xml_handler import gettree, getimage, addocr, addlog, writetree

#from logging import Logger

r = Redis()

# Config
queues = {"read":"ocr:to_process",
          "write":"ocr:processed",
          "work":"ocr:in_progress",
          "error":"ocr:errors"
          }

# was worker started with an id number?
if len(sys.argv) > 1 and sys.argv[1] == "-n" and int(sys.argv[2]) > 0:
    # TODO: Check if a worker with the same ID already exists! Ideally the supervisor would handle this, but a
    #       failsafe here is probably a good idea.
    worker_id = "_%s"%sys.argv[2]
else:
    worker_id = ""


status = "status:ocr_worker" + worker_id
pid = "pid:ocr_worker" + worker_id

output_path = "./output/ocr/"

wait_seconds = 15 # How long to sleep for if no items in the queue
wait_modifier = 1 # Multiplier for wait_seconds if consecutive polls are empty
wait_maxseconds = 900 # What stage to stop increasing the wait time
exit_when_empty = False

tesseract_dicts = ["eng", "enm"]


# write PID to redis
r.set(pid,os.getpid())

# initialise tesseract
try:
    tess = pyocr.get_available_tools()[0]
except Exception as e:
    r.set(status,"%s: Terminated with fatal error - No Tesseract found! - %s"%(datetime.now().strftime("%d/%m/%y %H:%M:%S"),e))
    #print("Fatal Error - No Tesseract found!")
    #print(e)
    sys.exit(1)

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
        # Do we have the data we need?
        if not item["shelfmark"] or not item["index"] or not item["sequence"]:
            # Well, this is awkward! If I'm the only one populating the queue, I would hope that we should
            # never end up here unless I've done something monumentally stupid, but better safe than sorry!
            error = {"error":"Missing shelfmark, index or sequence",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue

        # Does our item have an xml file with a valid cropped image?
        tree = gettree(item["shelfmark"], item["index"])  # This will always return a valid ET, so no check needed
        crop = getimage(tree, item["sequence"], "crop")
        if not crop:
            error = {"error": "No cropped image file",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            tree = addlog(tree, item["sequence"], "ocr_worker", "No cropped image file")
            writetree(item["shelfmark"], item["index"], tree)
            continue

        # Does the desired input file exist?
        if not os.path.isfile(crop):
            error = {"error": "Input cropped image file does not exist",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            tree = addlog(tree, item["sequence"], "ocr_worker", "Cropped image file does not exist")
            writetree(item["shelfmark"], item["index"], tree)
            continue
        # Does the proposed output directory exist?
        if not os.path.isdir(output_path):
            error = {"error": "Output path is not a directory",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue

        if "dicts" not in item: item["dicts"] = tesseract_dicts
        # Is the proposed list of tesseract dictionaries actually a list?
        if not isinstance(item["dicts"], list):
            error = {"error": "Tesseract dictionaries list is not actually a list!",
                     "timestamp": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        # ok, so at this point everything should be cool, let's try and process the image
        try:
            r.set(status, "%s: Processing %s"%(datetime.now().strftime("%d/%m/%y %H:%M:%S"),item["infile"]))
            #print("Running OCR...")
            # if no dictionaries specified, use all of them!
            if len(item["dicts"]) == 0:
                dicts = tesseract_dicts
            else:
                dicts = item["dicts"]

            image = Image.open(crop)
            for dict in dicts:
                image_text = tess.image_to_string(image, lang=dict, builder=pyocr.builders.TextBuilder())
                #word_boxes = tess.image_to_string(image, lang=dict, builder=pyocr.builders.WordBoxBuilder())
                #line_boxes = tess.image_to_string(image, lang=dict, builder=pyocr.builders.LineBoxBuilder())
                inf = item["infile"].split("/")[-1]
                outfile = output_path + inf + "-" + dict + "-" + "text.txt"
                with codecs.open(outfile, 'w', encoding='utf-8') as f:
                    pyocr.builders.TextBuilder().write_file(f, image_text)

                tree = addocr(tree, item["sequence"], "crop-firstpass", dict, outfile)
                tree = addlog(tree, item["sequence"], "ocr_worker",
                              "Successfully created first pass %s OCR on cropped image"%dict)
                #with codecs.open(item["outpath"] + inf + "-" + dict + "-" + "words.txt", 'w', encoding='utf-8') as f:
                #    pyocr.builders.WordBoxBuilder().write_file(f, word_boxes)
                #with codecs.open(item["outpath"] + inf + "-" + dict + "-" + "lines.txt", 'w', encoding='utf-8') as f:
                #    pyocr.builders.LineBoxBuilder().write_file(f,line_boxes)

            #process_image(item["infile"], item["outfile"])
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
            tree = addlog(tree, item["sequence"], "image_worker", "Error creating OCR: " % str(e))
            r.lrem(queues["work"], json_item)
            r.set(status, "%s: Waiting for work"%datetime.now().strftime("%d/%m/%y %H:%M:%S"))
        finally:
            writetree(item["shelfmark"], item["index"], tree)
    else:
        if exit_when_empty:
            r.set(status, "%s: Terminated due to empty queue"%datetime.now().strftime("%d/%m/%y %H:%M:%S"))
            sys.exit(1)
        # no item, wait and try again
        #print("No items in queue, sleeping for %ss"%current_wait)
        r.set(status, "%s: No items in queue, sleeping for %ss"%(datetime.now().strftime("%d/%m/%y %H:%M:%S"),current_wait))
        sleep(current_wait)
        current_wait = current_wait * wait_modifier
        if current_wait > wait_maxseconds: current_wait = wait_maxseconds
        continue





