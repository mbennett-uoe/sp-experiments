# -*- coding: utf-8 -*-
'''Poll a redis queue for images to OCR, run them through tesseract and write the results to disk'''

import os, sys, json, codecs
from datetime import datetime
from time import sleep
from redis import Redis
from PIL import Image
import pyocr
#from logging import Logger

r = Redis()

# Config
queues = {"read":"ocr:to_process",
          "write":"ocr:processed",
          "work":"ocr:in_progress",
          "error":"ocr:errors"
          }

status = "status:ocr_worker"

wait_seconds = 15 # How long to sleep for if no items in the queue
wait_modifier = 1 # Multiplier for wait_seconds if consecutive polls are empty
wait_maxseconds = 900 # What stage to stop increasing the wait time
exit_when_empty = False

tesseract_dicts = ["eng", "enm"]

# initialise tesseract
try:
    tess = pyocr.get_available_tools()[0]
except Exception as e:
    r.set(status,"%s: Terminated with fatal error - No Tesseract found! - %s"%(datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"),e))
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
                     "timestamp": datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"),
                     "data": json_item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        # Do we have the data we need?
        if "infile" not in item or "outpath" not in item:
            # Well, this is awkward! If I'm the only one populating the queue, I would hope that we should
            # never end up here unless I've done something monumentally stupid, but better safe than sorry!
            error = {"error":"Missing required data",
                     "timestamp": datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        # Does the desired input file exist?
        if not os.path.isfile(item["infile"]):
            error = {"error": "Input file does not exist",
                     "timestamp": datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        # Does the proposed output directory exist?
        if not os.path.isdir(item["outpath"]):
            error = {"error": "Output path is not a directory",
                     "timestamp": datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        if "dicts" not in item: item["dicts"] = tesseract_dicts
        # Is the proposed list of tesseract dictionaries actually a list?
        if not isinstance(item["dicts"], list):
            error = {"error": "Tesseract dictionaries list is not actually a list!",
                     "timestamp": datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            continue
        # ok, so at this point everything should be cool, let's try and process the image
        try:
            r.set(status, "%s: Processing %s"%(datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"),item["infile"]))
            #print("Running OCR...")
            # if no dictionaries specified, use all of them!
            if len(item["dicts"]) == 0:
                dicts = tesseract_dicts
            else:
                dicts = item["dicts"]

            image = Image.open(item["infile"])
            for dict in dicts:
                image_text = tess.image_to_string(image, lang=dict, builder=pyocr.builders.TextBuilder())
                #word_boxes = tess.image_to_string(image, lang=dict, builder=pyocr.builders.WordBoxBuilder())
                #line_boxes = tess.image_to_string(image, lang=dict, builder=pyocr.builders.LineBoxBuilder())
                inf = item["infile"].split("/")[-1]
                with codecs.open(item["outpath"] + inf + "-" + dict + "-" + "text.txt", 'w', encoding='utf-8') as f:
                    pyocr.builders.TextBuilder().write_file(f, image_text)
                #with codecs.open(item["outpath"] + inf + "-" + dict + "-" + "words.txt", 'w', encoding='utf-8') as f:
                #    pyocr.builders.WordBoxBuilder().write_file(f, word_boxes)
                #with codecs.open(item["outpath"] + inf + "-" + dict + "-" + "lines.txt", 'w', encoding='utf-8') as f:
                #    pyocr.builders.LineBoxBuilder().write_file(f,line_boxes)

            #process_image(item["infile"], item["outfile"])
            # if this didn't error out to the except block, we can assume process complete
            # write to complete, remove from in progress
            r.rpush(queues["write"], json_item)
            r.lrem(queues["work"], json_item)
            r.set(status, "%s: Waiting for work"%datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"))
            #print("Done")
            # all done, go to the top and start again!
            continue
        except Exception as e:
            # something went wrong with image processing
            error = {"error": str(e),
                     "timestamp": datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"),
                     "data": item}
            r.lpush(queues["error"], json.dumps(error))
            r.lrem(queues["work"], json_item)
            r.set(status, "%s: Waiting for work"%datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"))
    else:
        if exit_when_empty:
            r.set(status, "%s: Terminated due to empty queue"%datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"))
            sys.exit(1)
        # no item, wait and try again
        #print("No items in queue, sleeping for %ss"%current_wait)
        r.set(status, "%s: No items in queue, sleeping for %ss"%(datetime.utcnow().strftime("%d/%m/%y %H:%M:%S"),current_wait))
        sleep(current_wait)
        current_wait = current_wait * wait_modifier
        if current_wait > wait_maxseconds: current_wait = wait_maxseconds
        continue





