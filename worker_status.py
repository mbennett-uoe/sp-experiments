import os, subprocess
import curses, curses.panel
import time
from datetime import datetime
from redis import Redis
import json
r = Redis()

def init_screen():
    screen = curses.initscr() # start curses
    curses.start_color() # enable colour support
    curses.use_default_colors()
    for i in range(0, curses.COLORS): curses.init_pair(i, i, -1) # initialise colour pairs
    curses.noecho() # don't show typed chars
    curses.curs_set(0) # hide cursor
    curses.cbreak() # don't wait for enter to process keys
    screen.keypad(1) # convert special keys to constants
    screen.nodelay(1) # don't block waiting for user input
    return screen

def reset_terminal(screen):
    # !!! This must be called before program exit !!!
    # Otherwise your terminal will be screwed up
    # So make sure you try/except the main function and call this before exiting
    curses.nocbreak()
    screen.keypad(0)
    curses.curs_set(1)
    curses.echo()
    curses.endwin()

def spawn_window(lines, cols, y, x, title, colour = curses.COLOR_GREEN):
    # !!! internal function only, do not call this direct from any logic !!!
    # Use the wrapper function add_window, which also deals with the paneling system

    # draw the window, add the title, and create a subwindow for holding contents
    title_style = curses.A_BOLD + curses.color_pair(colour)
    window = curses.newwin(lines, cols, y, x)
    window.border()
    window.addstr(0, 2, title, title_style)
    #window.nodelay(1)
    window.timeout(0)
    window.noutrefresh()
    #spawn an inner text window and return that
    text_window = window.derwin(lines-4, cols-4, 2, 2)
    text_window.nodelay(1)
    #window.overlay(text_window)
    return window, text_window

def draw_initial_windows():
    # initialise all the default windows at the start
    # this could be reimplemented in the future to read from a config file instead
    # call the bordering wrapper for each window
    windows = {
        # top line
        "status": spawn_window(12, 40, 0, 0, "Program info"),
        "queues" : spawn_window(12, 40, 0, 40, "Queues"),
        "commands": spawn_window(12, 40, 0, 80, "Commands"),
        # below that
        "worker_messages" : spawn_window(16, 120, 12, 0, "Worker status"),
        # below that
        "errors" : spawn_window(30, 120, 28, 0, "Most recent errors"),
    }

    # spawn a panel object with the same id for each window, so we can stack them nicely rather than farting about
    # doing overlays for input by hand etc
    panels = {}
    for key, (parent, child) in windows.iteritems():
        panels[key] = {"parent":curses.panel.new_panel(parent), "child":curses.panel.new_panel(child)}
        windows[key] = child

    # populate the commands window
    commands = {
        "r":"Force refresh of data",
        "t":"Change update speed (default 5s)",
        "w":"Worker control",
        "e":"Error handling",
        "m":"Queue management",
        "q":"Quit",
    }
    for command, text in commands.iteritems():
        windows["commands"].addstr("%s"%command, curses.A_BOLD+curses.color_pair(curses.COLOR_CYAN))
        windows["commands"].addstr(": %s\n"%text)

    return windows, panels

def add_window(id, lines, cols, y, x, title, colour = curses.COLOR_GREEN, visible = True):
    # spawn the window object
    parent, child = spawn_window(lines, cols, y, x, title, colour)
    panels[id] = {"parent": curses.panel.new_panel(parent), "child": curses.panel.new_panel(child)}
    windows[id] = child
    panels[id]["parent"].top()
    panels[id]["child"].top()
    toggle_window(id, visible)
    return child


def del_window(id):
    del panels[id]
    del windows[id]
    refresh_screen()


def toggle_window(window, visible):
    if visible:
        panels[window]["parent"].show()
        panels[window]["child"].show()
    else:
        panels[window]["parent"].hide()
        panels[window]["child"].hide()

def refresh_screen():
    curses.panel.update_panels()
    curses.doupdate()

def show_alert(alert):
    window = add_window("alert", 5, 50, 7, 35, "ERROR", curses.COLOR_RED)
    window.addstr(alert, curses.A_BOLD)
    # block till cleared
    window.timeout(-1)
    window.getch()
    del_window("alert")


def get_queues():
    # removed this approach in favour of knowing the list of queues to monitor as redis removes empty queues,
    # and we still want to know about them
    #redis_queues = r.scan_iter(match="[^status]*:*") # Every key containing ":" should be a queue except worker statuses
    redis_queues = [
        "images:to_process",
        "images:processed",
        "images:errors",
        "ocr:to_process",
        "ocr:processed",
        "ocr:errors",
    ]
    queues = []
    for queue in redis_queues:
        length = r.llen(queue)
        queues.append((queue,length))
    return queues

def get_statuses():
    redis_statuses = r.scan_iter(match="status:*")
    statuses = []
    for status in redis_statuses:
        name = status.split(":")[1]
        message = r.get(status)
        statuses.append((name,message))
    return statuses

def get_workers():
    redis_workers = r.scan_iter(match="pid:*")
    pids = []
    for worker in redis_workers:
        name = worker.split(":")[1]
        pid = int(r.get(worker))
        pids.append((name, pid))
    return pids

def get_last_errors(num = 5):
    redis_errors = r.scan(match="*:errors") #[1]
    errors = []
    if len(redis_errors) == 0: return []
    eq = int(num / len(redis_errors))
    if eq == 0: eq = 1
    for error_queue in redis_errors:
        for error in r.lrange(error_queue,0,eq-1): errors.append((error_queue,json.loads(error)))
    return errors

def style_number(n):
    empty = curses.color_pair(curses.COLOR_GREEN)
    half = curses.color_pair(curses.COLOR_YELLOW)
    full = curses.color_pair(curses.COLOR_RED) + curses.A_BOLD
    if n == 0:
        return empty
    elif n > 100:
        return full
    else:
        return half

def update_data():
    # clear all non-static data. we could just overwrite it but then would have to handle for cases where the new
    # data is less than the old data, and would therefore leave old characters on the screen, so just erasing them
    # is quicker/easier. we attempt to save some resource by ignoring any windows that are never going to change
    for window in ["queues", "worker_messages", "errors"]: windows[window].erase()

    for queue, length in get_queues():
        windows["queues"].addstr("%s:"%queue)
        windows["queues"].addstr("%s\n"%length, style_number(length))
    for name, status in get_statuses(): windows["worker_messages"].addstr("%s: %s\n"%(name,status))
    for queue, error in get_last_errors(): windows["errors"].addstr("Time: %s - Source: %s\nError: %s\nData: %s\n\n"%(error["timestamp"], queue, error["error"], error["data"]))


def handle_keypress(char):
    pass

def manage_workers():
    # spawn and fill the window
    window = add_window("wmanage",18,80,5,20,"Worker management")

    workers = get_workers()
    dead_workers = []
    for worker, pid in workers:
        try:
            os.getpgid(pid)
        except OSError:
            dead_workers.append(worker)

    window.addstr("Workers:\n\n", curses.A_BOLD)
    for count, (worker, pid) in enumerate(workers):
        if worker not in dead_workers:
            window.addstr(2+count,0,"%s) "%(count+1))
            window.addstr(2+count,3,"%s (PID: %s)"%(worker,pid),curses.color_pair(curses.COLOR_GREEN))
        else:
            window.addstr(2 + count, 0, "%s) " % (count+1))
            window.addstr(2 + count, 3,"%s (PID: %s) - PID does not match any active process" % (worker, pid), curses.color_pair(curses.COLOR_RED))

    window.addstr(10,12,"Select a worker and press K to kill or S to start")
    window.addstr(11,18,"Press R to refresh worker process data")
    window.addstr(12,17,"Press Backspace to return to main screen")

    selected = 0
    while True:
        x = window.getch()
        if x == -1: continue
        elif x == 127: # backspace
            del_window("wmanage")
            return False # No action taken
        else: x = chr(x)

        if x.isdigit():
            input_num = int(x)
            if 0 < input_num > len(workers): continue # not a valid function number
            selected = input_num
            for line in xrange(2,2+len(workers)):
                if line == selected+1: window.chgat(line, 0, 2, curses.A_STANDOUT)
                else: window.chgat(line, 0, 2, curses.A_NORMAL)

        if x in ["k","K"]:
            if selected > 0:
                # are we sure?
                if user_input("Kill %s Y/N?" % workers[selected-1][0], False, bool):
                    # faster (than) ps+cat, kill kill kill!
                    try:
                        os.kill(workers[selected-1][1],9) # send hup - nope, not good enough, time for sigkill
                        time.sleep(1) # wait a second for process to spawn and then reload the window
                        return True # action taken
                    except:
                        show_alert("Kill attempt failed")
            else:
                show_alert("Please select a worker!")
        if x in ["s", "S"]:
            if selected > 0:
                try:
                    cmd = ["python", "%s.py"%workers[selected-1][0]]
                    subprocess.Popen(cmd, close_fds=True)
                    time.sleep(1)
                    return True
                except:
                    show_alert("Start attempt failed - python %s.py"%workers[selected-1][0])
            else:
                show_alert("Please select a worker!")
        if x in ["r", "R"]:
            return True




def manage_queues():
    # spawn and fill the window
    window = add_window("qmanage",18,80,5,20,"Queue management")

    window.addstr("Select function:\n\n", curses.A_BOLD)
    window.addstr("1) ", curses.A_BOLD)
    window.addstr("Empty queue\n")
    window.addstr("2) ", curses.A_BOLD)
    window.addstr("Move items\n")
    window.addstr("3) ", curses.A_BOLD)
    window.addstr("Dump queue to file\n")
    window.addstr("4) ", curses.A_BOLD)
    window.addstr("Load queue from file")

    queues = [queue for queue, length in get_queues()]
    source_letters = "abcdefghijklm"
    dest_letters = "nlopqrstuvwxyz"

    # generate lookup object
    pairs = dict(zip(source_letters, queues) + zip(dest_letters, queues))


    window.addstr(0,25,"Select source queue:", curses.A_BOLD)
    for count, (letter, queue) in enumerate(zip(source_letters, queues)):
        window.addstr(2+count,25,"%s)"%letter, curses.A_BOLD )
        window.addstr(2+count,28,queue)

    window.addstr(0, 50, "Select destination queue:", curses.A_BOLD)
    for count, (letter, queue) in enumerate(zip(dest_letters, queues)):
        window.addstr(2 + count, 50, "%s)" % letter, curses.A_BOLD)
        window.addstr(2 + count, 53, queue)

    window.addstr(10,8,"Press Enter to execute or Backspace to return to main screen")

    # enter loop until user hits enter or backspace
    selected = {
        "function":0,
        "inqueue":None,
        "outqueue":None
    }
    while True:
        x = window.getch()
        if x == -1: continue
        elif x == 10: # enter
            # clear previous status
            window.move(12, 0)
            window.clrtoeol()
            # did the user select a function?
            if selected["function"] == 0:
                show_alert("Please select a function!")
            # yes, amazing!
            if selected["function"] == 1:
                if selected["inqueue"]:
                    if user_input("Empty queue %s Y/N?"%pairs[selected["inqueue"]],False, bool):
                        if empty_queue(pairs[selected["inqueue"]]):
                            window.addstr(12,4,"Queue %s emptied successfully"%pairs[selected["inqueue"]],
                                          curses.color_pair(curses.COLOR_GREEN))
                        else:
                            window.addstr(12,4,"Error emptying queue", curses.color_pair(curses.COLOR_RED))
                else:
                    show_alert("Please select an input queue")

            if selected["function"] == 2:
                if selected["inqueue"] and selected["outqueue"]:
                    num = user_input("Move how many items (0 for all, blank to cancel)?",-1,int)
                    if num == -1: continue # cancel
                    elif num >= 0:
                        if move_items(pairs[selected["inqueue"]], pairs[selected["outqueue"]], num):
                            window.addstr(12, 4, "Items moved successfully", curses.color_pair(curses.COLOR_GREEN))
                        else:
                            window.addstr(12, 4, "Error moving items", curses.color_pair(curses.COLOR_RED))
                    else: show_alert("Please enter a positive number")
                else:
                    show_alert("Please select input and output queues")

            if selected["function"] == 3:
                if selected["inqueue"]:
                    if dump_queue(pairs[selected["inqueue"]]):
                        window.addstr(12, 4, "Written to disk: /tmp/%s.queue"%pairs[selected["inqueue"]],
                                      curses.color_pair(curses.COLOR_GREEN))
                    else:
                        window.addstr(12, 4, "Error writing queue to disk", curses.color_pair(curses.COLOR_RED))
                else:
                    show_alert("Please select an input queue")

            if selected["function"] == 4:
                if selected["outqueue"]:
                    result = load_queue(pairs[selected["outqueue"]])
                    if result:
                        window.addstr(12, 4, "%s records loaded into %s" % (result, pairs[selected["outqueue"]]),
                                      curses.color_pair(curses.COLOR_GREEN))
                    else:
                        window.addstr(12, 4, "Error loading queue from disk", curses.color_pair(curses.COLOR_RED))
                else:
                    show_alert("Please select a destination queue")

            continue

        elif x == 127: # backspace
            del_window("qmanage")
            break
        else: x = chr(x)

        if x.isdigit():
            input_num = int(x)
            if 0 < input_num > 4: continue # not a valid function number
            selected["function"] = input_num
            for line in xrange(2,6):
                if line == selected["function"]+1: window.chgat(line, 3, 20, curses.A_STANDOUT)
                else: window.chgat(line, 3, 20, curses.A_NORMAL)
        elif x in source_letters[:len(queues)]:
            selected["inqueue"] = x
            for line in xrange(2,len(queues)+2):
                if window.instr(line, 25, 1) == x: window.chgat(line, 28, 18, curses.A_STANDOUT)
                else: window.chgat(line, 28, 18, curses.A_NORMAL)
        elif x in dest_letters[:len(queues)]:
            selected["outqueue"] = x
            for line in xrange(2, len(queues) + 2):
                if window.instr(line, 50, 1) == x:
                    window.chgat(line, 53, 18, curses.A_STANDOUT)
                else:
                    window.chgat(line, 53, 18, curses.A_NORMAL)

def user_input(query, default = "", responsetype = str):
    # initialise the input overlay
    winheight = int(len(query)/56)+5 # Make sure window is big enough to fit the query!
    window = add_window("input",winheight,60,10,30,"User input")
    window.addstr(query)
    window.timeout(-1) # wait for enter when using getstr

    # input loop
    valid_input = False
    while not valid_input:
        # show a cursor and echo input
        curses.curs_set(1)
        curses.echo()
        # clear any previous input
        window.move(0,len(query)+1)
        window.clrtoeol()
        # get input from user
        response = window.getstr(0,len(query)+1)
        # turn echo + cursor back off
        curses.noecho()
        curses.curs_set(0)

        # if response is blank, return the provided default
        if response == "":
            response = default
            valid_input = True
        else:
            if responsetype == str:
                valid_input = True
            elif responsetype == int:
                try:
                    response = int(round(float(response)))
                    valid_input= True
                except ValueError:
                    show_alert("Not a valid input, expecting a number")
            elif responsetype == bool:
                if response in ["Y","y"]:
                    response = True
                    valid_input = True
                elif response in ["N", "n"]:
                    response = False
                    valid_input = True
                else:
                    show_alert("Please enter Y/N")
            else:
                # don't know now what other types we might try and handle, probably other number types, but there's
                # no harm in coding well now to make later expansion easier. edit 12/07 - apparently bool! :P
                valid_input = True

    # clear window and return val
    del_window("input")
    return response

def empty_queue(id):
    try:
        r.delete(id)
        return True
    except:
        return False

def move_items(src, dest, num):
    try:
        if num == 0: num = r.llen(src)
        for x in xrange(0, num): r.rpoplpush(src, dest)
        return True
    except:
        return False

def dump_queue(src):
    try:
        items = r.lrange(src,0,-1)
        with open("/tmp/%s.queue"%src, 'w') as outfile:
            outfile.writelines(["%s\n"%item for item in items])
        return True
    except:
        return False

def load_queue(dest):
    loaded = 0
    try:
        #items = r.lrange(src,0,-1)
        with open("/tmp/%s.queue"%dest, 'r') as infile:
            lines = infile.readlines()
            for line in lines:
                r.lpush(dest,line)
                loaded += 1
        return loaded
    except IOError:
        show_alert("File /tmp/%s.queue not found!"%dest)
        return False
    except:
        return False

if __name__ == "__main__":
    try:
        screen = init_screen()
        windows, panels = draw_initial_windows()

        start_time = datetime.now().replace(microsecond = 0)
        interval = 5
        sec = 0
        while True:
            try:
                keypress = windows["errors"].getch()
            except:
                keypress = -1

            if keypress > -1:
                char = chr(keypress)
                if char == "q":
                    break
                elif char == "r":
                    update_data()
                elif char == "t":
                    interval = user_input("New update interval? (seconds):", 5, int)
                elif char in ["l", "e"]: show_alert("Not implemented yet, sorry!")
                elif char == "w":
                    while manage_workers():
                        del_window("wmanage")
                        # manage_workers returns true if an action has occured (i.e a window update is needed)
                        # deleting and respawning the window is actually slightly less hassle than building an extra
                        # refresh loop inside the window management, since it is only performing simple tasks
                        # therefore, we loop the function until it returns False (i.e window was exited by user)
                        # After each True response, we delete the window and let the next iteration of the loop respawn
                        # it with fresh data.
                elif char == "m": manage_queues()
                else: handle_keypress(char)

            sec += 0.1
            if int(sec) % interval == 0: update_data()

            windows["status"].addstr(0,0,"Uptime: %s"%(datetime.now().replace(microsecond = 0) - start_time))
            windows["status"].addstr(1,0,"Update interval: %ss"%interval)
            windows["status"].clrtoeol()
            refresh_screen()
            time.sleep(0.1)

    finally:
        reset_terminal(screen)

