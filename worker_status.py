import curses
import time, datetime
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

def spawn_window(lines, cols, y, x, title):
    # draw the window, add the title, and create a subwindow for holding contents
    title_style = curses.A_BOLD + curses.color_pair(curses.COLOR_GREEN)
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
    return text_window

def draw_windows():
    # call the bordering wrapper for each window
    windows = {
        # top line
        "status": spawn_window(12, 40, 0, 0, "Program info"),
        "queues" : spawn_window(12, 40, 0, 40, "Queues"),
        "commands": spawn_window(12, 40, 0, 80, "Commands"),
        # below that
        "worker_messages" : spawn_window(8, 120, 12, 0, "Worker status"),
        # below that
        "errors" : spawn_window(30, 120, 20, 0, "Most recent errors"),
    }

    # populate the commands window
    commands = {
        "r":"Force refresh of screen",
        "t":"Change update speed (default 5s)",
        "w":"Worker control",
        "e":"Error handling",
        "q":"Quit",
    }
    for command, text in commands.iteritems():
        windows["commands"].addstr("%s"%command, curses.A_BOLD+curses.color_pair(curses.COLOR_CYAN))
        windows["commands"].addstr(": %s\n"%text)
    return windows

def refresh_screen(screen, windows):
    for window in windows: windows[window].noutrefresh()
    curses.doupdate()

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

def get_last_errors(num = 5):
    redis_errors = r.scan(match="*:errors")[1]
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

def update_data(screen, windows):
    # clear all non-static data. we could just overwrite it but then would have to handle for cases where the new
    # data is less than the old data, and would therefore leave old characters on the screen, so just erasing them
    # is quicker/easier. we attempt to save some resource by ignoring any windows that are never going to change
    static_windows = ["commands"]
    for window in [x for x in windows if x not in static_windows]: windows[window].erase()

    for queue, length in get_queues():
        windows["queues"].addstr("%s:"%queue)
        windows["queues"].addstr("%s\n"%length, style_number(length))
    for name, status in get_statuses(): windows["worker_messages"].addstr("%s: %s\n"%(name,status))
    for queue, error in get_last_errors(): windows["errors"].addstr("Time: %s - Source: %s\nError: %s\nData: %s\n\n"%(error["timestamp"], queue, error["error"], error["data"]))



if __name__ == "__main__":
    try:
        screen = init_screen()
        windows = draw_windows()
        sec = 0
        while True:
            try:
                keypress = windows["errors"].getch()
                if keypress == ord('q'): break
                if keypress == ord('f'): screen.flash()
                if keypress == ord('b'):
                    windows["commands"].addstr("ZOMG", curses.color_pair(curses.COLOR_CYAN))
                    windows["commands"].refresh()
            except:
                pass

            if int(sec) % 5 == 0:
                update_data(screen, windows)
                refresh_screen(screen, windows)

            sec += 0.1
            windows["status"].addstr(0,0,"Uptime: %s"%datetime.timedelta(seconds=int(sec)))
            windows["status"].refresh()
            time.sleep(0.1)

    finally:
        reset_terminal(screen)

