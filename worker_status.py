import curses
import time
from redis import Redis

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
    windows = {
        "queues" : spawn_window(10, 40, 0, 0, "Queues"),
        "worker_messages" : spawn_window(10, 80, 10, 0, "Worker status"),
        "key_help" : spawn_window(10, 40, 0, 40, "Actions"),
        "errors" : spawn_window(10, 80, 20, 0, "Most recent errors"),
    }
    return windows


def refresh_screen(screen, windows):
    for window in windows: windows[window].noutrefresh()
    curses.doupdate()

def get_queues():
    redis_queues = r.scan_iter(match="[^status]*:*") # Every key containing ":" should be a queue except worker statuses
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
    redis_errors = r.scan(match="*:error")[1]
    errors = []
    if len(redis_errors) == 0: return []
    eq = int(num / len(redis_errors))
    if eq == 0: eq = 1
    for error_queue in redis_errors:
        errors.append(r.lrange(error_queue,0,eq-1))
    return errors

def style_number(n):
    empty = curses.color_pair(curses.COLOR_GREEN)
    half = curses.color_pair(curses.COLOR_YELLOW)
    full = curses.color_pair(curses.COLOR_RED) + curses.A_BOLD
    if n == 0: return empty
    elif n > 100: return full
    else: return half

def update_data(screen, windows):
    for window in windows: windows[window].erase()
    for queue, length in get_queues():
        windows["queues"].addstr("%s: "%queue)
        windows["queues"].addstr("%s\n"%length, style_number(length))
    for name, status in get_statuses(): windows["worker_messages"].addstr("%s: %s\n"%(name,status))
    for error in get_last_errors(): windows["errors"].addstr("%s\n"%error)



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
                if keypress == ord('b'): windows["errors"].addstr("ZOMG", curses.color_pair(curses.COLOR_CYAN))
            except:
                pass

            if int(sec) % 2 == 0:
                update_data(screen, windows)
                refresh_screen(screen, windows)

            sec += 0.1
            windows["key_help"].addstr(0,0,"Uptime: %ss"%sec)
            windows["key_help"].refresh()
            time.sleep(0.1)



    #except Exception as e:
    #    reset_terminal(screen)
    #    print e
    finally:
        reset_terminal(screen)

