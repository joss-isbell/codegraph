"""Send CTRL_BREAK only to an owned test's private hidden console."""
import ctypes
import sys
import time
kernel = ctypes.WinDLL("kernel32", use_last_error=True)
kernel.FreeConsole()
if not kernel.AttachConsole(int(sys.argv[1])):
    raise ctypes.WinError(ctypes.get_last_error())
Handler = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_uint32)
handler = Handler(lambda event: True)
kernel.SetConsoleCtrlHandler(handler, True)
if not kernel.GenerateConsoleCtrlEvent(1, 0):
    raise ctypes.WinError(ctypes.get_last_error())
time.sleep(0.1)
