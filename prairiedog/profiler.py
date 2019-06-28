import platform
import logging

log = logging.getLogger("prairiedog")


if platform.python_implementation() == 'PyPy':
    class Profiler:
        """
        PyPy is missing some functions required for pyinstrument
        """
        def __init__(self):
            log.warning("Initialized empty Profiler since platform is"
                        " {}".format(platform.python_implementation()))

        def start(self):
            pass

        def stop(self):
            pass

        def output_text(self, unicode, color):
            pass
else:
    import pyinstrument
    Profiler = pyinstrument.Profiler
