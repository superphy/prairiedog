from prairiedog.profiler import Profiler


def test_profiler():
    profiler = Profiler()
    profiler.start()
    profiler.stop()
    print(profiler.output_text(unicode=True, color=True))
    assert True
