import COCA_Base as b
import datetime
import pytz


def test_str_to_dt():
    s = '2022-10-20T00:00:00.000Z'
    assert (b.str_to_dt(s) == datetime.datetime(2022, 10, 20, 00, 00, 00, 000))


# def test_dt_to_str():
#     dt = datetime.datetime(2022, 10, 20, 00, 00, 00,
#                            tzinfo=pytz.timezone("UTC"))
#     print(dt)
#     assert (b.dt_to_str(dt) == '2022-10-20T00:00:00.111Z')
