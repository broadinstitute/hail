import pytest
from ..helpers import *

import hail.experimental.time as htime

@skip_unless_spark_backend()
def test_strftime():
    assert hl.eval(htime.strftime("%A, %B %e, %Y. %r", 876541523, "America/New_York")) == "Friday, October 10, 1997. 11:45:23 PM"
    assert hl.eval(htime.strftime("%A, %B %e, %Y. %r", 876541523, "GMT+2")) == "Saturday, October 11, 1997. 05:45:23 AM"
    assert hl.eval(htime.strftime("%A, %B %e, %Y. %r", 876541523, "+08:00")) == "Saturday, October 11, 1997. 11:45:23 AM"

@skip_unless_spark_backend()
def test_strptime():
    assert hl.eval(htime.strptime("Friday, October 10, 1997. 11:45:23 PM", "%A, %B %e, %Y. %r", "America/New_York")) == 876541523
    assert hl.eval(htime.strptime("Friday, October 10, 1997. 11:45:23 PM", "%A, %B %e, %Y. %r", "GMT+2")) == 876519923
    assert hl.eval(htime.strptime("Friday, October 10, 1997. 11:45:23 PM", "%A, %B %e, %Y. %r", "+08:00")) == 876498323
