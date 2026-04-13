# import core python packages
import os, time, csv, re, subprocess, math

# import other modules
from io import open
from datetime import date, datetime, timedelta
from uuid import uuid4
from pprint import pprint
from uuid import uuid4

# import django core packages
from django.contrib import messages
from django.shortcuts import redirect

# import app modules


import app.controllers.MainHelpers as mh


def test_event_id():
    MH = mh.MainHelpers()
    eventid_1 = f"{MH.generate_event_id()}.csv"
    eventid_2 = f"{datetime.now().strftime('%Y%m-%d%H-%M%S-')}{str(uuid4())}.csv"
    # <IMPROVEMENTS> change to assert and unit test
    pf = type(eventid_2) == type(eventid_1)
    length = len(eventid_2) == len(eventid_1)
    result = all([pf, length])

    pf = str(pf)
    length = str(length)
    result = str(result)
    return (eventid_1, eventid_2, pf, length, result)
