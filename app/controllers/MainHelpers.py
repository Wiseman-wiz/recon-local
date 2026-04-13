from django.contrib import messages 

# import other modules
from datetime import datetime
from uuid import uuid4
class MainHelpers:
    def __init__(self):
        return None
    
    def upload_message(self,request, msg_level, msg):
        if msg_level == "error":
            return messages.error(self,request, msg)
        elif msg_level == "success":
            return messages.success(self,request, msg)

    def generate_event_id(self):
        return f"{datetime.now().strftime('%Y%m-%d%H-%M%S-')}{str(uuid4())}"

