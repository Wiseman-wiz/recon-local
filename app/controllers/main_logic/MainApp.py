from helper_v2.Performance import timer_decorator
import json
import pathlib
from typing import Callable, Dict, List, Tuple, Type,Union

PATH_TO_FILE = pathlib.Path(__file__).parent.resolve()
CONFIG_FILE = "main_app.json"

class MainApp:
    def __init__(self) -> None:
        self.config: dict = {}
        with open(f"{PATH_TO_FILE}/{CONFIG_FILE}", "r") as f:
            json_data: str = f.read()
            print(json_data)
            self.config: dict = json.loads(json_data).get("config")

    def test_crumble(self) -> None:
        from Crumble import Crumble
    
    def test_helper(self) -> None:
        from helper_v2 import Performance
        class_ = Performance.timer_decorator()

    def test_cases(self)-> None:
        self.test_crumble


if __name__ == "__main__":
    ma = MainApp()
    ma.test_cases()
