import json
import pathlib
from pprint import pprint
from typing import Callable, Dict, List, Tuple, Type, Union, Any

PATH_TO_FILE = pathlib.Path(__file__).parent.resolve()
CONFIG_FILE = "crumble.json"

class Crumb:
    """
    Sample Main Code:
        import Crumble
        crumble = Crumble()
        context[crumble] = crumble.get_crumble_html()

    Status:
        VERSION - 1.0

    Description:
        Handles "crumble" (links to sidebar) data

    Parameters:
        crumble_path(str) - Path of custom crumble config file
        debug(bool) - override for testing

        Methods:
            get_crumble - Returns crumble data from a config file

    Improvements:
        1.code: self.HELPER_V2_CONFIG_PATH = self.config.get("helper_v2_config")
        1.desc: Define the crumble path in a general config file and assign value to the crumble_PATH file variable

        2.code: self.DEFAULT_crumble_PATH = self.config.get("default_crumble_path")
        2.desc: Set this in a general config file and not just in a folder localized config file in helper_v2`

        3.gnrl: add validation for crumble configs

        4.gnrl: add better arguments
    """
    def __init__(self, crumble_path: str = None, debug: bool = None) -> None:
        self.CONFIG_PATH: str = f"{PATH_TO_FILE}/{CONFIG_FILE}"
        self.config: Dict = {}

        with open(self.CONFIG_PATH, "r") as f:
            json_data: str = f.read()
            self.config: dict = json.loads(json_data).get("config")
        self.DEFAULT_crumble_PATH = self.config.get("default_crumble_path")
        self.DEBUG_MODE = debug if debug else self.config.get("debug").get("crumble_py")
        self.crumble_path: str = (
            f"{PATH_TO_FILE}/"
            f"{(crumble_path if crumble_path else self.DEFAULT_crumble_PATH)}"
        )

    def __call__(self) -> Any:
        return self.get_crumble_html()

    def get_crumble_data(self, override_crumble_path: str = None) -> dict:
        """
        Status:
            DEV - ONGOING
            FRONTEND - PENDING
            BACKEND - PENDING

        Description:
           Handles "crumble" (links to sidebar) data

        Parameters:
            override_crumble_path(str) - Path of custom crumble config file

        Returns:
            Dict[dict] - Returns dictionary of crumble data with the following schema:
                {
                    "crumble":List[dict]
                    "crumble_schema":Dict[dict]
                }
        """
        if override_crumble_path:
            self.crumble_path = override_crumble_path

        with open(self.crumble_path, "r") as f:
            json_data: str = f.read()
        crumble_config: dict = json.loads(json_data)
        return crumble_config

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return (self.get_crumble_html())

    def generate_crumble(
        self,
        crumbles: Dict[str, Union[str, dict]] = None,
        crumble_parent: Union[str, None] = None,
        default_data: bool = False,
        BASE_URL: str = "https://34.146.114.227:8979",
    ) -> str:
        """
        Description:
            Formats html based on page and set parameters

        Parameters:
            crumble(Dict[str,Union[str,dict]]) - Crumble data

        Returns:
            str:HTML Format for crumble data

        Improvements:
            Add support for config based html formatting
        """
        if not crumbles:
            crumbles = self.get_crumble_data()
        data: str = ""
        # print(len(crumbles.get("crumble")))
        x = 0
        for crumb in crumbles.get("crumble"):
            # print(x)
            x += 1
            # print(crumb)
            data += str(
                f"""<li class="nav-item">\n"""
                f"""  <a href="{BASE_URL}{crumb.get('crumb_link')}" class="nav-link">\n"""
                f"""    <i class="nav-icon fas fa-cube"></i>\n"""
                f"""    <p>\n"""
                f"""      {crumb.get("crumb_name")}\n"""
                f"""    </p>\n"""
                f"""  </a>\n"""
                f"""</li>\n"""
            )
        # print(len(data))
        return data

    def get_crumble_html(self) -> str:
        # print("1_get")
        inner_func = InnerCall()
        return inner_func.generate_crumble()

    def add_crumble(
        self,
        new_crumble: List[Dict[str, str]] = None,
        config_file: str = None,
        **kwargs,
    ) -> None:
        """
        Status:
            DEV | UNUSED

        Description:
            Method to accept new crumble

        Parameters:
            new_crumble(List[[Dict[str,str]]]) - List of new crumble
            Keyword Aguments:
                crumb_name(str): Name of the crumb
                crumb_link(str): Link to the crumb
                crumb_desc(str): Description of the crumb
                crumb_floc(str): Location of the crumb
                crumb_prnt(str): Parent crumb

        Returns:
            None:None

        Improvements:
            Add support for CSV file and custom uploads
        """
        self.config_file: str = (
            config_file if config_file else self.DEFAULT_crumble_PATH
        )

        crumb_keys: List[str] = self.config.get("crumb_keys")
        crumb_prio: List[str] = self.config.get("crumb_prio")

        crumb_data: dict = {}

        for crumb in crumb_keys:
            if crumb in crumb_prio:
                try:
                    crumb_data[crumb] = str(kwargs[crumb])
                except Exception as e:
                    raise e
            else:
                crumb_data[crumb] = kwargs[crumb]

        with open(self.config_file, "r") as f:
            json_data: str = f.read()
            crumble_file: dict = json.loads(json_data)
        crumb_data = {}

        for new_crumb in new_crumble:
            crumb_schema = {
                "crumb_id": "",
                "crumb_name": "",
                "crumb_link": "",
                "crumb_desc": "",
                "crumb_floc": "",
                "crumb_prnt": "",
            }

        return None

class InnerCall(Crumb):
    def __init__(self):
        super().__init__()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    argument_list: List[Tuple[str, str, Type]] = [
        ("crumb_id", "id for the crumb", int),
        ("crumb_name", "name for the crumb", str),
        ("crumb_link", "link for the crumb", str),
        ("crumb_desc", "description for the crumb", str),
        ("crumb_floc", "file location for the crumb", str),
        ("crumb_prnt", "parent locationfor the crumb", str),
    ]

    for arg in argument_list:
        parser.add_argument(f"--{arg[0]}", help=arg[1], type=arg[2])

    args = parser.parse_args()
    inner_func = InnerCall()

    #print(inner_func.get_crumble_html())
    data = Crumb()
    data = data()
    print(data)